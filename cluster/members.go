package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// peerDeadMargin is added to the lease before a silent peer is called dead.
	// It is fixed: the lease already says how long a node's claim lasts, and the
	// extra 2 s covers a late heartbeat and the watch's own lag.
	peerDeadMargin = 2 * time.Second
	// minMonitorTick floors the monitor's check interval.
	minMonitorTick = 5 * time.Millisecond
	// startWait bounds Start when its context has no deadline.
	startWait = 10 * time.Second
)

// heartbeat is the value of a node's key in the members bucket.
type heartbeat struct {
	NodeID    string    `json:"node_id"`
	StartedAt time.Time `json:"started_at"`
	Seq       uint64    `json:"seq"`
}

// peerState is what a node knows about one other node. lastSeen is a local
// clock reading (with its monotonic part), never a KV timestamp.
type peerState struct {
	lastSeen  time.Time
	startedAt time.Time
	rev       uint64
	dead      bool
}

const (
	stateNew = iota
	stateRunning
	stateStopped
)

// Members tracks which nodes of the cell are alive and fences this node when it
// cannot prove that it is.
//
// Each node writes {node_id, started_at, seq} under its own key in the members
// bucket every Heartbeat, and watches every key. A node is fenced when no
// write has been acknowledged for Lease/2: it can no longer count on holding
// its sessions, and the broker must stop acting on them (OnFence). It lifts the
// fence when a write is acknowledged again (OnUnfence). A peer that has not been
// seen for Lease plus 2 s is dead (OnPeerDead), on every node that is not
// fenced. All times are local monotonic readings, so a clock step changes
// nothing.
//
// A node watches its own key too, and treats a watch that has not shown its own
// heartbeat for a Lease as stale: while it is stale or the node is fenced it
// declares nobody dead and restarts every peer's clock, so a node that comes
// back from a partition gives its peers a full window to be heard.
//
// Callbacks run one at a time on a goroutine of Members, in the order the
// transitions happened, and never under a lock of Members. A callback may call
// any method except Stop. A slow callback delays the callbacks behind it and
// Stop, so hand off real work to your own goroutine.
//
// It is safe for concurrent use.
type Members struct {
	c      *Conn
	cfg    Config
	bucket string
	logger *slog.Logger

	// ctx lives from NewMembers to Stop; the goroutines run on it.
	ctx    context.Context //nolint:containedctx // the lifetime of the goroutines Members owns
	cancel context.CancelFunc

	life sync.Mutex // serializes Start and Stop

	mu         sync.Mutex
	state      int
	onFence    func()
	onUnfence  func()
	onPeerDead func(nodeID string)
	startedAt  time.Time
	seq        uint64
	lastRenew  time.Time
	fenced     bool
	failing    bool // the last renew failed (for logging)
	selfSeen   time.Time
	peers      map[string]*peerState
	queue      []func()
	wake       chan struct{}

	kv      jetstream.KeyValue
	watcher jetstream.KeyWatcher
	wg      sync.WaitGroup
}

// NewMembers returns the membership service of the node cfg describes, over c.
// The members bucket must exist: call Provision first. Nothing runs until
// Start.
func NewMembers(c *Conn, cfg Config) *Members {
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = DefaultHeartbeat
	}

	if cfg.Lease <= 0 {
		cfg.Lease = DefaultLease
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Members{
		c:      c,
		cfg:    cfg,
		bucket: cfg.MembersBucket(),
		logger: c.logger.With("component", "members"),
		ctx:    ctx,
		cancel: cancel,
		peers:  make(map[string]*peerState),
		wake:   make(chan struct{}, 1),
	}
}

// OnFence registers fn to run once when no heartbeat write has been
// acknowledged for Lease/2. It runs again only after the node has been unfenced
// and then fenced anew. Register before Start.
func (m *Members) OnFence(fn func()) {
	m.mu.Lock()
	m.onFence = fn
	m.mu.Unlock()
}

// OnUnfence registers fn to run when a heartbeat write is acknowledged again
// after a fence. Register before Start.
func (m *Members) OnUnfence(fn func()) {
	m.mu.Lock()
	m.onUnfence = fn
	m.mu.Unlock()
}

// OnPeerDead registers fn to run once for each peer that has not been seen for
// Lease + 2 s, on every node that is not fenced. A peer that is heard again is
// live again (no callback) and can be declared dead a second time. A node never
// declares itself dead. Register before Start.
func (m *Members) OnPeerDead(fn func(nodeID string)) {
	m.mu.Lock()
	m.onPeerDead = fn
	m.mu.Unlock()
}

// Start writes the first heartbeat, reads the members that exist, and starts
// the heartbeat, watch and monitor goroutines. It returns once the first
// heartbeat is acknowledged and the watch has delivered the current members.
// ctx bounds Start only (10 s when it has no deadline); the goroutines run
// until Stop. Start can be called once.
func (m *Members) Start(ctx context.Context) error {
	m.life.Lock()
	defer m.life.Unlock()

	m.mu.Lock()
	if m.state != stateNew {
		m.mu.Unlock()

		return errors.New("cluster: members: already started or stopped")
	}

	m.state = stateRunning
	m.mu.Unlock()

	if err := m.start(ctx); err != nil {
		m.teardown()

		return err
	}

	return nil
}

func (m *Members) start(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, startWait)
		defer cancel()
	}

	// Stop during Start ends it too.
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	stopAfter := context.AfterFunc(m.ctx, cancelCtx)
	defer stopAfter()

	kv, err := m.c.js.KeyValue(ctx, m.bucket)
	if err != nil {
		return m.wrapErr("open members bucket", err)
	}

	m.kv = kv

	m.mu.Lock()
	m.startedAt = time.Now().UTC()
	m.mu.Unlock()

	if rerr := m.renew(ctx); rerr != nil {
		return m.wrapErr("first heartbeat", rerr)
	}

	w, err := kv.WatchAll(m.ctx)
	if err != nil {
		return m.wrapErr("watch members", err)
	}

	m.mu.Lock()
	m.watcher = w
	m.mu.Unlock()

	ready := make(chan struct{})

	m.wg.Add(4)

	go m.watchLoop(w, ready)
	go m.heartbeatLoop()
	go m.monitorLoop()
	go m.dispatchLoop()

	select {
	case <-ready:
		m.logger.Info("members started", "heartbeat", m.cfg.Heartbeat, "lease", m.cfg.Lease)

		return nil
	case <-ctx.Done():
		return m.wrapErr("watch members", ctx.Err())
	}
}

// teardown stops everything Start began, and is what Stop calls too.
func (m *Members) teardown() {
	m.mu.Lock()
	m.state = stateStopped
	w := m.watcher
	m.queue = nil
	m.mu.Unlock()

	m.cancel()

	if w != nil {
		if err := w.Stop(); err != nil {
			m.logger.Debug("stopping members watcher", "error", err)
		}
	}

	m.wg.Wait()
}

// Stop ends the heartbeat, the watch and the monitor, and waits for them and
// for a callback that is running. No callback runs after Stop returns. The
// node's key stays in the bucket: its peers declare it dead after the lease
// like any node that went away. It is idempotent. Do not call it from a
// callback.
func (m *Members) Stop() {
	// Cancel first so a Start that is waiting on NATS lets go of the lock.
	m.cancel()

	m.life.Lock()
	defer m.life.Unlock()

	m.mu.Lock()
	already := m.state == stateStopped
	m.mu.Unlock()

	if already {
		m.wg.Wait()

		return
	}

	m.teardown()
}

// Live returns the node IDs this node believes are alive, sorted: itself and
// every peer not declared dead. A fenced node cannot see the cell and returns
// nil.
func (m *Members) Live() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.liveLocked("")
}

// Dead returns the peers this node has declared dead and not heard from
// since, sorted. A dead peer stays in it until it heartbeats again, so a
// caller that missed or lost the OnPeerDead work for one can find it here
// later. It is what this node last decided, fenced or not.
func (m *Members) Dead() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []string

	for id, p := range m.peers {
		if p.dead {
			out = append(out, id)
		}
	}

	sort.Strings(out)

	return out
}

// Fenced reports whether this node is fenced.
func (m *Members) Fenced() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.fenced
}

// IsLeaderFor reports whether this node is the one that should take over for
// the dead node deadNode: true when this node is live (not fenced, not
// stopped) and has the lowest node ID among the live nodes other than
// deadNode. Every node that sees the same live set agrees on one leader.
func (m *Members) IsLeaderFor(deadNode string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state != stateRunning || m.fenced || deadNode == m.cfg.NodeID {
		return false
	}

	live := m.liveLocked(deadNode)

	return len(live) > 0 && live[0] == m.cfg.NodeID
}

// liveLocked is the sorted live set without exclude.
func (m *Members) liveLocked(exclude string) []string {
	if m.fenced {
		return nil
	}

	out := make([]string, 0, len(m.peers)+1)

	if m.cfg.NodeID != exclude {
		out = append(out, m.cfg.NodeID)
	}

	for id, p := range m.peers {
		if !p.dead && id != exclude {
			out = append(out, id)
		}
	}

	sort.Strings(out)

	return out
}

func (m *Members) wrapErr(op string, err error) error {
	if isUnavailable(err) {
		return fmt.Errorf("cluster: members: %s: %w: %w", op, ErrUnavailable, err)
	}

	return fmt.Errorf("cluster: members: %s: %w", op, err)
}

// putTimeout is how long one heartbeat write may take: well under Lease/2, so
// a write that hangs does not hold the fence back.
func (m *Members) putTimeout() time.Duration {
	return max(m.cfg.Lease/4, 20*time.Millisecond)
}

// renew writes one heartbeat and, when it is acknowledged, records the renew.
func (m *Members) renew(ctx context.Context) error {
	m.mu.Lock()
	m.seq++
	hb := heartbeat{NodeID: m.cfg.NodeID, StartedAt: m.startedAt, Seq: m.seq}
	m.mu.Unlock()

	data, err := json.Marshal(hb)
	if err != nil {
		return err
	}

	// No lock is held across the NATS call.
	if _, err := m.kv.Put(ctx, m.cfg.NodeID, data); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastRenew = time.Now()

	if m.failing {
		m.failing = false

		m.logger.Info("heartbeat write recovered")
	}

	if m.fenced && m.state == stateRunning {
		m.fenced = false

		m.logger.Info("unfenced")
		m.enqueueLocked(m.onUnfence)
	}

	return nil
}

func (m *Members) heartbeatLoop() {
	defer m.wg.Done()

	t := time.NewTicker(m.cfg.Heartbeat)
	defer t.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}

		pctx, cancel := context.WithTimeout(m.ctx, m.putTimeout())
		err := m.renew(pctx)

		cancel()

		if err != nil && m.ctx.Err() == nil {
			m.noteFailure(err)
		}
	}
}

// noteFailure logs the first failed write of a run at Warn and the rest at
// Debug.
func (m *Members) noteFailure(err error) {
	m.mu.Lock()
	first := !m.failing
	m.failing = true
	m.mu.Unlock()

	if first {
		m.logger.Warn("heartbeat write failed", "error", err)

		return
	}

	m.logger.Debug("heartbeat write failed", "error", err)
}

func (m *Members) monitorLoop() {
	defer m.wg.Done()

	t := time.NewTicker(max(m.cfg.Heartbeat/4, minMonitorTick))
	defer t.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
			m.check()
		}
	}
}

// check applies the fence rule and the dead-peer rule.
func (m *Members) check() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state != stateRunning {
		return
	}

	if !m.fenced && time.Since(m.lastRenew) >= m.cfg.Lease/2 {
		m.fenced = true

		m.logger.Warn("fenced: no heartbeat write acknowledged", "since", time.Since(m.lastRenew).Round(time.Millisecond))
		m.enqueueLocked(m.onFence)
	}

	now := time.Now()
	observing := !m.fenced && !m.selfSeen.IsZero() && now.Sub(m.selfSeen) < m.cfg.Lease

	for id, p := range m.peers {
		if p.dead {
			continue
		}

		if !observing {
			// We cannot see the cell, so silence proves nothing: restart the
			// clock so the peer gets a full window once we can see again.
			p.lastSeen = now

			continue
		}

		if now.Sub(p.lastSeen) < m.cfg.Lease+peerDeadMargin {
			continue
		}

		p.dead = true

		m.logger.Warn("peer is dead", "peer", id, "silent", now.Sub(p.lastSeen).Round(time.Millisecond))

		if fn := m.onPeerDead; fn != nil {
			m.queue = append(m.queue, func() { fn(id) })
			m.signalLocked()
		}
	}
}

// enqueueLocked queues fn (when not nil) for the dispatcher.
func (m *Members) enqueueLocked(fn func()) {
	if fn == nil {
		return
	}

	m.queue = append(m.queue, fn)
	m.signalLocked()
}

func (m *Members) signalLocked() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// dispatchLoop runs the callbacks one at a time, outside the lock, and never
// after Stop has begun.
func (m *Members) dispatchLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wake:
		}

		for {
			m.mu.Lock()

			if m.state != stateRunning || len(m.queue) == 0 {
				m.mu.Unlock()

				break
			}

			fn := m.queue[0]
			m.queue[0] = nil
			m.queue = m.queue[1:]

			m.mu.Unlock()

			fn()
		}
	}
}

func (m *Members) watchLoop(w jetstream.KeyWatcher, ready chan<- struct{}) {
	defer m.wg.Done()

	signalled := false

	for {
		select {
		case <-m.ctx.Done():
			return
		case e, ok := <-w.Updates():
			if !ok {
				return
			}

			if e == nil {
				// nil marks the end of the entries that existed at the start.
				if !signalled {
					signalled = true

					close(ready)
				}

				continue
			}

			m.observe(e)
		}
	}
}

// observe applies one watch update.
func (m *Members) observe(e jetstream.KeyValueEntry) {
	if e.Operation() != jetstream.KeyValuePut {
		m.logger.Debug("ignoring members key removal", "key", e.Key(), "op", e.Operation().String())

		return
	}

	var hb heartbeat
	if err := json.Unmarshal(e.Value(), &hb); err != nil {
		m.logger.Warn("ignoring unreadable heartbeat", "key", e.Key(), "error", err)

		return
	}

	id := e.Key()
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	if id == m.cfg.NodeID {
		m.selfSeen = now

		return
	}

	p, ok := m.peers[id]
	if !ok {
		m.peers[id] = &peerState{lastSeen: now, startedAt: hb.StartedAt, rev: e.Revision()}

		m.logger.Info("peer seen", "peer", id)

		return
	}

	// An update that is not newer is a replay: it does not prove the peer is up.
	if e.Revision() <= p.rev && hb.StartedAt.Equal(p.startedAt) {
		return
	}

	switch {
	case p.dead:
		m.logger.Info("peer is live again", "peer", id)
	case !hb.StartedAt.Equal(p.startedAt):
		m.logger.Info("peer restarted", "peer", id)
	}

	p.dead = false
	p.lastSeen = now
	p.startedAt = hb.StartedAt
	p.rev = e.Revision()
}
