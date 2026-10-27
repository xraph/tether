package broker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/topic"
)

// Sessions in cluster mode (spec "Persistent sessions and cursors" and
// "Ownership, takeover and liveness"; spike 1 answers 1 and 2).
//
// Every CONNECT takes its session key in the cell's session bucket by
// compare-and-set (Sessions.AcquireWith) before its CONNACK, so a second
// CONNECT with the same client ID anywhere in the cell displaces the first.
// The node that loses a session it was serving hears it from its owned-keys
// watch, checks the key, and disconnects its connection with 0x8E.
//
// A persistent session lives in the key while its client is away; mochi only
// holds it while it is connected. At CONNECT a node with no local copy builds
// mochi's offline client from the record, so mochi's takeover finds it and the
// client gets session present. The session's subscriptions stay out of mochi's
// topic index while the node replays what the session missed from the log
// (cursor.go), and go in when the replay meets the tenant's live reader. After
// DISCONNECT the node writes the record one last time, with the session's
// expiry as the key's TTL, and evicts the session from mochi.

const (
	// defaultFlushInterval is how often changed session records are written:
	// at most once per interval per session.
	defaultFlushInterval = time.Second
	// sessionWriteWait bounds one session write that is not on a CONNECT.
	sessionWriteWait = 5 * time.Second
	// sessionWriteSlots bounds the session writes one node runs at once.
	sessionWriteSlots = 64
	// closedGrace is how long a session whose connection is closed waits for
	// mochi's OnDisconnect before the sweep ends it (mochi skips the hook when
	// the CONNACK or the inflight resend cannot be written).
	closedGrace = 2 * time.Second
	// connackWait bounds how long a takeover's disconnect waits for the
	// losing connection's CONNACK to go out first.
	connackWait = 5 * time.Second
)

// errSessionLost is what a write of a session another node took returns.
var errSessionLost = fmt.Errorf("tether/broker: %w: session owned elsewhere", tether.ErrConflict)

// session is one session this node owns: its key's write state, the record
// it keeps in the bucket and, for a tracked session, its cursor (cursor.go).
//
// Fields marked "node" are guarded by clusterNode.sessMu, "w" by wmu (held
// across the write), the rest by mu. Lock order: tenantTrack.mu, then mu;
// wmu, then mu. Never mu across a NATS call.
type session struct {
	nsID, tenantID, rawID string
	tt                    *tenantTrack

	// tracked: the session has a cursor, replays and persists (persistent, or
	// resumed from a persistent record). Set once at CONNECT.
	tracked bool
	// expiryCap is the connection's session expiry cap, for the final TTL.
	expiryCap uint32

	cl      *mqtt.Client // node
	prevCl  *mqtt.Client // node: the connection cl took the session over from on this node
	st      *clientState // node
	lost    bool         // node: another node took the key, or it was deleted
	deleted bool         // node: lost because the key was deleted
	ended   bool         // node: the session's end has been handled
	// deleting is set by DeleteSession on a session it is about to end: no
	// final write, the delete removes the key.
	deleting bool // node

	epoch atomic.Uint64
	rev   uint64 // w
	// done (w) is set by the session's last write, or its key's delete:
	// nothing is written after it.
	done bool

	// connacked is closed once the connection's CONNACK is out, so a
	// takeover's DISCONNECT never goes first.
	connacked chan struct{}
	ackOnce   sync.Once

	wmu sync.Mutex
	mu  sync.Mutex

	// base carries the record fields the session does not change while it
	// runs: protocol version, username, device, expiry, will, clean flag.
	base cluster.SessionRecord

	// Cursor state (cursor.go).
	mode      int
	pos       uint64
	liveFloor uint64
	flipFloor uint64
	loaded    map[string]packets.Subscription // namespaced filter: held out of the index while replaying
	skip      map[uint64]struct{}             // seqs the replay must not send: restored or already sent
	out       map[uint16]uint64               // outbound QoS 1/2 deliveries in flight: packet ID to seq
	queued    map[uint64]struct{}             // replayed QoS 1/2 waiting for the writer to take a packet ID
	pinned    bool
	pin       uint64 // with pinned: the cursor may not pass it
	repinning bool   // a repin is on its way
	// afterPin holds the deliveries completed after a pin, which the
	// repin's replay must not send again.
	afterPin map[uint64]struct{}
	// skipThrough and redo come from a resumed record: up to skipThrough
	// every message was dealt with except those in redo (its Pending).
	skipThrough uint64
	redo        map[uint64]struct{}
	// replaySeen is the highest log sequence a replay of the session has
	// been handed (sent, skipped or not matching).
	replaySeen uint64
	qos2Out    map[uint16]cluster.OutRecord
	qos2Done   map[uint64]struct{}
	recvMax    int
	closedAt   time.Time

	gen, savedGen uint64
	savedCursor   uint64

	// restored counts the QoS 2 deliveries loadSession put in the offline
	// client, which mochi resends after the CONNACK (mayResend).
	restored int

	// finalSubs are the subscriptions read just before an eviction, for
	// the last record.
	finalSubs    []cluster.SubRecord
	hasFinalSubs bool

	replayCancel context.CancelFunc
	replayDone   chan struct{}
	// writer is the running replay's writer (under mu); replayMu serialises
	// starting and stopping replays.
	writer   *replayOut
	replayMu sync.Mutex
	// handoverPos is the tenant's live position when a connection of this
	// node took the session over (handOver), for established.
	handoverPos uint64

	// willDelay, set by OnWill (clusterWill) for a will with a delay, keeps
	// the will and this node as the owner in the session's last record, and
	// finish schedules the will willDelay after willAt (clusterlive.go).
	willDelay time.Duration
	willAt    time.Time
	// willOwed, set by clusterWill, says the will was not published (the
	// node was fenced, or the append failed): the key keeps it, and finish
	// writes nothing but leaves the key to a reclaim.
	willOwed  bool
	willMsgID string
}

// touch marks the session's record as changed.
func (s *session) touch() {
	s.mu.Lock()
	s.gen++
	s.mu.Unlock()
}

// markConnacked closes connacked. Safe to call more than once.
func (s *session) markConnacked() {
	s.ackOnce.Do(func() { close(s.connacked) })
}

// sessionLocks is a lock per namespaced client ID that serialises what a
// node does to one session: a CONNECT from its key acquire to the end of
// OnSessionEstablish, a session's end at disconnect, a takeover check and a
// delete. It is held across NATS calls, never across the registry lock.
type sessionLocks struct {
	mu sync.Mutex
	m  map[string]*sessionLock
}

type sessionLock struct {
	token chan struct{}
	refs  int
}

// lock takes nsID's lock and returns its release, which is safe to call more
// than once.
func (l *sessionLocks) lock(nsID string) func() {
	l.mu.Lock()

	if l.m == nil {
		l.m = make(map[string]*sessionLock)
	}

	e := l.m[nsID]
	if e == nil {
		e = &sessionLock{token: make(chan struct{}, 1)}
		l.m[nsID] = e
	}

	e.refs++
	l.mu.Unlock()

	e.token <- struct{}{}

	var once sync.Once

	return func() {
		once.Do(func() {
			<-e.token

			l.mu.Lock()
			e.refs--

			if e.refs == 0 {
				delete(l.m, nsID)
			}

			l.mu.Unlock()
		})
	}
}

// sessionOf returns the session cl serves, nil when cl serves none on this
// node (a connection that was taken over, or one of no session).
func (n *clusterNode) sessionOf(cl *mqtt.Client) *session {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()

	if s := n.sess[cl.ID]; s != nil && s.cl == cl && !s.ended {
		return s
	}

	return nil
}

// deliveriesOf returns the tracked session whose deliveries cl carries: its
// connection, or the one it was handed over from on this node (mochi clones
// that one's inflight messages into the new connection). Unlike sessionOf it
// still answers while the session ends, until its cursor is frozen.
func (n *clusterNode) deliveriesOf(cl *mqtt.Client) *session {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()

	if s := n.sess[cl.ID]; s != nil && s.tracked && (s.cl == cl || s.prevCl == cl) {
		return s
	}

	return nil
}

// sessionByID returns the session this node owns under nsID, or nil.
func (n *clusterNode) sessionByID(nsID string) *session {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()

	return n.sess[nsID]
}

// client returns the connection serving s.
func (n *clusterNode) client(s *session) *mqtt.Client {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()

	return s.cl
}

// isLost reports whether s was taken over or deleted.
func (n *clusterNode) isLost(s *session) bool {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()

	return s.lost
}

// acquireSession takes cl's session key for this node, at the end of
// OnConnectAuthenticate, and prepares what mochi will find when it looks the
// session up: the offline client of a session resumed from the cell, or the
// local session it is taking over. It holds the session's lock and leaves it
// to OnSessionEstablish (st.sessUnlock). It returns an error, with the lock
// released, when the CONNECT must be refused: 0x88, or 0x95 for one whose
// session record (its will, say) is too large for the cell
// (cluster.ErrTooLarge).
func (b *Broker) acquireSession(cl *mqtt.Client, st *clientState, log *slog.Logger) error {
	n := b.cluster
	nsID := namespacedID(st.tenantID, st.rawID)
	unlock := n.locks.lock(nsID)

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	s, err := b.establish(ctx, cl, st, nsID)
	if err != nil {
		unlock()
		log.Info("tether: session not acquired", slog.Any("error", err))

		return err
	}

	st.sess = s
	st.sessUnlock = unlock

	return nil
}

// establish is acquireSession under the session's lock.
func (b *Broker) establish(ctx context.Context, cl *mqtt.Client, st *clientState, nsID string) (*session, error) {
	n := b.cluster
	srv := n.mochi
	tt := n.track(st.tenantID)

	resume := !cl.Properties.Clean
	persistent := st.sessionExpiry > 0

	n.sessMu.Lock()
	old := n.sess[nsID]
	n.sessMu.Unlock()

	local, hasLocal := srv.Clients.Get(nsID)

	// A live session of this node that the connection resumes: mochi hands it
	// over itself (inheritClientSession), cursor and all.
	handover := resume && old != nil && !old.lost && !old.ended && hasLocal && local == old.cl

	switch {
	case handover || !hasLocal:
	case local.Closed():
		// A session mochi still holds for a connection that is gone, and
		// that is not this node's to hand over: forget it, so mochi does not
		// give its stale state to this connection.
		b.evictLocal(srv, local)
	case old != nil && local == old.cl && old.lost:
		// The connection of a session another node took, still being
		// disconnected: its state goes now, and the session comes from the
		// cell. mochi then finds the offline client instead of it.
		b.evictLocal(srv, local)
	}

	will := willRecord(cl)
	livePos := tt.livePos.Load()

	var (
		rec, prev *cluster.SessionRecord
		rev       uint64
		err       error
		readEpoch uint64 // the epoch the latest attempt read
	)

	build := func(p *cluster.SessionRecord) *cluster.SessionRecord {
		readEpoch = 0
		if p != nil {
			readEpoch = p.Epoch
		}

		r := &cluster.SessionRecord{
			Clean:           cl.Properties.Clean,
			ProtocolVersion: cl.Properties.ProtocolVersion,
			Username:        st.username,
			DeviceID:        st.ident.Load().DeviceID,
			ExpirySeconds:   st.sessionExpiry,
			Will:            will,
			Cursor:          livePos,
			DoneThrough:     livePos,
		}

		if resume && p != nil && (p.ExpirySeconds > 0 || handover) {
			r.Subscriptions = p.Subscriptions
			r.Cursor = p.Cursor
			r.DoneThrough = p.DoneThrough
			r.Pending = p.Pending
			r.QoS2In = p.QoS2In
			r.QoS2Out = p.QoS2Out
			r.QoS2Done = p.QoS2Done
		}

		return r
	}

	if handover {
		old.wmu.Lock()
	}

	// Read the log messages a resume will resend before taking the key, so a
	// slow log delays or refuses this CONNECT and does not leave the key
	// taken (and its previous connection displaced) for nothing.
	var fetched map[uint64]cluster.LogMessage

	if resume && !handover {
		if pre, _, gerr := n.Sessions.Get(ctx, st.tenantID, st.rawID); gerr == nil && pre.ExpirySeconds > 0 {
			own, pinned := resendSeqs(pre)
			if fetched, err = b.fetchLogs(st.tenantID, own, pinned); err != nil {
				return nil, err
			}
		}
	}

	rec, prev, rev, err = b.acquireKey(ctx, st, build, &readEpoch)

	if err != nil {
		if handover {
			old.wmu.Unlock()
		}

		return nil, fmt.Errorf("tether/broker: acquire session: %w", err)
	}

	// A new epoch: whatever this node noted about the key's previous one is
	// moot.
	n.clearNoWill(nsID, 0)

	if handover {
		old.rev = rev
		old.epoch.Store(rec.Epoch)
		old.wmu.Unlock()

		b.handOver(old, cl, st, persistent, rec)

		return old, nil
	}

	if old != nil {
		// The session this node served under the ID is over here (lost, or
		// replaced by a clean start): its replay stops and its QoS 0 routes
		// go; a resumed session takes its own below.
		b.retire(old)
		n.releaseAll(nsID, st.tenantID)
	}

	s := &session{
		nsID: nsID, tenantID: st.tenantID, rawID: st.rawID, tt: tt,
		expiryCap: st.expiryCap,
		cl:        cl, st: st,
		connacked: make(chan struct{}),
		rev:       rev,
		base:      *rec,
		out:       make(map[uint16]uint64),
		qos2Out:   make(map[uint16]cluster.OutRecord),
		qos2Done:  make(map[uint64]struct{}),
		mode:      modeLive,
		pos:       livePos,
		liveFloor: livePos,
		recvMax:   int(cl.Properties.Props.ReceiveMaximum),
	}
	s.epoch.Store(rec.Epoch)

	// The record just written is what the flusher would write: nothing to
	// write until something changes.
	s.savedCursor = rec.Cursor

	if hook := n.afterAcquire; hook != nil {
		hook()
	}

	resumed := resume && prev != nil && prev.ExpirySeconds > 0
	s.tracked = persistent || resumed

	if resumed {
		if err := b.loadSession(s, cl, st, prev, fetched); err != nil {
			// Ruling R28: the key goes back as it was, offline, so nothing
			// is left owned by this node or carrying this CONNECT's will.
			b.giveBack(s, rec, prev, rev)

			return nil, err
		}
	}

	n.sessMu.Lock()
	n.sess[nsID] = s
	n.sessMu.Unlock()

	if s.tracked {
		tt.mu.Lock()
		tt.sessions[s] = struct{}{}
		tt.mu.Unlock()
	}

	return s, nil
}

// Acquire retry pacing (ruling R27).
const (
	acquireRetryWindow = 2 * time.Second
	acquireRetryBase   = 5 * time.Millisecond
	acquireRetryMax    = 100 * time.Millisecond
)

// acquireKey takes st's session key with AcquireWith (ruling R27). A lost
// compare-and-set is read again: when the record still has the epoch the
// attempt read, the revision moved by the owner's own write (a flush, a QoS 2
// step), not by another CONNECT, so it is tried again with a short backoff
// for up to acquireRetryWindow. When the epoch moved, another CONNECT took
// the key in between: the first time that is a simultaneous connect, which
// this one, the later, takes over (the other gets 0x8E); a second time the
// key keeps moving and the CONNECT is refused (0x88), as it is when the
// window runs out. readEpoch is set by build on every attempt.
func (b *Broker) acquireKey(ctx context.Context, st *clientState, build func(*cluster.SessionRecord) *cluster.SessionRecord,
	readEpoch *uint64,
) (rec, prev *cluster.SessionRecord, rev uint64, err error) {
	n := b.cluster
	deadline := time.Now().Add(acquireRetryWindow)
	delay := acquireRetryBase
	raced := false

	for {
		rec, prev, rev, err = n.Sessions.AcquireWith(ctx, st.tenantID, st.rawID, n.Config.NodeID, build)
		if err == nil || !errors.Is(err, cluster.ErrLostCAS) {
			return rec, prev, rev, err
		}

		n.casRetries.Add(1)

		cur, _, gerr := n.Sessions.Get(ctx, st.tenantID, st.rawID)

		var epoch uint64

		switch {
		case gerr == nil:
			epoch = cur.Epoch
		case errors.Is(gerr, tether.ErrNotFound):
		default:
			return nil, nil, 0, gerr
		}

		if epoch != *readEpoch {
			if raced {
				return nil, nil, 0, err
			}

			raced = true

			continue
		}

		if time.Now().After(deadline) {
			return nil, nil, 0, err
		}

		select {
		case <-ctx.Done():
			return nil, nil, 0, err
		case <-time.After(delay):
		}

		delay = min(2*delay, acquireRetryMax)
	}
}

// giveBack returns the key of a resume that failed after it was acquired:
// the session as it was, offline (no owner, no will) with its expiry as the
// key's TTL (ruling R28).
func (b *Broker) giveBack(s *session, rec, prev *cluster.SessionRecord, rev uint64) {
	n := b.cluster

	back := *rec
	back.Owner = ""
	back.Will = nil
	back.ExpirySeconds = prev.ExpirySeconds
	back.DisconnectedAt = time.Now().UTC()

	ttl := time.Duration(max(prev.ExpirySeconds, 1)) * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), sessionWriteWait)
	defer cancel()

	if _, err := n.Sessions.Expire(ctx, s.tenantID, s.rawID, rev, &back, ttl); err != nil && !errors.Is(err, cluster.ErrLostCAS) {
		// Until it goes through, the key looks like a live connection of
		// this node with the refused CONNECT's will: this node's own
		// reclaim settles it without the will, and the write is retried.
		n.log.Warn("tether: session key not given back after a failed resume, retrying", slog.String("client", s.nsID), slog.Any("error", err))
		n.markNoWill(s.nsID, rec.Epoch)
		n.goLive(func() { b.retryGiveBack(s.tenantID, s.rawID, rev, &back, ttl) })
	}

	n.qos2Mu.Lock()
	delete(n.qos2In, s.nsID)
	n.qos2Mu.Unlock()

	n.releaseAll(s.nsID, s.tenantID)
}

// retryGiveBack retries giveBack's write until it goes through, the key
// moves (a lost CAS: a new CONNECT, or this node's reclaim settled it), or
// the broker stops; it waits while the node is fenced.
func (b *Broker) retryGiveBack(tenantID, rawID string, rev uint64, back *cluster.SessionRecord, ttl time.Duration) {
	n := b.cluster
	nsID := namespacedID(tenantID, rawID)
	delay := willRetry

	for {
		if !n.fenced.Load() {
			ctx, cancel := context.WithTimeout(n.ctx, sessionWriteWait)
			_, err := n.Sessions.Expire(ctx, tenantID, rawID, rev, back, ttl)

			cancel()

			if err == nil || errors.Is(err, cluster.ErrLostCAS) {
				n.clearNoWill(nsID, back.Epoch)

				return
			}
		}

		select {
		case <-n.ctx.Done():
			return
		case <-time.After(delay):
		}

		delay = min(2*delay, willRetryMax)
	}
}

// handOver gives s, a session this node serves on another connection, to
// the connection cl that takes it over here; mochi moves its subscriptions
// and inflight messages when it runs inheritClientSession.
func (b *Broker) handOver(s *session, cl *mqtt.Client, st *clientState, persistent bool, rec *cluster.SessionRecord) {
	n := b.cluster

	s.mu.Lock()

	prevCl := s.cl

	s.base = *rec
	s.base.Subscriptions, s.base.QoS2In, s.base.QoS2Out, s.base.QoS2Done = nil, nil, nil, nil
	s.recvMax = int(cl.Properties.Props.ReceiveMaximum)
	s.expiryCap = st.expiryCap
	wasTracked := s.tracked
	s.gen++
	s.mu.Unlock()

	if !wasTracked && persistent {
		// A clean session becomes a persistent one: it has no cursor yet. It
		// starts at the live position, and what the old connection has in
		// flight from the log keeps it below those messages.
		lp := s.tt.livePos.Load()

		s.mu.Lock()
		s.tracked = true
		s.mode, s.pos, s.liveFloor = modeLive, lp, lp

		for _, pk := range prevCl.State.Inflight.GetAll(false) {
			if seq, ok := seqOf(pk); ok && pk.FixedHeader.Type == packets.Publish {
				s.out[pk.PacketID] = seq
			}
		}

		s.mu.Unlock()

		s.tt.mu.Lock()
		s.tt.sessions[s] = struct{}{}
		s.tt.mu.Unlock()
	}

	// The live position before mochi clones the old connection's inflight
	// map into this one: if the clone fills the map, the cursor is held
	// there (established). Ruling R29: it is read before the client is
	// switched. Until then pinFull checks the old connection, which mochi
	// keeps delivering to until Clients.Add, so a message that its full map
	// drops is pinned there; after the switch pinFull checks the new, empty
	// connection, and a message dropped on the old one is above this
	// position, so established's pin still holds it.
	s.mu.Lock()
	s.handoverPos = s.tt.livePos.Load()
	s.mu.Unlock()

	n.sessMu.Lock()
	s.prevCl = prevCl
	s.cl, s.st = cl, st
	s.connacked = make(chan struct{})
	s.ackOnce = sync.Once{}
	s.closedAt = time.Time{}
	n.sessMu.Unlock()

	// A replay running for the old connection stops, its writer abandoned
	// and what it had queued given back; OnSessionEstablished starts the
	// replay again for this one. The client is switched first, so a replay
	// started for the old one meanwhile (a repin) is stopped here or never
	// starts.
	n.stopReplay(s)
}

// loadSession prepares a session resumed from the cell for mochi: the
// subscriptions the connection's grants still allow (held out of mochi's
// topic index until the replay is done), the QoS 2 deliveries in flight as an
// offline client's inflight messages, and the QoS 2 pending entries of the
// client's own publishes.
func (b *Broker) loadSession(s *session, cl *mqtt.Client, st *clientState, prev *cluster.SessionRecord,
	fetched map[uint64]cluster.LogMessage,
) error {
	n := b.cluster

	if hook := n.loadHook; hook != nil {
		if err := hook(); err != nil {
			return err
		}
	}

	s.mode = modeReplay
	s.pos = prev.Cursor
	s.replaySeen = prev.Cursor
	s.liveFloor = prev.Cursor
	s.skipThrough = max(prev.DoneThrough, prev.Cursor)
	s.flipFloor = s.skipThrough
	s.loaded = make(map[string]packets.Subscription, len(prev.Subscriptions))
	s.skip = make(map[uint64]struct{})
	s.redo = make(map[uint64]struct{}, len(prev.Pending))

	for _, seq := range prev.Pending {
		s.redo[seq] = struct{}{}
	}

	for _, sr := range prev.Subscriptions {
		ns := topic.Namespace(s.tenantID, sr.Filter)
		if !b.canRead(s.tenantID, st.acl(), ns) {
			continue
		}

		s.loaded[ns] = packets.Subscription{
			Filter: ns, Qos: sr.QoS, NoLocal: sr.NoLocal, RetainAsPublished: sr.RetainAsPublished,
			RetainHandling: sr.RetainHandling, Identifier: sr.Identifier,
		}
	}

	for _, seq := range prev.QoS2Done {
		if seq > prev.Cursor {
			s.qos2Done[seq] = struct{}{}
			s.skip[seq] = struct{}{}
			s.flipFloor = max(s.flipFloor, seq)
		}
	}

	var inflight []packets.Packet

	ids := make([]uint16, 0, len(prev.QoS2Out))
	for id := range prev.QoS2Out {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, func(a, c uint16) int { return cmp.Compare(outSeq(prev.QoS2Out[a]), outSeq(prev.QoS2Out[c])) })

	// The messages of deliveries to send again: read before the acquire,
	// and in one pass over the log for any the record gained since.
	var missing, missingPinned []uint64

	own, pinned := resendSeqs(prev)

	for _, seq := range own {
		if _, ok := fetched[seq]; !ok {
			missing = append(missing, seq)
		}
	}

	for _, seq := range pinned {
		if _, ok := fetched[seq]; !ok {
			missingPinned = append(missingPinned, seq)
		}
	}

	msgs, err := b.fetchLogs(s.tenantID, missing, missingPinned)
	if err != nil {
		return err
	}

	for seq, m := range fetched {
		if msgs == nil {
			msgs = make(map[uint64]cluster.LogMessage, len(fetched))
		}

		msgs[seq] = m
	}

	now := time.Now().Unix()

	for i, id := range ids {
		or := prev.QoS2Out[id]

		if or.Seq == 0 {
			// A pinned shared QoS 2 delivery (ruling R39): its PUBREL, or
			// its message again under the same packet ID (mochi resends it
			// with DUP). No cursor, skip or flip floor: it is not the
			// session's own log message.
			pk, ok := sharedResend(s.tenantID, id, or, msgs, time.Now())
			if !ok {
				continue // gone from the log, or expired: dropped
			}

			pk.Created = now - int64(len(ids)) + int64(i)

			s.qos2Out[id] = or
			inflight = append(inflight, pk)
			s.restored++

			continue
		}

		if or.Seq <= prev.Cursor {
			continue
		}

		var pk packets.Packet

		switch or.State {
		case outPubrel:
			pk = packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1}, PacketID: id}
		default:
			m, ok := msgs[or.Seq]
			if !ok {
				// Gone from the log: nothing left to send.
				continue
			}

			pk = packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2},
				PacketID:    id,
				TopicName:   topic.Namespace(s.tenantID, m.Topic),
				Payload:     m.Payload,
				Properties:  packetProps(m.Props),
			}
			if m.ClientID != "" {
				pk.Origin = namespacedID(m.Tenant, m.ClientID)
			}
		}

		// mochi resends in the order of Created.
		pk.Created = now - int64(len(ids)) + int64(i)
		tagSeq(&pk, or.Seq)

		s.qos2Out[id] = or
		s.out[id] = or.Seq
		s.skip[or.Seq] = struct{}{}
		s.flipFloor = max(s.flipFloor, or.Seq)

		inflight = append(inflight, pk)
		s.restored++
	}

	// What is restored or already completed is accounted for by the
	// deliveries and qos2Done; redo keeps only what must be sent again.
	for seq := range s.skip {
		delete(s.redo, seq)
	}

	b.materialize(n.mochi, cl, s.nsID, prev, inflight)

	// The QoS 2 pending entries of the client's own publishes.
	if len(prev.QoS2In) > 0 {
		n.qos2Mu.Lock()
		set := make(map[uint16]string, len(prev.QoS2In))

		for id, msgID := range prev.QoS2In {
			set[id] = msgID
		}

		n.qos2In[s.nsID] = set
		n.qos2Mu.Unlock()
	}

	// QoS 0 from other nodes for the loaded filters; until the replay is done
	// the session is not in the index, so it gets none of it (QoS 0 may be
	// dropped).
	for ns, sub := range s.loaded {
		if rel, ok := routedFilter(s.tenantID, ns); ok {
			if err := n.subscribe(s.nsID, s.tenantID, rel); err != nil {
				n.routeErrors.Add(1)
				n.log.Warn("tether: route of a resumed subscription failed", slog.String("client", s.nsID),
					slog.String("filter", rel), slog.Any("error", err))
			}
		} else if mqtt.IsSharedFilter(ns) {
			// The session rejoins its groups here; deliveries wait for its
			// CONNACK (deliverShared).
			n.joinLoaded(s, sub)
		}
	}

	return nil
}

// fetchAttempts bounds the reads of fetchLogs.
const fetchAttempts = 3

// resendSeqs lists the log sequences of a record's QoS 2 deliveries that are
// resent as PUBLISH on resume (state outPublish): own, the session's own
// above the cursor, and pinned, its pinned shared ones wherever they are in
// the log (a group's consumer can lag the live log far behind the cursor).
func resendSeqs(rec *cluster.SessionRecord) (own, pinned []uint64) {
	for _, or := range rec.QoS2Out {
		switch {
		case or.State == outPubrel:
		case or.Seq == 0 && or.Shared > 0:
			pinned = append(pinned, or.Shared)
		case or.Seq > rec.Cursor:
			own = append(own, or.Seq)
		}
	}

	return own, pinned
}

// outSeq is the log sequence of an outbound QoS 2 record entry, its own or a
// pinned shared delivery's, for ordering the resend.
func outSeq(or cluster.OutRecord) uint64 {
	if or.Seq == 0 {
		return or.Shared
	}

	return or.Seq
}

// sharedResend builds the packet a resumed session sends again for a pinned
// shared QoS 2 delivery (ruling R39): its PUBREL, or its message from the log
// under the same packet ID, untagged (no cursor part). False when the message
// is gone from the log or expired: nobody is owed it any more.
func sharedResend(tenantID string, id uint16, or cluster.OutRecord, msgs map[uint64]cluster.LogMessage, now time.Time) (packets.Packet, bool) {
	if or.State == outPubrel {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1}, PacketID: id}, true
	}

	m, ok := msgs[or.Shared]
	if !ok || or.Shared == 0 {
		return packets.Packet{}, false
	}

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2},
		PacketID:    id,
		TopicName:   topic.Namespace(tenantID, m.Topic),
		Payload:     m.Payload,
		Properties:  packetProps(m.Props),
	}

	if m.ClientID != "" {
		pk.Origin = namespacedID(m.Tenant, m.ClientID)
	}

	if m.Props.MessageExpiry > 0 && !m.PublishedAt.IsZero() {
		exp := m.PublishedAt.Add(time.Duration(m.Props.MessageExpiry) * time.Second)
		if !exp.After(now) {
			return packets.Packet{}, false
		}

		pk.Expiry = exp.Unix()
	}

	return pk, true
}

// fetchLogs reads the log messages seqs of tenantID in one replay, with its
// own deadline; one the log no longer has is missing from the result.
func (b *Broker) fetchLogs(tenantID string, seqs, pinned []uint64) (map[uint64]cluster.LogMessage, error) {
	if len(seqs) == 0 && len(pinned) == 0 {
		return nil, nil //nolint:nilnil // nothing asked, nothing read
	}

	want := make(map[uint64]struct{}, len(seqs))
	for _, seq := range seqs {
		want[seq] = struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	got := make(map[uint64]cluster.LogMessage, len(seqs)+len(pinned))

	// Pinned shared deliveries one by one (ruling R40): they can sit far
	// below the session's own, and a ranged read from the lowest would
	// stream the tenant's whole log in between on the CONNECT path. One
	// gone from the log is not resent.
	for _, seq := range pinned {
		m, err := b.cluster.Log.Get(ctx, tenantID, seq)

		switch {
		case err == nil:
			got[seq] = m
		case errors.Is(err, tether.ErrNotFound):
		default:
			return nil, fmt.Errorf("tether/broker: read the log message of a pinned shared delivery: %w", err)
		}
	}

	if len(seqs) == 0 {
		return got, nil
	}

	// These messages were delivered, so the log has them unless they aged
	// out; a read that misses one may have lagged and is tried again.
	for attempt := range fetchAttempts {
		var missing []uint64

		for seq := range want {
			if _, ok := got[seq]; !ok {
				missing = append(missing, seq)
			}
		}

		if len(missing) == 0 {
			break
		}

		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("tether/broker: read the log messages of the deliveries in flight: %w", ctx.Err())
			case <-time.After(replayPoll * 5):
			}
		}

		err := b.cluster.Log.Replay(ctx, tenantID, slices.Min(missing), slices.Max(missing), func(m cluster.LogMessage) error {
			b.cluster.fetchRead.Add(1)

			if _, ok := want[m.Seq]; ok {
				got[m.Seq] = m
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("tether/broker: read the log messages of the deliveries in flight: %w", err)
		}
	}

	return got, nil
}

// materialize builds the offline mochi client of a session resumed from the
// cell, the way mochi's loadClients restores one (spike 1): closed, with the
// session's expiry and the QoS 2 deliveries in flight. inheritClientSession
// takes it over right after OnSessionEstablish, and the CONNACK says session
// present. Its subscriptions are not on it: they wait in session.loaded.
func (b *Broker) materialize(srv *mqtt.Server, cl *mqtt.Client, nsID string, prev *cluster.SessionRecord, inflight []packets.Packet) {
	off := srv.NewClient(nil, cl.Net.Listener, nsID, false)
	off.Properties.ProtocolVersion = cl.Properties.ProtocolVersion
	off.Properties.Clean = false
	off.Properties.Props.SessionExpiryInterval = prev.ExpirySeconds
	off.Properties.Props.SessionExpiryIntervalFlag = true

	for _, pk := range inflight {
		if off.State.Inflight.Set(pk) {
			atomic.AddInt64(&srv.Info.Inflight, 1)
		}
	}

	off.Stop(packets.ErrServerShuttingDown)
	srv.Clients.Add(off)
}

// retire forgets old, a session this node no longer serves under its ID
// (lost, ended, or replaced by a clean start).
func (b *Broker) retire(old *session) {
	n := b.cluster

	n.stopReplay(old)

	old.tt.mu.Lock()
	delete(old.tt.sessions, old)
	delete(old.tt.waiting, old)
	old.tt.mu.Unlock()

	old.tt.fullMu.Lock()
	delete(old.tt.full, old)
	old.tt.fullMu.Unlock()
}

// willRecord is the will of cl's CONNECT as the record keeps it, with its
// tenant-relative topic, or nil.
func willRecord(cl *mqtt.Client) *cluster.WillRecord {
	w := &cl.Properties.Will
	if atomic.LoadUint32(&w.Flag) != 1 {
		return nil
	}

	wr := &cluster.WillRecord{
		Topic:        w.TopicName,
		Payload:      w.Payload,
		QoS:          w.Qos,
		Retain:       w.Retain,
		DelaySeconds: w.WillDelayInterval,
	}

	if len(w.User) > 0 {
		wr.Props.User = make([]cluster.UserProperty, len(w.User))
		for i, u := range w.User {
			wr.Props.User[i] = cluster.UserProperty{Key: u.Key, Value: u.Val}
		}
	}

	return wr
}

// established runs in OnSessionEstablished, after mochi took the session
// over and wrote the CONNACK: a connection that is no longer the owner of
// its ID (a concurrent CONNECT on this node took it in between) or whose
// session another node took meanwhile is disconnected with 0x8E; otherwise a
// session that has a replay to do starts it.
func (b *Broker) established(cl *mqtt.Client, st *clientState) {
	n := b.cluster

	s := st.sess
	if s == nil {
		return
	}

	s.markConnacked()

	if s.tracked {
		// What mochi cloned may have filled the inflight map before this
		// ran, and a message injected meanwhile been dropped: the cursor
		// stays at the live position from before the clone.
		s.mu.Lock()
		pos := s.handoverPos
		s.mu.Unlock()

		if pos > 0 {
			n.pinIfFull(s, cl, pos)
			n.recoverHandover(s, cl)
		} else {
			n.noteFull(s, cl)
		}
	}

	if n.fenced.Load() {
		// Fenced while this CONNECT was on its way: it goes like the rest.
		n.dropClient(n.mochi, cl)

		return
	}

	if b.state.owner(cl.ID) != st {
		n.log.Info("tether: connection lost its client ID to a concurrent connect", slog.String("client", cl.ID))
		disconnect(n.mochi, b.log, cl, packets.ErrSessionTakenOver)

		return
	}

	n.sessMu.Lock()
	lost, mine := s.lost, s.cl == cl
	n.sessMu.Unlock()

	if !mine {
		return
	}

	if lost {
		disconnect(n.mochi, b.log, cl, packets.ErrSessionTakenOver)

		return
	}

	s.mu.Lock()
	replaying := s.tracked && s.mode != modeLive
	s.mu.Unlock()

	if replaying {
		b.startReplay(s, cl)
	}
}

// recoverHandover finds the log deliveries a same-node handover lost: mochi
// clones the old connection's inflight map into the new one but goes on
// delivering to the old connection until Clients.Add, so a delivery injected
// in between went to the old connection only. It is in the session's
// deliveries (deliveriesOf takes the old connection) and in no inflight map
// that will ever be resent. Each delivery of the session that the new
// connection's inflight map does not hold under the same packet ID and
// sequence is dropped from the books and the session is pinned below it, so
// a repin replays it (as onPublishDropped does). followShare does the same
// for shared deliveries. It runs at the CONNACK (established), after
// Clients.Add, when nothing more can go to the old connection.
func (n *clusterNode) recoverHandover(s *session, cl *mqtt.Client) {
	s.mu.Lock()

	repin := false

	for id, seq := range s.out {
		if held, ok := cl.State.Inflight.Get(id); ok {
			if hs, tagged := seqOf(held); tagged && hs == seq {
				continue
			}
		}

		if s.pinLocked(seq - 1) {
			repin = true
		}

		delete(s.out, id)
		delete(s.skip, seq)
		n.handoverLost.Add(1)
	}

	s.mu.Unlock()

	if repin {
		go n.b.repin(s)
	}
}

// endSession runs in lastHook.OnDisconnect for a network client in cluster
// mode, after its state is removed. If cl serves its session on this node,
// the session ends here: a clean one deletes its key; a persistent one is
// evicted from mochi and its record written one last time with its expiry as
// the key's TTL; one that another node took (or that is being deleted) is
// evicted with nothing written.
func (b *Broker) endSession(cl *mqtt.Client, expire bool) {
	n := b.cluster

	unlock := n.locks.lock(cl.ID)
	defer unlock()

	n.sessMu.Lock()
	s := n.sess[cl.ID]
	mine := s != nil && s.cl == cl && !s.ended
	n.sessMu.Unlock()

	if mine {
		b.finish(s, cl, expire)
	}
}

// finish ends s, whose connection cl has ended. The caller holds s's lock.
//
// The session keeps recording what is delivered to it until its cursor is
// frozen and it is out of mochi's index, both under the tenant's lock, so a
// message injected meanwhile either holds the cursor below it or does not
// reach the session at all. Only then is it taken out of the node's
// sessions.
func (b *Broker) finish(s *session, cl *mqtt.Client, expire bool) {
	n := b.cluster
	srv := n.mochi

	n.stopReplay(s)

	// mochi's clients are keyed by ID: never evict a connection that took
	// the session over. A clean session mochi removes itself.
	got, ok := srv.Clients.Get(cl.ID)
	evict := !expire && ok && got == cl && !cl.IsTakenOver()

	s.tt.mu.Lock()

	if evict {
		subs := n.subRecords(s, cl)

		s.mu.Lock()
		s.finalSubs, s.hasFinalSubs = subs, true
		s.mu.Unlock()

		evictSession(srv, cl)
		n.evictions.Add(1)
	}

	s.freeze()
	delete(s.tt.sessions, s)
	delete(s.tt.waiting, s)
	s.tt.mu.Unlock()

	// A repin may have started a replay after the stop above; out of the
	// tenant's sessions it ends at once.
	b.retire(s)

	n.sessMu.Lock()
	s.ended = true

	if n.sess[s.nsID] == s {
		delete(n.sess, s.nsID)
	}

	lost, deleting := s.lost, s.deleting
	n.sessMu.Unlock()

	held := ok && got == cl && !cl.IsTakenOver()
	if held {
		// The session's QoS 0 routes go with it, those of subscriptions a
		// replay still held out of the index included. (mochi's own
		// unsubscribe of a clean session releases nothing twice.) Its QoS 2
		// pending entries go once the last record has them.
		n.releaseAll(s.nsID, s.tenantID)
		defer n.dropQoS2(s.nsID)
	}

	s.mu.Lock()
	owed, msgID, delayed := s.willOwed, s.willMsgID, s.willDelay > 0
	s.mu.Unlock()

	if !owed && !delayed && !lost && !deleting {
		// No will is owed (a normal DISCONNECT, or one already published or
		// withheld), but the key keeps the CONNECT's will until the write
		// below clears it, which may fail or be skipped (fenced): this
		// node's own reclaim must then settle it without the will.
		n.markNoWill(s.nsID, s.epoch.Load())
	}

	// The fence is read after the session is marked ended, under its lock:
	// a reclaim of this node's keys at the unfence either runs after this
	// and finds the session ended, or the unfence came first and owed is
	// settled here.
	switch {
	case lost, deleting:
		return
	case n.fenced.Load():
		// Ruling R31: the write would fail. The key stays as it was, owned
		// by this node with its will, for whoever reclaims it; the next
		// owner replays from the last cursor written.
		return
	case owed:
		// The key keeps the will, which this node still owes: it settles
		// the key like a reclaim, once this session's lock is free.
		n.goLive(func() { b.reclaimID(s.tenantID, s.rawID, msgID) })

		return
	case expire || !s.tracked:
		go b.deleteKey(s)
	default:
		ttl := time.Duration(sessionExpiry(cl, s.expiryCap)) * time.Second
		if ttl <= 0 {
			go b.deleteKey(s)

			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), sessionWriteWait)
		defer cancel()

		err := n.persist(ctx, s, ttl)
		if errors.Is(err, errSessionLost) {
			n.clearNoWill(s.nsID, 0)
		}

		if err != nil && !errors.Is(err, errSessionLost) && !errors.Is(err, errFenced) {
			n.log.Warn("tether: final session write failed; the next owner replays from the last cursor written",
				slog.String("client", s.nsID), slog.Any("error", err))
		}

		s.mu.Lock()
		delay, at := s.willDelay, s.willAt
		s.mu.Unlock()

		if delay > 0 && !errors.Is(err, errSessionLost) && !errors.Is(err, errFenced) {
			// The record keeps the will (snapshot): it goes out when the
			// delay ends unless the session comes back first. A write that
			// failed left the CONNECT's record, which has it too.
			b.scheduleWill(s.tenantID, s.rawID, s.epoch.Load(), time.Until(at.Add(delay)))
		}
	}
}

// deleteKey deletes s's key at the revision this node last wrote. A lost
// race means someone else holds the key now, which is fine.
func (b *Broker) deleteKey(s *session) {
	n := b.cluster

	ctx, cancel := context.WithTimeout(context.Background(), sessionWriteWait)
	defer cancel()

	s.wmu.Lock()
	defer s.wmu.Unlock()

	s.done = true

	if n.fenced.Load() {
		// Ruling R31: the key stays for whoever reclaims it.
		return
	}

	err := n.Sessions.Delete(ctx, s.tenantID, s.rawID, s.rev)
	if err != nil && !errors.Is(err, cluster.ErrLostCAS) {
		n.log.Warn("tether: session key not deleted", slog.String("client", s.nsID), slog.Any("error", err))

		return
	}

	n.clearNoWill(s.nsID, s.epoch.Load())
}

// evictLocal evicts a closed client's leftover session from mochi.
func (b *Broker) evictLocal(srv *mqtt.Server, cl *mqtt.Client) {
	if got, ok := srv.Clients.Get(cl.ID); !ok || got != cl || cl.IsTakenOver() || cl.Net.Inline {
		return
	}

	if tenantID, ok := tenantOfID(cl.ID); ok {
		if tt := b.cluster.trackIfAny(tenantID); tt != nil {
			tt.mu.Lock()
			evictSession(srv, cl)
			tt.mu.Unlock()
		} else {
			evictSession(srv, cl)
		}

		b.cluster.releaseAll(cl.ID, tenantID)
	} else {
		evictSession(srv, cl)
	}

	b.cluster.evictions.Add(1)
}

// evictSession removes cl's session from mochi's memory without the hook
// events an UNSUBSCRIBE or a session expiry would fire (spike 1: never
// UnsubscribeClient or ClearInflights). The caller has checked that cl still
// owns its ID.
func evictSession(srv *mqtt.Server, cl *mqtt.Client) {
	for filter := range cl.State.Subscriptions.GetAll() {
		if srv.Topics.Unsubscribe(filter, cl.ID) {
			atomic.AddInt64(&srv.Info.Subscriptions, -1)
		}

		cl.State.Subscriptions.Delete(filter)
	}

	for _, pk := range cl.State.Inflight.GetAll(false) {
		if cl.State.Inflight.Delete(pk.PacketID) {
			atomic.AddInt64(&srv.Info.Inflight, -1)
		}
	}

	srv.Clients.Delete(cl.ID)
}

// snapshot is the record s should have now. final makes it the record of a
// session whose client is away: no owner, no will, disconnected now.
func (n *clusterNode) snapshot(s *session, cl *mqtt.Client, final bool) (rec *cluster.SessionRecord, gen uint64) {
	subs := n.subRecords(s, cl)
	lp := s.tt.livePos.Load()

	s.mu.Lock()

	if s.hasFinalSubs {
		subs = s.finalSubs
	}

	base := s.base
	rec = &base
	rec.Owner = n.Config.NodeID
	rec.Epoch = s.epoch.Load()
	rec.Subscriptions = subs

	done := s.baseLocked(lp)

	if s.tracked {
		rec.Cursor = s.cursorLocked(lp)

		// A resume still replaying keeps what its record said was dealt
		// with beyond its position, and what was pending there.
		if s.mode != modeLive && !s.pinned && s.skipThrough > done {
			done = s.skipThrough
		}

		rec.DoneThrough = done
		rec.Pending = nil

		seen := make(map[uint64]struct{}, len(s.out)+len(s.queued))
		for _, seq := range s.out {
			seen[seq] = struct{}{}
		}

		for seq := range s.queued {
			seen[seq] = struct{}{}
		}

		for seq := range s.redo {
			if _, completed := s.qos2Done[seq]; !completed && seq > s.pos && seq <= done {
				seen[seq] = struct{}{}
			}
		}

		for seq := range seen {
			rec.Pending = append(rec.Pending, seq)
		}

		slices.Sort(rec.Pending)
	}

	if len(s.qos2Out) > 0 {
		rec.QoS2Out = make(map[uint16]cluster.OutRecord, len(s.qos2Out))
		for id, or := range s.qos2Out {
			rec.QoS2Out[id] = or
		}
	} else {
		rec.QoS2Out = nil
	}

	rec.QoS2Done = nil

	for seq := range s.qos2Done {
		if seq <= done {
			delete(s.qos2Done, seq)

			continue
		}

		rec.QoS2Done = append(rec.QoS2Done, seq)
	}

	slices.Sort(rec.QoS2Done)

	gen = s.gen
	s.mu.Unlock()

	rec.QoS2In = nil
	if in := n.qos2Entries(s.nsID); len(in) > 0 {
		rec.QoS2In = in
	}

	if final {
		s.mu.Lock()
		pending := s.willDelay > 0
		s.mu.Unlock()

		// A delayed will stays in the record, with this node as the owner
		// that answers for it, until it is published or cancelled.
		if !pending {
			rec.Owner = ""
			rec.Will = nil
		}

		rec.DisconnectedAt = time.Now().UTC()
		rec.ExpirySeconds = sessionExpiry(cl, s.expiryCap)
	}

	return rec, gen
}

// subRecords lists s's subscriptions as the record keeps them: the
// connection's and those still held out of the index, tenant-relative,
// sorted by filter.
func (n *clusterNode) subRecords(s *session, cl *mqtt.Client) []cluster.SubRecord {
	all := cl.State.Subscriptions.GetAll()

	s.mu.Lock()
	for f, sub := range s.loaded {
		if _, ok := all[f]; !ok {
			all[f] = sub
		}
	}
	s.mu.Unlock()

	out := make([]cluster.SubRecord, 0, len(all))

	for f, sub := range all {
		rel, ok := topic.Strip(s.tenantID, f)
		if !ok {
			continue
		}

		out = append(out, cluster.SubRecord{
			Filter: rel, QoS: sub.Qos, NoLocal: sub.NoLocal, RetainAsPublished: sub.RetainAsPublished,
			RetainHandling: sub.RetainHandling, Identifier: sub.Identifier,
		})
	}

	slices.SortFunc(out, func(a, c cluster.SubRecord) int { return strings.Compare(a.Filter, c.Filter) })

	if len(out) == 0 {
		return nil
	}

	return out
}

// persist writes s's record as a compare-and-set on the revision this node
// last wrote, with ttl as the key's expiry when it is not zero (the final
// write of a session whose client is away). A lost race marks s lost and
// disconnects its connection.
func (n *clusterNode) persist(ctx context.Context, s *session, ttl time.Duration) error {
	select {
	case n.writeSlots <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("tether/broker: session write: %w", ctx.Err())
	}

	defer func() { <-n.writeSlots }()

	s.wmu.Lock()
	defer s.wmu.Unlock()

	if n.isLost(s) {
		return errSessionLost
	}

	if n.fenced.Load() {
		// Ruling R31: no write can succeed, and none should be tried.
		return errFenced
	}

	if s.done {
		// A flush that raced the session's end: the last record is in.
		return nil
	}

	cl := n.client(s)
	rec, gen := n.snapshot(s, cl, ttl > 0)

	var (
		rev uint64
		err error
	)

	for attempt := 0; ; attempt++ {
		if ttl > 0 {
			rev, err = n.Sessions.Expire(ctx, s.tenantID, s.rawID, s.rev, rec, ttl)
		} else {
			rev, err = n.Sessions.Update(ctx, s.tenantID, s.rawID, s.rev, rec)
		}

		if err == nil || !errors.Is(err, cluster.ErrLostCAS) {
			break
		}

		// A lost CAS can be this node's own earlier write: one whose wait
		// ran out (sessionWriteWait, shorter than a JetStream election)
		// can still land, and moves the revision. Only a record that names
		// another owner or epoch means the session moved.
		cur, ok := n.ownWrite(ctx, s)
		if !ok || attempt >= ownWriteRetries {
			n.lose(s, false)

			return errSessionLost
		}

		n.ownWrites.Add(1)
		s.rev = cur
	}

	if err != nil {
		return fmt.Errorf("tether/broker: session write: %w", err)
	}

	n.sessionWrites.Add(1)
	s.rev = rev
	s.done = ttl > 0

	if s.done {
		// The last record is in, with no will unless a delayed one is held.
		n.clearNoWill(s.nsID, rec.Epoch)
	}

	s.mu.Lock()
	s.savedGen = gen
	s.savedCursor = rec.Cursor
	s.mu.Unlock()

	return nil
}

// ownWriteRetries bounds how often persist adopts a revision its own
// earlier write left and writes again.
const ownWriteRetries = 3

// ownWrite reads s's key from the stream leader after a lost CAS and reports
// whether the record is still this node's at s's epoch, with its revision:
// the CAS lost to a write of this node that landed after its wait ran out.
// A read that fails, or a record of another owner or epoch, reports false.
func (n *clusterNode) ownWrite(ctx context.Context, s *session) (uint64, bool) {
	rec, rev, err := n.Sessions.Get(ctx, s.tenantID, s.rawID)
	if err != nil || rec.Owner != n.Config.NodeID || rec.Epoch != s.epoch.Load() {
		return 0, false
	}

	return rev, true
}

// lose marks s lost and disconnects its connection: 0x8E when another node
// took it, 0x98 when its key was deleted. The disconnect waits for the
// connection's CONNACK, so the DISCONNECT never comes first.
func (n *clusterNode) lose(s *session, deleted bool) {
	n.sessMu.Lock()

	if s.lost || s.ended {
		n.sessMu.Unlock()

		return
	}

	s.lost, s.deleted = true, deleted
	cl, acked := s.cl, s.connacked
	n.sessMu.Unlock()

	n.takeovers.Add(1)

	code := packets.ErrSessionTakenOver
	if deleted {
		code = packets.ErrAdministrativeAction
	}

	n.log.Info("tether: session moved off this node, disconnecting", slog.String("client", s.nsID), slog.Bool("deleted", deleted))

	go func() {
		timer := time.NewTimer(connackWait)
		defer timer.Stop()

		select {
		case <-acked:
		case <-timer.C:
		}

		if !cl.Closed() {
			disconnect(n.mochi, n.log, cl, code)
		}
	}()
}

// onOwnedLost is the owned-keys watch's callback: another node took the key
// of a session this node owned, or the key went. The watch can report a
// claim this node has already replaced, so the key is read again, under the
// session's lock, before anything is dropped.
func (n *clusterNode) onOwnedLost(tenantID, clientID string, _ *cluster.SessionRecord, _ bool) {
	if !validTenant(tenantID) {
		return
	}

	n.ownedEvents.Add(1)

	// No check of the node's sessions here: a CONNECT of this node may hold
	// the key without being registered yet (loadSession), and the watch has
	// already forgotten the key. verifyOwner waits for it under the lock.
	go n.verifyOwner(namespacedID(tenantID, clientID), tenantID, clientID)
}

// verifyOwnerRetry paces the reads of a takeover check that NATS did not
// answer, up to verifyOwnerRetryMax between them.
const (
	verifyOwnerRetry    = 100 * time.Millisecond
	verifyOwnerRetryMax = 5 * time.Second
)

// verifyOwner reads the key of a session the watch says this node lost and
// marks it lost unless the record still names this node at the epoch the
// session holds. It runs under the session's lock, so a CONNECT of this node
// in progress finishes first. A read that fails is tried again with backoff
// until there is an answer, the session is gone, or the broker stops.
func (n *clusterNode) verifyOwner(nsID, tenantID, clientID string) {
	quit := n.quit
	delay := verifyOwnerRetry

	for {
		unlock := n.locks.lock(nsID)

		s := n.sessionByID(nsID)
		if s == nil {
			unlock()

			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), sessionWriteWait)
		rec, _, err := n.Sessions.Get(ctx, tenantID, clientID)

		cancel()

		var deleted bool

		switch {
		case err == nil && rec.Owner == n.Config.NodeID && rec.Epoch == s.epoch.Load():
			unlock()

			return
		case err == nil:
		case errors.Is(err, tether.ErrNotFound):
			deleted = true
		default:
			unlock()
			n.log.Warn("tether: could not check a session the watch reported lost, retrying", slog.String("client", nsID),
				slog.Duration("in", delay), slog.Any("error", err))

			select {
			case <-quit:
				return
			case <-time.After(delay):
			}

			delay = min(2*delay, verifyOwnerRetryMax)

			continue
		}

		unlock()
		n.lose(s, deleted)

		return
	}
}

// startSessions starts the owned-keys watch and the flusher. Called once,
// from Start.
func (b *Broker) startSessions() error {
	n := b.cluster

	// The watch lives until Stop, so its context is not the start's: only
	// its first pass is bounded, by connectTimeout.
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(connectTimeout, cancel)

	n.quit = make(chan struct{})

	stop, err := n.Sessions.WatchOwned(ctx, n.Config.NodeID, n.onOwnedLost)
	if !timer.Stop() && err == nil {
		stop()
		err = context.DeadlineExceeded
	}

	if err != nil {
		cancel()

		return fmt.Errorf("tether/broker: watch owned sessions: %w", err)
	}

	n.watchStop = func() {
		stop()
		cancel()
	}
	n.flushStop, n.flushDone = make(chan struct{}), make(chan struct{})

	go b.runFlusher(n.flushStop, n.flushDone)

	return nil
}

// stopSessions stops the flusher and the watch. Called from Stop, after the
// clients are gone.
func (b *Broker) stopSessions() {
	n := b.cluster

	if n.flushStop != nil {
		close(n.flushStop)
		<-n.flushDone
		n.flushStop = nil
	}

	if n.watchStop != nil {
		n.watchStop()
		n.watchStop = nil
		close(n.quit)
	}
}

// runFlusher writes the changed records of the node's tracked sessions every
// flushInterval, in parallel up to sessionWriteSlots, and ends the sessions
// whose connection mochi closed without an OnDisconnect.
func (b *Broker) runFlusher(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	tick := time.NewTicker(b.cluster.flushInterval)
	defer tick.Stop()

	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			b.flush()
		}
	}
}

// flush is one round of runFlusher.
func (b *Broker) flush() {
	n := b.cluster
	now := time.Now()

	n.sessMu.Lock()

	var (
		dirty  []*session
		closed []*mqtt.Client
	)

	for _, s := range n.sess {
		if s.ended || s.lost {
			continue
		}

		if s.cl.Closed() {
			if s.closedAt.IsZero() {
				s.closedAt = now
			} else if now.Sub(s.closedAt) > closedGrace {
				closed = append(closed, s.cl)
			}

			continue
		}

		if s.tracked {
			dirty = append(dirty, s)
		}
	}

	n.sessMu.Unlock()

	var wg sync.WaitGroup

	for _, s := range dirty {
		if !n.needsWrite(s) {
			continue
		}

		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), sessionWriteWait)
			defer cancel()

			if err := n.persist(ctx, s, 0); err != nil && !errors.Is(err, errSessionLost) && !errors.Is(err, errFenced) {
				n.log.Warn("tether: session write failed", slog.String("client", s.nsID), slog.Any("error", err))
			}
		})
	}

	wg.Wait()

	// mochi ended these without OnDisconnect.
	for _, cl := range closed {
		expire := cl.Properties.ProtocolVersion == 5 && cl.Properties.Props.SessionExpiryInterval == 0 ||
			cl.Properties.ProtocolVersion < 5 && cl.Properties.Clean
		b.endSession(cl, expire)
	}
}

// needsWrite reports whether s's record changed since it was last written.
func (n *clusterNode) needsWrite(s *session) bool {
	lp := s.tt.livePos.Load()

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.gen != s.savedGen || s.cursorLocked(lp) != s.savedCursor
}

// mayResend runs at the CONNACK of a session resumed from the cell with QoS 2
// deliveries in flight, right before mochi resends them: the record is
// written once more, which only works while the key is still this node's. A
// node that resumed the session later may already have finished those
// deliveries with the client, and a resend from here would then be a new
// message to it. False means the key is gone: resend nothing.
func (n *clusterNode) mayResend(st *clientState) bool {
	s := st.sess
	if s == nil || s.restored == 0 {
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), sessionWriteWait)
	defer cancel()

	err := n.persist(ctx, s, 0)
	if err != nil && !errors.Is(err, errSessionLost) {
		n.log.Warn("tether: session write before the resend failed, resending nothing", slog.String("client", s.nsID), slog.Any("error", err))
	}

	return err == nil
}

// persistQoS2In is clusterNode.persistQoS2 in cluster mode: the pending entry
// of a QoS 2 publish (already in qos2In) is written to the session's record
// before the publish is appended, so a resend reaching any node finds its
// message ID. Sessions without a record to resume keep it in memory only.
func (n *clusterNode) persistQoS2In(tenantID, rawID string, _ uint16, _ string) error {
	s := n.sessionByID(namespacedID(tenantID, rawID))
	if s == nil || !s.tracked {
		return nil
	}

	s.touch()

	ctx, cancel := context.WithTimeout(context.Background(), n.appendAttempt)
	defer cancel()

	return n.persist(ctx, s, 0)
}

// deleteClusterSession is DeleteSession in cluster mode (ruling R7). A
// session this node serves is disconnected first and its end writes
// nothing; then the key is deleted at the revision read, whoever wrote it
// last, unless another node owns the session now. That node is asked to
// delete it over the control bus (deleteRemote, Task 13) when the broker has
// one and the call did not come from the bus itself (fromBus); otherwise it
// is an error naming the owner.
//
// epoch, when not zero, is the epoch the session must still have (the one a
// session.delete event names): a session of another epoch here, or a record
// of another epoch, is a client that connected again, and the delete fails
// with tether.ErrConflict.
func (b *Broker) deleteClusterSession(srv *mqtt.Server, tenantID, clientID, nsID string, epoch uint64, fromBus bool) error {
	n := b.cluster
	log := b.log.With(slog.String("client", nsID), slog.String("tenant", tenantID))

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	local, hasLocal := srv.Clients.Get(nsID)

	var cl *mqtt.Client
	if hasLocal {
		cl = local
	}

	st, ok := b.state.beginDelete(nsID, cl)
	if !ok {
		return deleteConflict(clientID, "another connection or delete for the client ID is in progress")
	}
	defer b.state.endDelete(nsID)

	s := n.sessionByID(nsID)
	if s != nil && epoch != 0 && s.epoch.Load() != epoch {
		return deleteConflict(clientID, "the client connected again")
	}

	if s != nil {
		n.sessMu.Lock()
		s.deleting = true
		live := s.cl
		n.sessMu.Unlock()

		if !live.Closed() {
			log.Info("tether: disconnecting client to delete its session")
			disconnect(srv, b.log, live, packets.ErrAdministrativeAction)
		}

		if st != nil && !waitEnded(st, b.endWait) && !live.Closed() {
			return deleteConflict(clientID, "the connection is still open")
		}

		// mochi skipped OnDisconnect, or it has not run yet: end it here.
		b.endSession(live, false)
	}

	if hasLocal {
		if cur, ok := srv.Clients.Get(nsID); ok && cur == local && local.Closed() {
			b.evictLocal(srv, local)
		}
	}

	// A session of this node that just ended may still be writing its last
	// record; its end holds the session's lock until that is in.
	unlock := n.locks.lock(nsID)
	locked := true

	defer func() {
		if locked {
			unlock()
		}
	}()

	rec, rev, err := n.Sessions.Get(ctx, tenantID, clientID)
	if errors.Is(err, tether.ErrNotFound) {
		if hasLocal {
			return nil
		}

		return noSession(clientID)
	}

	if err != nil {
		return fmt.Errorf("tether/broker: delete session %q: %w", clientID, err)
	}

	if epoch != 0 && rec.Epoch != epoch {
		return deleteConflict(clientID, "the client connected again")
	}

	if rec.Owner != "" && rec.Owner != n.Config.NodeID {
		if fromBus || n.Control == nil {
			return fmt.Errorf("tether/broker: delete session %q: %w: owned by node %q", clientID, tether.ErrConflict, rec.Owner)
		}

		// The owner deletes it. Neither the session's lock nor anything
		// else is held across the publish and the wait; the tombstone
		// stays, so this node refuses the client ID until it is done.
		unlock()

		locked = false

		return b.deleteRemote(tenantID, clientID, rec.Owner, rec.Epoch)
	}

	if err := n.Sessions.Delete(ctx, tenantID, clientID, rev); err != nil {
		if errors.Is(err, cluster.ErrLostCAS) {
			return deleteConflict(clientID, "the session changed while it was being deleted")
		}

		return fmt.Errorf("tether/broker: delete session %q: %w", clientID, err)
	}

	n.dropQoS2(nsID)
	log.Info("tether: session deleted")

	return nil
}

// clusterClients is Clients in cluster mode: the tenant's sessions in the
// cell, with this node's open connections described from their state. A
// session another node serves is listed as connected, without a remote
// address. If the cell cannot be read, only this node's sessions are listed.
func (b *Broker) clusterClients(local []ClientInfo, tenantID string) []ClientInfo {
	n := b.cluster

	ctx, cancel := context.WithTimeout(context.Background(), sessionWriteWait)
	defer cancel()

	entries, err := n.Sessions.Entries(ctx, tenantID)
	if err != nil {
		n.log.Warn("tether: listing the tenant's sessions in the cell failed", slog.String("tenant", tenantID), slog.Any("error", err))

		return local
	}

	seen := make(map[string]struct{}, len(local))
	for _, ci := range local {
		seen[ci.ClientID] = struct{}{}
	}

	out := local

	for _, e := range entries {
		if _, ok := seen[e.ClientID]; ok {
			continue
		}

		rec := e.Record
		subs := make([]string, 0, len(rec.Subscriptions))

		for _, sr := range rec.Subscriptions {
			subs = append(subs, sr.Filter)
		}

		slices.Sort(subs)

		out = append(out, ClientInfo{
			ClientID:        e.ClientID,
			Username:        rec.Username,
			DeviceID:        rec.DeviceID,
			Connected:       rec.Owner != "",
			ProtocolVersion: rec.ProtocolVersion,
			Subscriptions:   subs,
			SessionExpiry:   rec.ExpirySeconds,
		})
	}

	slices.SortFunc(out, func(x, y ClientInfo) int { return strings.Compare(x.ClientID, y.ClientID) })

	return out
}

// checkClientID is ruling R13 at CONNECT: a client ID the cell cannot carry
// is refused before any KV call.
func checkClientID(rawID string) error {
	if err := subject.CheckClientID(rawID); err != nil {
		return fmt.Errorf("tether/broker: client ID: %w", err)
	}

	return nil
}
