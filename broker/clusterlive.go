package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/topic"
)

// Liveness in cluster mode (spec "Ownership, takeover and liveness" and
// "Partitions"; rulings R18, R30, R31).
//
// Members tells the broker three things. OnFence: no heartbeat of this node
// has been acknowledged for half the lease, so the node can no longer count on
// holding its sessions. It drops every client with 0x88 and refuses new
// CONNECTs until OnUnfence, and writes nothing for the sessions that end
// meanwhile: their keys stay as they were, owned by this node and carrying
// their wills, for whoever reclaims them. OnPeerDead: a peer has been silent
// for the lease plus 2 s. It kicks a sweep, which also runs once per lease:
// the node reclaims every key of every dead peer it leads for that it has not
// cleared yet (sweepDead), so no dead node's keys are left behind when a
// leader dies mid-reclaim or several nodes die together. OnUnfence: the node
// reclaims the keys it still owns but no longer serves, those it dropped
// while fenced, without the wills nobody owed (noWill).
//
// Reclaiming a key is one compare-and-set from the record the scan read, and
// only the node whose write wins publishes the record's will (R18), so two
// nodes that both think they lead, or a ghost node ID declared dead at
// startup, publish nothing twice. A record with no will left to wait for is
// written back offline (Owner "", no will, the rest of its expiry as TTL; a
// clean session's key is deleted). A will whose MQTT 5 delay has not run out,
// counted from DisconnectedAt or from now for a session that was connected, is
// taken over instead: the reclaiming node writes itself as the owner, keeps
// the will and publishes it when the delay ends, unless the session came back
// first. So a record's Owner always names the node that answers for its will.
//
// Wills of local connections (R30): a will mochi fires for a client of this
// node goes to the cell as the tenant's message. QoS 0 goes through the
// Router and mochi delivers it here as usual; QoS 1 and 2 are appended to the
// log and mochi's local delivery is withheld, so every node, this one
// included, delivers it once from its reader. A will with a delay is never
// left to mochi, whose delayed wills skip every hook: the session's last
// record keeps the will with this node as its owner, and this node publishes
// it when the delay ends, unless the record moved (a reconnect anywhere).
// The delay is the lower of the will delay and the session's expiry, as MQTT
// 5 says: a session that ends at once publishes its will at once.

const (
	// fenceDropSlots bounds the disconnects a fence runs at once.
	fenceDropSlots = 64
	// reclaimSlots bounds the keys one reclaim writes at once.
	reclaimSlots = 16
	// willPublishWait bounds one will's publish, the log commit included.
	willPublishWait = 5 * time.Second
	// willRetry and willRetryMax pace the retries of a will's append and of
	// a delayed will's check.
	willRetry    = 200 * time.Millisecond
	willRetryMax = 5 * time.Second
)

// errFenced is what a session write returns while the node is fenced.
var errFenced = fmt.Errorf("tether/broker: session write: %w", cluster.ErrFenced)

// pendingWill is a delayed will this node answers for: the will of the
// session (tenantID, clientID) at epoch, due when its timer fires.
type pendingWill struct {
	tenantID, clientID string
	epoch              uint64
	timer              *time.Timer
}

// liveness is the broker's side of Members, on clusterNode.
type liveness struct {
	// fenced is set by OnFence and cleared by OnUnfence.
	fenced atomic.Bool

	// ctx lives until Stop; the goroutines liveness starts run on it and
	// Stop waits for them (wg).
	ctx    context.Context //nolint:containedctx // the lifetime of the liveness goroutines
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// willsMu guards wills, the delayed wills this node answers for by
	// namespaced client ID; noWill; deadGen and cleared; and stopped, set by
	// Stop: nothing starts after.
	willsMu sync.Mutex
	wills   map[string]*pendingWill
	stopped bool

	// noWill holds, by namespaced client ID, the epoch of a key of this node
	// whose will must not be published although the record may still carry
	// it: the session ended without owing one (a normal DISCONNECT, a will
	// already published or withheld) or a refused CONNECT's giveBack has not
	// gone through, and the write that clears it failed or was skipped. A
	// reclaim of this node's own keys settles such a key without its will.
	// It is memory only: after a real crash the cell can still publish one.
	noWill map[string]uint64

	// deadGen counts, per peer, the OnPeerDead calls this node saw; cleared
	// holds the deadGen at which a sweep last settled every key of that peer
	// (sweepDead).
	deadGen, cleared map[string]uint64
	sweepKick        chan struct{}

	// willFault, when a test sets it before Start, runs before every attempt
	// to send a will and fails the attempt with its error.
	willFault func() error

	// Counters for tests: wills published to the cell, delayed wills
	// cancelled, keys reclaimed (written back or taken over), clients
	// dropped by a fence, sweeps that read the bucket, wills given up on
	// because they can never be sent.
	willsSent, willsCancelled, reclaimed, fenceDrops, sweeps, willsDropped atomic.Uint64
}

func newLiveness() liveness {
	ctx, cancel := context.WithCancel(context.Background())

	return liveness{
		ctx: ctx, cancel: cancel,
		wills:     make(map[string]*pendingWill),
		noWill:    make(map[string]uint64),
		deadGen:   make(map[string]uint64),
		cleared:   make(map[string]uint64),
		sweepKick: make(chan struct{}, 1),
	}
}

// markNoWill records that the key nsID at epoch must settle without its
// will (noWill).
func (n *clusterNode) markNoWill(nsID string, epoch uint64) {
	n.willsMu.Lock()
	n.noWill[nsID] = epoch
	n.willsMu.Unlock()
}

// clearNoWill forgets nsID's noWill entry when it is for epoch, or whatever
// its epoch when epoch is 0.
func (n *clusterNode) clearNoWill(nsID string, epoch uint64) {
	n.willsMu.Lock()
	defer n.willsMu.Unlock()

	if e, ok := n.noWill[nsID]; ok && (epoch == 0 || e == epoch) {
		delete(n.noWill, nsID)
	}
}

// willBarred reports whether the key nsID at epoch must settle without its
// will.
func (n *clusterNode) willBarred(nsID string, epoch uint64) bool {
	n.willsMu.Lock()
	defer n.willsMu.Unlock()

	e, ok := n.noWill[nsID]

	return ok && e == epoch
}

// watchMembers registers the broker's callbacks on the cell's Members, when
// there is one. Called once, from New: Members wants them before its Start.
func (b *Broker) watchMembers() {
	m := b.cluster.Members
	if m == nil {
		return
	}

	m.OnFence(b.onFence)
	m.OnUnfence(b.onUnfence)
	m.OnPeerDead(b.onPeerDead)
}

// server returns the mochi server injections go to, nil before Start and
// once Stop began.
func (n *clusterNode) server() *mqtt.Server {
	n.gate.RLock()
	defer n.gate.RUnlock()

	return n.srv
}

// goLive runs fn on a goroutine Stop waits for, unless Stop has begun. It
// reports whether fn will run.
func (n *clusterNode) goLive(fn func()) bool {
	n.willsMu.Lock()
	defer n.willsMu.Unlock()

	if n.stopped {
		return false
	}

	n.wg.Go(fn)

	return true
}

// goPublishHeld publishes a will whose key this node has settled (publishHeld)
// on a goroutine Stop waits for. Once Stop has begun nothing more starts, and
// the node holds the will's only copy, so a will dropped then is logged at
// Warn.
func (b *Broker) goPublishHeld(tenantID, clientID string, w *cluster.WillRecord, msgID string) {
	n := b.cluster

	if !n.goLive(func() { b.publishHeld(tenantID, clientID, w, msgID) }) {
		n.log.Warn("tether: will lost, its key was settled as the broker stopped", slog.String("client", namespacedID(tenantID, clientID)),
			slog.String("topic", w.Topic))
	}
}

// stopLive cancels the pending delayed wills and the reclaims, and waits for
// the goroutines liveness started. The keys of the cancelled wills keep this
// node as their owner, so the cell publishes them once this node is declared
// dead. Called from Stop, after the clients are gone.
func (b *Broker) stopLive() {
	n := b.cluster

	n.willsMu.Lock()
	n.stopped = true

	for id, pw := range n.wills {
		pw.timer.Stop()
		delete(n.wills, id)
	}

	n.willsMu.Unlock()

	n.cancel()
	n.wg.Wait()
}

// onFence is Members.OnFence: drop every client and refuse new ones. It runs
// on the Members dispatcher, so the disconnects run on their own goroutine.
func (b *Broker) onFence() {
	n := b.cluster
	if !n.fenced.CompareAndSwap(false, true) {
		return
	}

	n.metrics.ClusterFenced(true)

	srv := n.server()
	if srv == nil {
		return
	}

	n.log.Warn("tether: fenced from the cell, dropping every client and refusing new ones")
	n.goLive(func() { n.dropClients(srv) })
}

// onUnfence is Members.OnUnfence: accept CONNECTs again and reclaim the keys
// this node still owns but dropped while it was fenced.
func (b *Broker) onUnfence() {
	n := b.cluster
	n.fenced.Store(false)
	n.metrics.ClusterFenced(false)

	if n.server() == nil {
		return
	}

	n.log.Info("tether: unfenced, accepting connections again")
	n.goLive(func() { b.reclaim(n.Config.NodeID) })
}

// onPeerDead is Members.OnPeerDead: a sweep runs now (sweepDead). Every node
// that is not fenced gets the call; the sweep does the work only on the node
// that leads for the dead peer.
func (b *Broker) onPeerDead(node string) {
	n := b.cluster
	if n.server() == nil {
		return
	}

	n.willsMu.Lock()
	n.deadGen[node]++
	n.willsMu.Unlock()

	select {
	case n.sweepKick <- struct{}{}:
	default:
	}
}

// sweepLoop runs sweepDead at every OnPeerDead and once per lease, until
// Stop. Started from Start when Members is set.
func (b *Broker) sweepLoop() {
	n := b.cluster

	tick := time.NewTicker(n.lease())
	defer tick.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.sweepKick:
		case <-tick.C:
		}

		b.sweepDead()
	}
}

// sweepDead reclaims, in one pass over the bucket, the keys of every peer
// Members holds dead that this node leads for and has not cleared since the
// peer last died. OnPeerDead fires once per death and only the leader of
// that moment acts on it, so a leader that dies or stops mid-reclaim, or
// several nodes dying together (a dead node whose leader was itself about to
// die), would otherwise leave keys owned by a dead node forever. Running
// again is safe: every key is a CAS (R18). A pass that met errors clears
// nothing and the next tick tries again.
func (b *Broker) sweepDead() {
	n := b.cluster
	if n.fenced.Load() || n.server() == nil {
		return
	}

	var lead []string

	for _, d := range n.Members.Dead() {
		if n.Members.IsLeaderFor(d) {
			lead = append(lead, d)
		}
	}

	gens := make(map[string]uint64, len(lead))
	nodes := lead[:0]

	n.willsMu.Lock()

	for _, d := range lead {
		if c, ok := n.cleared[d]; ok && c == n.deadGen[d] {
			continue
		}

		gens[d] = n.deadGen[d]
		nodes = append(nodes, d)
	}

	n.willsMu.Unlock()

	if len(nodes) == 0 {
		return
	}

	n.sweeps.Add(1)

	if !b.reclaimPass(nodes...) {
		return
	}

	n.willsMu.Lock()
	for d, g := range gens {
		n.cleared[d] = g
	}
	n.willsMu.Unlock()
}

// dropClients disconnects every network client of srv with 0x88, in
// parallel up to fenceDropSlots; a 3.1.1 client, which has no DISCONNECT to
// receive, is closed. No registry lock is held here: each connection's end
// runs the hooks on its own goroutine.
func (n *clusterNode) dropClients(srv *mqtt.Server) {
	slots := make(chan struct{}, fenceDropSlots)

	var wg sync.WaitGroup

	for _, cl := range srv.Clients.GetAll() {
		if cl.Net.Inline || cl.Closed() {
			continue
		}

		slots <- struct{}{}

		wg.Go(func() {
			defer func() { <-slots }()

			n.dropClient(srv, cl, "")
		})
	}

	wg.Wait()
}

// dropClient ends one connection of a fenced node: 0x88 for 5.0, closed for
// 3.1.1. name is the client ID for the logs, which dropClients leaves empty
// because a connection it finds may not have written its ID yet.
func (n *clusterNode) dropClient(srv *mqtt.Server, cl *mqtt.Client, name string) {
	n.fenceDrops.Add(1)

	if cl.Properties.ProtocolVersion < 5 {
		cl.Stop(packets.ErrServerUnavailable)

		return
	}

	disconnect(srv, n.log, cl, name, packets.ErrServerUnavailable)
}

// ClusterHealth reports whether this node can act for its cell: nil when it
// can, or when the broker runs without a cluster. It returns an error
// wrapping cluster.ErrFenced while the node is fenced, and one wrapping
// cluster.ErrUnavailable when its NATS connection is down or JetStream has
// not answered for longer than Lease/2 (cluster.Conn.Healthy: a meta leader
// election, during which no node gets an answer, does not fail it).
func (b *Broker) ClusterHealth(ctx context.Context) error {
	n := b.cluster
	if n == nil {
		return nil
	}

	if n.fenced.Load() || n.Members != nil && n.Members.Fenced() {
		return fmt.Errorf("tether/broker: cluster health: %w", cluster.ErrFenced)
	}

	if err := n.Conn.Healthy(ctx); err != nil {
		return fmt.Errorf("tether/broker: cluster health: %w", err)
	}

	return nil
}

// clusterWill is OnWill in cluster mode for a network client whose will
// passed the grants (rel is its tenant-relative topic): it sends the will to
// the cell, or leaves it to the session's last record, and returns what mochi
// publishes locally (ruling R30).
func (b *Broker) clusterWill(cl *mqtt.Client, st *clientState, rel string, will mqtt.Will) mqtt.Will {
	n := b.cluster
	withheld := mqtt.Will{Flag: will.Flag, TopicName: withheldTopic}

	s := st.sess

	n.sessMu.Lock()
	mine := s != nil && s.cl == cl && !s.lost && !s.ended
	deleting := s != nil && s.deleting
	n.sessMu.Unlock()

	if n.fenced.Load() {
		// The key keeps the will (finish writes nothing): the node that
		// reclaims it publishes it.
		if mine {
			s.owe("")
		}

		return withheld
	}

	var delay uint32
	if cl.Properties.ProtocolVersion == 5 {
		delay = min(will.WillDelayInterval, sessionExpiry(cl, st.expiryCap))
	}

	if delay > 0 && s != nil {
		s.mu.Lock()
		tracked := s.tracked.Load()
		s.mu.Unlock()

		if deleting || !tracked {
			// The session ends now, and its will goes with it.
			delay = 0
		}
	}

	if delay > 0 {
		if !mine {
			// A new connection holds the session: a delayed will is
			// cancelled by it.
			n.willsCancelled.Add(1)

			return withheld
		}

		// finish writes the record with the will and this node as its
		// owner, then waits out the delay (scheduleWill).
		s.mu.Lock()
		s.willDelay = time.Duration(delay) * time.Second
		s.willAt = time.Now()
		s.mu.Unlock()

		return withheld
	}

	w := &cluster.WillRecord{Topic: rel, Payload: will.Payload, QoS: will.Qos, Retain: will.Retain}
	if len(will.User) > 0 {
		w.Props.User = make([]cluster.UserProperty, len(will.User))
		for i, u := range will.User {
			w.Props.User[i] = cluster.UserProperty{Key: u.Key, Value: u.Val}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), willPublishWait)
	defer cancel()

	msgID := newMsgID()

	// A QoS 0 will is delivered on this node by the injector (local), so a
	// retained one carries its retain flag to Retain As Published
	// subscribers without mochi storing it; the log delivers a QoS 1 or 2
	// one here too. mochi's own publish is withheld either way.
	if err := b.publishWill(ctx, st.tenantID, st.rawID, w, will.Qos == 0, msgID); err != nil {
		n.log.Warn("tether: will not published to the cell", slog.String("client", cl.ID), slog.Any("error", err))

		if will.Qos > 0 && mine && !errors.Is(err, errWillDropped) {
			// The record keeps it, and finish has it settled like a
			// reclaimed key, under the same message ID in case an append
			// committed after all. (A QoS 0 will was delivered here
			// anyway.)
			s.owe(msgID)
		}
	}

	return withheld
}

// publishWill publishes w as tenantID's message from client rawID: QoS 0
// through the Router, QoS 1 and 2 through the log, retrying with backoff
// until it goes out or ctx ends. clusterWill bounds ctx by willPublishWait;
// a will whose key this node already settled (reclaim, a delayed will) uses
// the broker's lifetime, so it retries until Stop. Every attempt appends
// under msgID ("" mints one), so the log deduplicates a retry, and a later
// publish of the same will after an append that errored but committed. A
// will that can never be sent (permanentWillErr) is dropped at once, logged
// at Error, with an error wrapping errWillDropped.
//
// local makes this node deliver a QoS 0 will itself (the Router never
// delivers to its own node), which is what mochi does for a will it fires.
// A retained will, once out, is written to the cell's retained store and
// this node's cache (clusterretained.go) under the same ctx; when ctx ends
// first the write goes on in the background.
func (b *Broker) publishWill(ctx context.Context, tenantID, rawID string, w *cluster.WillRecord, local bool, msgID string) error {
	n := b.cluster

	if msgID == "" {
		msgID = newMsgID()
	}

	if local && w.QoS == 0 {
		err := b.inject(tenantID, w.Topic, w.Payload, 0, w.Props, namespacedID(tenantID, rawID), 0, w.Retain)
		if err != nil && !errors.Is(err, errNotInjecting) {
			n.log.Warn("tether: will not delivered on this node", slog.String("tenant", tenantID), slog.Any("error", err))
		}
	}

	delay := willRetry

	for {
		err := b.sendWill(ctx, tenantID, rawID, w, msgID)
		if err == nil {
			break
		}

		if permanentWillErr(err) {
			// Retrying cannot help (too large for NATS, refused as invalid):
			// the will is dropped.
			n.willsDropped.Add(1)
			n.log.Error("tether: will can never be published, dropped", slog.String("client", namespacedID(tenantID, rawID)),
				slog.String("topic", w.Topic), slog.Any("error", err))

			return fmt.Errorf("tether/broker: publish will: %w: %w", errWillDropped, err)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("tether/broker: publish will: %w", err)
		case <-time.After(delay):
		}

		delay = min(2*delay, willRetryMax)
	}

	if w.QoS > 0 {
		n.appends.Add(1)
	}

	n.willsSent.Add(1)

	if w.Retain {
		rw := n.newRetainWrite(tenantID, w.Topic, w.Payload, w.QoS, w.Props, rawID)
		if err := n.storeRetained(ctx, rw, true); err != nil {
			n.log.Warn("tether: retained will not stored yet", slog.String("client", namespacedID(tenantID, rawID)), slog.Any("error", err))
		}
	}

	return nil
}

// errWillDropped is what publishWill returns for a will it gave up on at
// once, because no retry could get it out.
var errWillDropped = errors.New("tether/broker: will dropped")

// jsErrMessageTooLarge is nats-server's JSStreamMessageExceedsMaximumErr
// (10054). nats.go has no exported constant for it.
const jsErrMessageTooLarge jetstream.ErrorCode = 10054

// permanentWillErr reports whether a failed will send can never succeed:
// a message over NATS's max_payload or the stream's message size limit, or
// one the cluster refuses as invalid.
func permanentWillErr(err error) bool {
	if errors.Is(err, tether.ErrInvalidInput) || errors.Is(err, nats.ErrMaxPayload) || errors.Is(err, nats.ErrBadSubject) {
		return true
	}

	var api *jetstream.APIError

	return errors.As(err, &api) && api.ErrorCode == jsErrMessageTooLarge
}

// sendWill is one attempt of publishWill.
func (b *Broker) sendWill(ctx context.Context, tenantID, rawID string, w *cluster.WillRecord, msgID string) error {
	n := b.cluster

	if fault := n.willFault; fault != nil {
		if err := fault(); err != nil {
			return err
		}
	}

	if w.QoS == 0 {
		if err := n.router.Publish(tenantID, w.Topic, w.Payload, w.Props, w.Retain); err != nil {
			n.routeErrors.Add(1)

			return err
		}

		n.published.Add(1)

		return nil
	}

	_, _, err := n.Log.Append(ctx, cluster.LogMessage{
		Tenant: tenantID, Topic: w.Topic, Payload: w.Payload, QoS: w.QoS, Retain: w.Retain, Props: w.Props,
		Origin: n.Config.NodeID, ClientID: rawID,
	}, msgID)

	return err
}

// validWill reports whether a will read from a record is one this node may
// publish for tenantID: the record comes off NATS, so it is checked like a
// routed message.
func validWill(tenantID string, w *cluster.WillRecord) bool {
	return validTenant(tenantID) && w.QoS <= 2 && topic.Valid(w.Topic, false, subject.MaxTopicBytes) == nil && !hasSysPrefix(w.Topic)
}

// willWait is how long after now a record's will is due: its delay, capped
// at the session's expiry, counted from discAt.
func willWait(w *cluster.WillRecord, expirySeconds uint32, discAt, now time.Time) time.Duration {
	delay := time.Duration(min(w.DelaySeconds, expirySeconds)) * time.Second

	return discAt.Add(delay).Sub(now)
}

// expiryLeft is how much of a record's session expiry is left at now,
// counted from discAt, rounded up to the whole second a key TTL needs; zero
// when none is left.
func expiryLeft(rec *cluster.SessionRecord, discAt, now time.Time) time.Duration {
	left := discAt.Add(time.Duration(rec.ExpirySeconds) * time.Second).Sub(now)
	if left <= 0 {
		return 0
	}

	return left.Truncate(time.Second) + time.Second
}

// willKeyGrace is how long a session key that holds a delayed will lives
// past the will's due time: twice the time a silent node takes to be
// declared dead (Lease + 2 s). The will goes out only once a settle of the
// key wins its CAS, and a key that has expired reads as a will a reconnect
// cancelled, so the key must outlive the timer of the node holding the will
// (a delay at or past the session's expiry is cut short by it, which put the
// key's TTL and the timer on the same instant) and, if that node dies, the
// reclaim by its leader. The settle ends the session, so the grace only
// shows when the holder is slow, fenced or dead.
func (n *clusterNode) willKeyGrace() time.Duration {
	return 2 * (n.lease() + 2*time.Second)
}

// lease is the cell's Config.Lease, or cluster.DefaultLease when it is not
// set.
func (n *clusterNode) lease() time.Duration {
	if n.Config.Lease <= 0 {
		return cluster.DefaultLease
	}

	return n.Config.Lease
}

// willKeyTTL is the TTL of a session key whose will is due after wait, when
// left of the session's expiry remains: left, unless the will is due less
// than willKeyGrace before the key would expire.
func (n *clusterNode) willKeyTTL(left, wait time.Duration) time.Duration {
	return max(left, wait+n.willKeyGrace())
}

// reclaim settles the keys this node owns but no session of it serves (at
// the unfence, and once at Start): written back offline with their wills
// published, or held while a delayed will waits. A pass that met errors runs
// again (each key is a CAS, so a second pass does nothing twice), with
// backoff, until one succeeds or the broker stops; while the node is fenced
// it waits. Dead peers' keys go through sweepDead.
func (b *Broker) reclaim(node string) {
	n := b.cluster
	delay := willRetry

	for {
		if !n.fenced.Load() && b.reclaimPass(node) {
			return
		}

		select {
		case <-n.ctx.Done():
			return
		case <-time.After(delay):
		}

		delay = min(2*delay, willRetryMax)
	}
}

// reclaimID is reclaim for one key of this node: the session that just
// ended here owing its will (finish), whose append went out under msgID, if
// any. It runs once the session's lock is free, and retries like reclaim.
func (b *Broker) reclaimID(tenantID, clientID, msgID string) {
	n := b.cluster
	delay := willRetry
	self := n.Config.NodeID

	for {
		if !n.fenced.Load() {
			err := b.reclaimKey(self, cluster.SessionEntry{Tenant: tenantID, ClientID: clientID}, msgID)
			if err == nil {
				return
			}

			if n.ctx.Err() == nil {
				n.log.Warn("tether: settling a session that owes its will failed, retrying",
					slog.String("client", namespacedID(tenantID, clientID)), slog.Any("error", err))
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

// reclaimPass reads the keys nodes own in one pass over the bucket and
// reclaims each. It reports whether every key was dealt with.
func (b *Broker) reclaimPass(nodes ...string) bool {
	n := b.cluster

	ctx, cancel := context.WithTimeout(n.ctx, connectTimeout)
	entries, err := n.Sessions.OwnedBy(ctx, nodes...)

	cancel()

	if err != nil {
		if n.ctx.Err() == nil {
			n.log.Warn("tether: listing the sessions of nodes failed", slog.Any("nodes", nodes), slog.Any("error", err))
		}

		return n.ctx.Err() != nil
	}

	if len(entries) > 0 {
		n.log.Warn("tether: reclaiming sessions", slog.Any("owners", nodes), slog.Int("keys", len(entries)))
	}

	slots := make(chan struct{}, reclaimSlots)

	var (
		wg     sync.WaitGroup
		failed atomic.Bool
	)

	for _, e := range entries {
		slots <- struct{}{}

		wg.Go(func() {
			defer func() { <-slots }()

			if err := b.reclaimKey(e.Record.Owner, e, ""); err != nil {
				failed.Store(true)

				if n.ctx.Err() == nil {
					n.log.Warn("tether: reclaiming a session failed", slog.String("owner", e.Record.Owner),
						slog.String("client", namespacedID(e.Tenant, e.ClientID)), slog.Any("error", err))
				}
			}
		})
	}

	wg.Wait()

	return !failed.Load() || n.ctx.Err() != nil
}

// reclaimKey reclaims one key of node. A lost CAS means the key moved (a
// reconnect, or another node reclaimed it first): nothing to do. For this
// node's own keys the record is read again under the session's lock, and
// e's record and revision are not used; a key in noWill settles without its
// will. msgID is the log message ID the will goes out under ("" mints one).
//
// Once the CAS that clears the will is won, this node holds the will's only
// copy, so its publish retries until it goes out or the broker stops.
func (b *Broker) reclaimKey(node string, e cluster.SessionEntry, msgID string) error {
	n := b.cluster
	self := node == n.Config.NodeID
	rec, rev := e.Record, e.Revision
	nsID := namespacedID(e.Tenant, e.ClientID)

	ctx, cancel := context.WithTimeout(n.ctx, sessionWriteWait)
	defer cancel()

	if self {
		// A CONNECT of this node holds the session's lock from its acquire
		// until the session is registered, so under it a key this node
		// serves is told from one it dropped.
		unlock := n.locks.lock(nsID)
		defer unlock()

		var err error

		rec, rev, err = n.Sessions.Get(ctx, e.Tenant, e.ClientID)
		if errors.Is(err, tether.ErrNotFound) {
			n.clearNoWill(nsID, 0)

			return nil
		}

		if err != nil {
			return err
		}

		if rec.Owner != node || n.serves(nsID, rec.Epoch) {
			return nil
		}
	}

	now := time.Now().UTC()

	discAt := rec.DisconnectedAt
	if discAt.IsZero() {
		discAt = now
	}

	will := rec.Will
	barred := self && n.willBarred(nsID, rec.Epoch)

	if barred {
		// The session ended without owing a will: it settles without it.
		will = nil
	}

	if will != nil && !validWill(e.Tenant, will) {
		n.log.Warn("tether: dropping a will that is not a tenant topic", slog.String("client", nsID), slog.String("topic", will.Topic))

		will = nil
	}

	if will != nil {
		if wait := willWait(will, rec.ExpirySeconds, discAt, now); wait > 0 {
			// Not due yet: this node answers for it now.
			next := *rec
			next.Owner = n.Config.NodeID
			next.DisconnectedAt = discAt

			_, err := n.Sessions.Expire(ctx, e.Tenant, e.ClientID, rev, &next, n.willKeyTTL(expiryLeft(rec, discAt, now), wait))
			if errors.Is(err, cluster.ErrLostCAS) {
				return nil
			}

			if err != nil {
				return err
			}

			n.reclaimed.Add(1)
			b.scheduleWill(e.Tenant, e.ClientID, rec.Epoch, wait)

			return nil
		}
	}

	won, err := n.settle(ctx, e.Tenant, e.ClientID, rec, rev, discAt)
	if err != nil || !won {
		return err
	}

	n.reclaimed.Add(1)

	if barred {
		n.clearNoWill(nsID, rec.Epoch)
	}

	if will != nil {
		// On its own goroutine: the publish may retry until Stop, and must
		// hold neither the session's lock (a CONNECT of the client waits on
		// it) nor the reclaim's slot (the sweep waits on those).
		b.goPublishHeld(e.Tenant, e.ClientID, will, msgID)
	}

	return nil
}

// publishHeld publishes a will whose key this node has already settled, so
// it holds the only copy: publishWill on the broker's lifetime, retried
// until Stop unless it can never succeed.
func (b *Broker) publishHeld(tenantID, clientID string, w *cluster.WillRecord, msgID string) {
	n := b.cluster

	err := b.publishWill(n.ctx, tenantID, clientID, w, true, msgID)
	if err != nil && !errors.Is(err, errWillDropped) {
		n.log.Warn("tether: will lost at Stop before it went out", slog.String("client", namespacedID(tenantID, clientID)), slog.Any("error", err))
	}
}

// serves reports whether a session of this node serves nsID at epoch, or
// waits out its delayed will.
func (n *clusterNode) serves(nsID string, epoch uint64) bool {
	if s := n.sessionByID(nsID); s != nil && s.epoch.Load() == epoch {
		n.sessMu.Lock()
		ended := s.ended
		n.sessMu.Unlock()

		if !ended {
			return true
		}
	}

	n.willsMu.Lock()
	defer n.willsMu.Unlock()

	pw := n.wills[nsID]

	return pw != nil && pw.epoch == epoch
}

// settle writes rec back as an offline session with no will, as a CAS on
// rev: Owner "", disconnected at discAt, with the rest of its expiry as the
// key's TTL, or deleted when none is left (a clean session). It reports
// whether the write won.
func (n *clusterNode) settle(ctx context.Context, tenantID, clientID string, rec *cluster.SessionRecord, rev uint64, discAt time.Time) (bool, error) {
	var err error

	if left := expiryLeft(rec, discAt, time.Now()); left <= 0 {
		err = n.Sessions.Delete(ctx, tenantID, clientID, rev)
	} else {
		next := *rec
		next.Owner = ""
		next.Will = nil
		next.DisconnectedAt = discAt
		_, err = n.Sessions.Expire(ctx, tenantID, clientID, rev, &next, left)
	}

	if errors.Is(err, cluster.ErrLostCAS) {
		return false, nil
	}

	return err == nil, err
}

// scheduleWill makes this node publish the will of (tenantID, clientID) at
// epoch after wait, unless its key moves first.
func (b *Broker) scheduleWill(tenantID, clientID string, epoch uint64, wait time.Duration) {
	n := b.cluster
	nsID := namespacedID(tenantID, clientID)

	n.willsMu.Lock()
	defer n.willsMu.Unlock()

	if n.stopped {
		return
	}

	if old := n.wills[nsID]; old != nil {
		old.timer.Stop()
	}

	pw := &pendingWill{tenantID: tenantID, clientID: clientID, epoch: epoch}
	pw.timer = time.AfterFunc(max(wait, 0), func() {
		n.goLive(func() { b.fireWill(pw) })
	})
	n.wills[nsID] = pw
}

// fireWill publishes a delayed will that is due, if its key still names this
// node as the owner at the epoch the will was left at and still carries the
// will: a CONNECT anywhere moves both, which cancels the will. The key is
// written back offline first, and only a won write publishes. A read or
// write that fails is tried again until it is answered or the broker stops.
func (b *Broker) fireWill(pw *pendingWill) {
	n := b.cluster
	nsID := namespacedID(pw.tenantID, pw.clientID)

	defer func() {
		n.willsMu.Lock()
		if n.wills[nsID] == pw {
			delete(n.wills, nsID)
		}
		n.willsMu.Unlock()
	}()

	delay := willRetry

	for {
		published, retry := b.tryFire(pw)
		if !retry {
			if !published {
				n.willsCancelled.Add(1)
			}

			return
		}

		select {
		case <-n.ctx.Done():
			return
		case <-time.After(delay):
		}

		delay = min(2*delay, willRetryMax)
	}
}

// tryFire is one attempt of fireWill. retry says it met an error.
func (b *Broker) tryFire(pw *pendingWill) (published, retry bool) {
	n := b.cluster

	if n.fenced.Load() {
		return false, true
	}

	ctx, cancel := context.WithTimeout(n.ctx, sessionWriteWait)
	defer cancel()

	rec, rev, err := n.Sessions.Get(ctx, pw.tenantID, pw.clientID)

	switch {
	case errors.Is(err, tether.ErrNotFound):
		return false, false
	case err != nil:
		return false, n.ctx.Err() == nil
	case rec.Owner != n.Config.NodeID || rec.Epoch != pw.epoch || rec.Will == nil:
		return false, false
	}

	discAt := rec.DisconnectedAt
	if discAt.IsZero() {
		discAt = time.Now().UTC()
	}

	won, err := n.settle(ctx, pw.tenantID, pw.clientID, rec, rev, discAt)
	if err != nil {
		return false, n.ctx.Err() == nil
	}

	if !won {
		// The key moved between the read and the write: read it again.
		return false, true
	}

	if !validWill(pw.tenantID, rec.Will) {
		return false, false
	}

	// This node holds the will's only copy now. No lock is held here, so
	// the publish runs inline, retried until Stop.
	b.publishHeld(pw.tenantID, pw.clientID, rec.Will, "")

	return true, false
}

// owe marks s as ending with its will unpublished: its key keeps the will
// for a reclaim (finish). msgID is the message ID an append of it already
// used, "" for none.
func (s *session) owe(msgID string) {
	s.mu.Lock()
	s.willOwed = true
	s.willMsgID = msgID
	s.mu.Unlock()
}

// pendingWills returns the namespaced client IDs of the delayed wills this
// node answers for, for tests.
func (n *clusterNode) pendingWills() []string {
	n.willsMu.Lock()
	defer n.willsMu.Unlock()

	out := make([]string, 0, len(n.wills))
	for id := range n.wills {
		out = append(out, id)
	}

	return out
}
