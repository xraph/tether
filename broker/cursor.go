package broker

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/topic"
)

// Cursors and replay.
//
// A tracked session's cursor is the highest log sequence such that every
// earlier message of its tenant was either acknowledged by the session (if it
// matched) or did not match. QoS 0 is not in the log and plays no part.
//
// Live, a session gets the tenant's messages from the node's reader through
// mochi's fan-out (logDeliver, under the tenant's lock). Each injected packet
// carries its sequence (tagSeq) into the copy mochi makes per subscriber, so
// OnQosPublish learns which packet ID carries which sequence (session.out)
// and the PUBACK or PUBCOMP read from the client completes it. The cursor is
// then the reader's position (livePos), held below the oldest delivery still
// in flight and below any message mochi could not queue for the session
// (session.pin).
//
// Resumed, a session's subscriptions stay out of mochi's topic index
// (session.loaded) while a goroutine replays the log from cursor+1 to the
// reader's position and sends what matches straight to the client. It loops
// until it has caught up, then, under the tenant's lock, so between two
// messages of the reader, puts the subscriptions into the index: the session
// is live from the next message on, with nothing missed and nothing sent
// twice. A replay that got ahead of the reader (the cursor came from a node
// further along) waits, out of the index, until the reader passes it.

// Session modes.
const (
	modeLive    = iota // in mochi's index, fed by the reader
	modeReplay         // out of the index, replaying
	modeWaiting        // out of the index, ahead of the reader
)

// OutRecord states of an outbound QoS 2 delivery.
const (
	outPublish byte = iota // PUBLISH sent, no PUBREC yet
	outPubrel              // PUBREC read, PUBREL sent
)

// replayPoll is how often a replay held back by a full inflight window looks
// again, and how long one waits after a failed Replay.
const replayPoll = 20 * time.Millisecond

var (
	errBackpressure = errors.New("tether/broker: replay: client window full")
	errClientGone   = errors.New("tether/broker: replay: connection ended")
)

// tenantTrack is this node's delivery state of one tenant's log.
type tenantTrack struct {
	id string

	// mu is held by logDeliver around each message and by every move of a
	// session into or out of mochi's topic index while it replays, so a
	// session goes live exactly between two messages.
	mu sync.Mutex

	// livePos is the sequence of the last message the reader handed over,
	// or where it started. lastMsg is the sequence of the last message it
	// handed over, 0 before the first: a replay must have seen at least
	// that much of what it covers (cluster.Log.Replay cuts its range at the
	// tenant's last message as read when it starts, which can lag the
	// reader).
	livePos atomic.Uint64
	lastMsg atomic.Uint64

	sessions map[*session]struct{} // mu: tracked sessions
	waiting  map[*session]struct{} // mu: sessions ahead of the reader

	// full holds tracked sessions whose inflight map was full at their last
	// delivery: the next message may not fit.
	fullMu sync.Mutex
	full   map[*session]struct{}
}

// track returns the delivery state of tenantID, making it on first use.
func (n *clusterNode) track(tenantID string) *tenantTrack {
	n.tenantsMu.Lock()
	defer n.tenantsMu.Unlock()

	tt := n.tenants[tenantID]
	if tt == nil {
		tt = &tenantTrack{
			id:       tenantID,
			sessions: make(map[*session]struct{}),
			waiting:  make(map[*session]struct{}),
			full:     make(map[*session]struct{}),
		}
		n.tenants[tenantID] = tt
	}

	return tt
}

// trackIfAny returns the delivery state of tenantID, or nil.
func (n *clusterNode) trackIfAny(tenantID string) *tenantTrack {
	n.tenantsMu.Lock()
	defer n.tenantsMu.Unlock()

	return n.tenants[tenantID]
}

// advanceLive raises tenantID's live position to at least pos (a reader's
// starting position).
func (n *clusterNode) advanceLive(tenantID string, pos uint64) {
	tt := n.track(tenantID)

	for {
		cur := tt.livePos.Load()
		if pos <= cur || tt.livePos.CompareAndSwap(cur, pos) {
			return
		}
	}
}

// baseLocked is how far s has dealt with its tenant's log apart from the
// deliveries still in flight: the replay position, or the tenant's live
// position lp (read before s.mu was taken) for a live session, never past a
// pin. s.mu is held.
func (s *session) baseLocked(lp uint64) uint64 {
	c := s.pos
	if s.mode == modeLive {
		c = max(lp, s.liveFloor)
	}

	if s.pinned {
		c = min(c, s.pin)
	}

	return c
}

// cursorLocked is s's cursor: baseLocked held below every delivery in
// flight. s.mu is held.
func (s *session) cursorLocked(lp uint64) uint64 {
	c := s.baseLocked(lp)

	for _, seq := range s.out {
		c = min(c, seq-1)
	}

	for seq := range s.queued {
		c = min(c, seq-1)
	}

	return c
}

// freeze fixes s's position where it is now, for a session that is ending:
// it no longer follows the tenant's live position. The tenant's lock is held.
func (s *session) freeze() {
	lp := s.tt.livePos.Load()

	s.mu.Lock()
	s.pos = s.baseLocked(lp)
	s.mode = modeReplay
	s.mu.Unlock()
}

// pinLocked keeps s's cursor at or below c. It reports true when the caller
// must start a repin (Broker.repin) once s.mu is released: a live session
// that missed a message goes back to replaying from the pin, so the pin
// clears instead of holding the cursor for the rest of the connection.
// s.mu is held.
func (s *session) pinLocked(c uint64) bool {
	if !s.pinned || c < s.pin {
		s.pinned, s.pin = true, c
	}

	if s.mode == modeLive && !s.repinning {
		s.repinning = true

		return true
	}

	return false
}

// repin takes a live session that missed a message (it is pinned) back to
// replaying from its pin: out of mochi's index under the tenant's lock, the
// deliveries still in flight and those completed since the pin marked as not
// to be sent again, then the usual replay, which waits for the client's
// window and goes live again when it has caught up. The pin clears.
func (b *Broker) repin(s *session) {
	n := b.cluster
	tt := s.tt

	tt.mu.Lock()

	if _, ok := tt.sessions[s]; !ok {
		tt.mu.Unlock()

		return
	}

	cl := n.client(s)

	s.mu.Lock()
	s.repinning = false

	if s.mode != modeLive || !s.pinned {
		s.mu.Unlock()
		tt.mu.Unlock()

		return
	}

	s.mode = modeReplay
	s.pos = s.pin
	s.replaySeen = s.pin
	s.pinned = false
	s.skipThrough, s.redo, s.flipFloor = 0, nil, 0
	s.skip = make(map[uint64]struct{})

	for _, seq := range s.out {
		if seq > s.pos {
			s.skip[seq] = struct{}{}
		}
	}

	for seq := range s.afterPin {
		if seq > s.pos {
			s.skip[seq] = struct{}{}
		}
	}

	for seq := range s.qos2Done {
		if seq > s.pos {
			s.skip[seq] = struct{}{}
		}
	}

	s.afterPin = nil
	s.mu.Unlock()

	for _, sub := range cl.State.Subscriptions.GetAll() {
		if n.mochi.Topics.Unsubscribe(sub.Filter, cl.ID) {
			atomic.AddInt64(&n.mochi.Info.Subscriptions, -1)
		}
	}

	tt.mu.Unlock()

	n.repins.Add(1)
	n.log.Info("tether: session missed a message, replaying it from its cursor", slog.String("client", s.nsID))

	b.startReplay(s, cl)
}

// orderResend makes mochi resend cl's inflight messages in log order at the
// CONNACK of a session it took over or restored: mochi sorts them by the
// low 16 bits of Created, whole seconds, so deliveries made within one
// second would come back in any order. Tagged ones get Created in their
// sequence's order, just in the past; untagged ones (inbound PUBREC
// reservations) go first.
func orderResend(cl *mqtt.Client) {
	all := cl.State.Inflight.GetAll(false)
	if len(all) < 2 {
		return
	}

	type tagged struct {
		pk  packets.Packet
		seq uint64
	}

	var (
		seqd  []tagged
		other []packets.Packet
	)

	for _, pk := range all {
		if seq, ok := seqOf(pk); ok {
			seqd = append(seqd, tagged{pk, seq})
		} else {
			other = append(other, pk)
		}
	}

	if len(seqd) < 2 {
		return
	}

	slices.SortFunc(seqd, func(a, b tagged) int { return cmp.Compare(a.seq, b.seq) })

	base := time.Now().Unix() - int64(len(all))

	for _, pk := range other {
		pk.Created = base - 1
		cl.State.Inflight.Set(pk)
	}

	for i, t := range seqd {
		t.pk.Created = base + int64(i)
		cl.State.Inflight.Set(t.pk)
	}
}

// pinIfFull is noteFull for something that filled cl's inflight map outside
// the tenant's lock (an inbound QoS 2 reservation, a takeover's clone): a
// message injected between the last pinFull and now may already have been
// dropped, so a live session is pinned at lp, the live position read before
// the map grew, and sent back to replaying.
func (n *clusterNode) pinIfFull(s *session, cl *mqtt.Client, lp uint64) {
	if cl.State.Inflight.Len() < n.maxInflight {
		return
	}

	s.mu.Lock()
	repin := s.mode == modeLive && s.pinLocked(lp)
	s.mu.Unlock()

	n.noteFull(s, cl)

	if repin {
		go n.b.repin(s)
	}
}

// noteFull marks s as one whose inflight map is full, so the next message
// injected pins it (pinFull): mochi drops a delivery that does not fit
// without telling any hook.
func (n *clusterNode) noteFull(s *session, cl *mqtt.Client) {
	if cl.State.Inflight.Len() < n.maxInflight {
		return
	}

	s.tt.fullMu.Lock()
	s.tt.full[s] = struct{}{}
	s.tt.fullMu.Unlock()
}

// tagSeq marks pk with its log sequence. mochi copies the field into every
// subscriber's packet and its inflight entry, and never encodes it in a
// PUBLISH.
func tagSeq(pk *packets.Packet, seq uint64) {
	pk.Connect.WillTopic = strconv.FormatUint(seq, 10)
}

// seqOf returns the log sequence tagSeq put on pk.
func seqOf(pk packets.Packet) (uint64, bool) {
	if pk.Connect.WillTopic == "" {
		return 0, false
	}

	seq, err := strconv.ParseUint(pk.Connect.WillTopic, 10, 64)

	return seq, err == nil && seq > 0
}

// deliverLog is the reader's handling of one message of tenantID: sessions
// waiting for the reader to pass them go live first, then the message is
// injected for mochi's fan-out, and the live position moves to it. A message
// that reaches nobody because the injection failed holds every live
// session's cursor below it.
func (b *Broker) deliverLog(tenantID string, m cluster.LogMessage) error {
	n := b.cluster
	tt := n.track(tenantID)

	tt.mu.Lock()
	defer tt.mu.Unlock()
	defer tt.livePos.Store(m.Seq)
	defer tt.lastMsg.Store(m.Seq)

	n.flipWaiting(tt, m.Seq)

	if m.Tenant != tenantID || !validTenant(m.Tenant) || topic.Valid(m.Topic, false, subject.MaxTopicBytes) != nil ||
		hasSysPrefix(m.Topic) || m.QoS < 1 || m.QoS > 2 {
		n.log.Warn("tether: log message dropped, not a tenant topic", slog.String("tenant", m.Tenant),
			slog.String("topic", m.Topic), slog.Uint64("seq", m.Seq))

		return nil
	}

	n.pinFull(tt, m.Seq)

	if hook := n.beforeInject; hook != nil {
		hook(m.Seq)
	}

	origin := ""
	if m.ClientID != "" {
		origin = namespacedID(m.Tenant, m.ClientID)
	}

	err := b.inject(m.Tenant, m.Topic, m.Payload, m.QoS, m.Props, origin, m.Seq, m.Retain)
	if err != nil {
		// At Stop (errNotInjecting) the sessions are ending: pinned, not
		// replayed.
		n.pinAll(tt, m.Seq, !errors.Is(err, errNotInjecting))

		if errors.Is(err, errNotInjecting) {
			return nil
		}

		return err
	}

	n.logDelivered.Add(1)

	return nil
}

// flipWaiting makes live the sessions of tt that are ahead of the reader and
// that the message seq passes. tt.mu is held.
func (n *clusterNode) flipWaiting(tt *tenantTrack, seq uint64) {
	for s := range tt.waiting {
		if _, ok := tt.sessions[s]; !ok {
			delete(tt.waiting, s)

			continue
		}

		s.mu.Lock()
		pos, seen := s.pos, s.replaySeen
		missed := seq <= pos && seq > seen

		if missed {
			// A message of the tenant inside the range the replay covered
			// that it never saw: back to replaying from what it saw.
			s.mode, s.pos = modeReplay, seen
		}
		s.mu.Unlock()

		switch {
		case pos < seq:
			n.flip(tt, s)
		case missed:
			delete(tt.waiting, s)

			go n.b.startReplay(s, n.client(s))
		}
	}
}

// pinFull holds below seq the cursor of every live session whose inflight
// map is full, since mochi drops what does not fit, and sends it back to
// replaying (repin). The map fills from mochi's deliveries (onQosPublish),
// the replay (deliverDirect), a takeover's clone (established) and inbound
// QoS 2 reservations (commitPublish); each notes it (noteFull). tt.mu is
// held.
func (n *clusterNode) pinFull(tt *tenantTrack, seq uint64) {
	tt.fullMu.Lock()
	defer tt.fullMu.Unlock()

	for s := range tt.full {
		cl := n.client(s)
		if cl.State.Inflight.Len() < n.maxInflight {
			delete(tt.full, s)

			continue
		}

		s.mu.Lock()
		repin := s.mode == modeLive && s.pinLocked(seq-1)
		s.mu.Unlock()

		if repin {
			go n.b.repin(s)
		}
	}
}

// pinAll holds below seq the cursor of every live session of tt, and sends
// them back to replaying when replay says so. tt.mu is held.
func (n *clusterNode) pinAll(tt *tenantTrack, seq uint64, replay bool) {
	for s := range tt.sessions {
		s.mu.Lock()
		repin := s.mode == modeLive && s.pinLocked(seq-1) && replay

		if !replay {
			s.repinning = false
		}
		s.mu.Unlock()

		if repin {
			go n.b.repin(s)
		}
	}
}

// flip puts s's subscriptions into mochi's topic index: from the next
// message the reader injects, s gets it live. tt.mu is held.
func (n *clusterNode) flip(tt *tenantTrack, s *session) {
	srv := n.mochi
	cl := n.client(s)

	delete(tt.waiting, s)

	if _, ok := tt.sessions[s]; !ok {
		return // the session ended meanwhile
	}

	s.mu.Lock()
	s.mode = modeLive
	s.liveFloor = s.pos

	if s.pinned && s.pin <= s.pos {
		// Set while replaying, which covered it.
		s.pinned = false
	}

	loaded := s.loaded
	s.loaded = nil
	s.skip, s.skipThrough, s.redo = nil, 0, nil
	s.mu.Unlock()

	for f, sub := range loaded {
		if _, ok := cl.State.Subscriptions.Get(f); !ok {
			cl.State.Subscriptions.Add(f, sub)
		}
	}

	for _, sub := range cl.State.Subscriptions.GetAll() {
		if srv.Topics.Subscribe(cl.ID, sub) {
			atomic.AddInt64(&srv.Info.Subscriptions, 1)
		}
	}

	n.replaysDone.Add(1)
	n.noteFull(s, cl)
}

// holdOut takes subs out of mochi's topic index for a session that is not
// live yet, so no live message reaches it before its replay is done: a
// SUBSCRIBE during the replay (OnSubscribed), or what mochi's takeover put
// back. They stay on the client and go back in at flip.
func (n *clusterNode) holdOut(s *session, cl *mqtt.Client, subs []packets.Subscription) {
	tt := s.tt

	tt.mu.Lock()
	defer tt.mu.Unlock()

	s.mu.Lock()
	live := s.mode == modeLive
	s.mu.Unlock()

	if live {
		return
	}

	for _, sub := range subs {
		if n.mochi.Topics.Unsubscribe(sub.Filter, cl.ID) {
			atomic.AddInt64(&n.mochi.Info.Subscriptions, -1)
		}
	}
}

// startReplay starts s's replay for cl, after its CONNACK, stopping any
// replay still running for the session first. It does nothing at all when cl
// no longer serves the session (a handover got there first: the new
// connection starts its own at its CONNACK).
func (b *Broker) startReplay(s *session, cl *mqtt.Client) {
	n := b.cluster

	s.replayMu.Lock()
	defer s.replayMu.Unlock()

	// A stale caller (a repin or a waiting flip that read the client before
	// a handover) neither stops the current connection's replay nor touches
	// the index under the ID it shares with it. handOver stopped the old
	// connection's replay itself.
	if n.client(s) != cl {
		return
	}

	n.stopReplayLocked(s)

	// What mochi's takeover put into the index (a replay handed over on
	// this node) comes out again.
	all := cl.State.Subscriptions.GetAll()
	subs := make([]packets.Subscription, 0, len(all))

	for _, sub := range all {
		subs = append(subs, sub)
	}

	n.holdOut(s, cl, subs)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.mu.Lock()
	s.replayCancel, s.replayDone = cancel, done
	s.mu.Unlock()

	go b.runReplay(ctx, s, cl, done)
}

// stopReplay ends s's replay, if one runs, and waits for it.
func (n *clusterNode) stopReplay(s *session) {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()

	n.stopReplayLocked(s)
}

// stopReplayLocked is stopReplay under s.replayMu. The replay's writer is
// abandoned: it sends nothing more, even when its blocked write comes back
// (a handover's new connection gets what the old one had in flight from
// mochi's clone instead). What it had queued and not sent is given back to
// the replay: the position goes back below the first of it, and the
// deliveries in flight above that are marked not to send again, so the next
// replay, for whichever connection, sends it in order.
func (n *clusterNode) stopReplayLocked(s *session) {
	s.mu.Lock()
	cancel, done := s.replayCancel, s.replayDone
	s.replayCancel, s.replayDone = nil, nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writer != nil {
		s.writer.abandoned = true
		s.writer = nil
	}

	if len(s.queued) == 0 {
		return
	}

	first := uint64(0)
	for seq := range s.queued {
		if first == 0 || seq < first {
			first = seq
		}
	}

	s.pos = min(s.pos, first-1)
	s.replaySeen = min(s.replaySeen, s.pos)
	s.queued = nil

	if s.skip == nil {
		s.skip = make(map[uint64]struct{})
	}

	for _, seq := range s.out {
		if seq > s.pos {
			s.skip[seq] = struct{}{}
		}
	}
}

// replayBuffer bounds the replayed packets of one session waiting for its
// writer.
const replayBuffer = 64

// replayCallWait bounds one Log.Replay call of a session's replay.
const replayCallWait = 10 * time.Second

// replayOut is one replay's writer: the replay hands it packets (replayOne)
// and it sends them on its own goroutine, so a client that stops reading
// holds up its own session and not the tenant's shared replay consumer
// (Log.Replay runs every call's fn on one goroutine). It also takes the
// packet ID and the inflight entry of each QoS 1 or 2 packet: mochi's
// NextPacketID needs the client's lock, which a blocked write holds.
type replayOut struct {
	n       *clusterNode
	s       *session
	cl      *mqtt.Client
	q       chan packets.Packet
	pending atomic.Int64 // queued or being written
	failed  atomic.Bool  // a write failed: the connection is gone
	// abandoned (under s.mu) is set when the replay stops: nothing more is
	// registered or written.
	abandoned bool
}

func newReplayOut(n *clusterNode, s *session, cl *mqtt.Client) *replayOut {
	return &replayOut{n: n, s: s, cl: cl, q: make(chan packets.Packet, replayBuffer)}
}

// run sends until q is closed. A write that blocks blocks only this.
func (ro *replayOut) run() {
	for pk := range ro.q {
		if !ro.failed.Load() && !ro.send(pk) {
			ro.failed.Store(true)
		}

		ro.pending.Add(-1)
	}
}

// send registers a QoS 1 or 2 packet in the client's inflight map and the
// session's deliveries (it was queued until now, holding the cursor) and
// writes it. False when the connection is gone or out of packet IDs; a packet
// not sent stays queued, so the cursor stays below it.
func (ro *replayOut) send(pk packets.Packet) bool {
	s, cl := ro.s, ro.cl

	if pk.FixedHeader.Qos > 0 {
		seq, _ := seqOf(pk)

		id, err := cl.NextPacketID()
		if err != nil {
			return false
		}

		pk.PacketID = uint16(id) //nolint:gosec // NextPacketID stays within the uint16 packet ID space

		// Registered under s.mu with the abandon check, so a stopped
		// replay never leaves a delivery behind it.
		s.mu.Lock()

		if ro.abandoned {
			s.mu.Unlock()

			return false
		}

		if cl.State.Inflight.Set(pk) {
			atomic.AddInt64(&ro.n.mochi.Info.Inflight, 1)
		}

		s.out[pk.PacketID] = seq
		delete(s.queued, seq)
		s.mu.Unlock()

		cl.State.Inflight.DecreaseSendQuota()
		ro.n.noteFull(s, cl)
	} else {
		s.mu.Lock()
		abandoned := ro.abandoned
		s.mu.Unlock()

		if abandoned {
			return false
		}
	}

	return cl.WritePacket(pk) == nil
}

// runReplay replays the log for s from its position to the reader's, again
// and again, until it has caught up, and then makes s live (or leaves it
// waiting, out of the index, for the reader to pass it). It goes live only
// once its writer has written everything it replayed, so no live message
// overtakes a replayed one.
func (b *Broker) runReplay(ctx context.Context, s *session, cl *mqtt.Client, done chan<- struct{}) {
	defer close(done)

	n := b.cluster
	tt := s.tt
	delay := replayPoll

	ro := newReplayOut(n, s, cl)

	s.mu.Lock()
	s.writer = ro
	s.mu.Unlock()
	defer close(ro.q)

	go ro.run()

	for ctx.Err() == nil {
		if !b.drain(ctx, ro) {
			return
		}

		tt.mu.Lock()

		if _, ok := tt.sessions[s]; !ok {
			tt.mu.Unlock()

			return
		}

		lp := tt.livePos.Load()
		last := tt.lastMsg.Load()

		s.mu.Lock()
		pos, floor := s.pos, s.flipFloor
		s.mu.Unlock()

		if pos >= lp && pos >= floor {
			if pos == lp {
				n.flip(tt, s)
			} else {
				s.mu.Lock()
				s.mode = modeWaiting
				s.mu.Unlock()

				tt.waiting[s] = struct{}{}
			}

			tt.mu.Unlock()

			return
		}

		tt.mu.Unlock()

		target := max(lp, floor)

		// Each call is bounded: a replay consumer that stops delivering (seen
		// under heavy load) must not leave the session out of the index for
		// good. The waiter leaves, the consumer closes once no call uses it,
		// and the next call starts a new one from where this got to.
		callCtx, cancelCall := context.WithTimeout(ctx, replayCallWait)
		err := n.Log.Replay(callCtx, s.tenantID, pos+1, target, func(m cluster.LogMessage) error {
			return b.replayOne(s, cl, ro, m)
		})

		cancelCall()

		s.mu.Lock()
		short := err == nil && last > pos && s.replaySeen < last
		s.mu.Unlock()

		switch {
		case short:
			// The replay ended before a message the reader has already
			// handed over: its view of the log lagged. Again from where it
			// got to.
			n.replayRetries.Add(1)

			select {
			case <-ctx.Done():
				return
			case <-time.After(replayPoll):
			}
		case err == nil:
			s.mu.Lock()
			s.pos = max(s.pos, target)
			s.mu.Unlock()

			delay = replayPoll
		case ctx.Err() != nil || errors.Is(err, errClientGone):
			return
		case errors.Is(err, errBackpressure):
			// This session has left the shared replay; it starts its own
			// again from its position once it has room.
			b.waitRoom(ctx, s, cl, ro)
		default:
			n.log.Warn("tether: session replay failed, retrying", slog.String("client", s.nsID), slog.Any("error", err))

			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}

			delay = min(2*delay, n.appendRetryMax)
		}
	}
}

// drain waits until ro has written everything queued. False when the
// connection is gone or ctx ended.
func (b *Broker) drain(ctx context.Context, ro *replayOut) bool {
	tick := time.NewTicker(replayPoll / 4)
	defer tick.Stop()

	for ro.pending.Load() > 0 && !ro.failed.Load() {
		select {
		case <-ctx.Done():
			return false
		case <-tick.C:
		}
	}

	return !ro.failed.Load() && ctx.Err() == nil
}

// waitRoom waits until cl has room for another delivery and ro for another
// packet, or the connection or ctx ends.
func (b *Broker) waitRoom(ctx context.Context, s *session, cl *mqtt.Client, ro *replayOut) {
	tick := time.NewTicker(replayPoll)
	defer tick.Stop()

	for (b.windowFull(s, cl) || ro.pending.Load() >= replayBuffer) && !cl.Closed() && !ro.failed.Load() {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// windowFull reports whether cl cannot take another QoS 1 or 2 delivery now,
// counting those queued for the replay's writer: its inflight map would be
// full, or as many deliveries as its Receive Maximum would be unacknowledged.
func (b *Broker) windowFull(s *session, cl *mqtt.Client) bool {
	n := b.cluster
	inflight := cl.State.Inflight.Len()

	s.mu.Lock()
	defer s.mu.Unlock()

	queued := len(s.queued)

	return inflight+queued >= n.maxInflight || s.recvMax > 0 && len(s.out)+queued >= s.recvMax
}

// replayOne hands m to ro if it matches s, and moves s's position past it.
// A full buffer returns errBackpressure at once: this session leaves the
// shared replay rather than hold it.
func (b *Broker) replayOne(s *session, cl *mqtt.Client, ro *replayOut, m cluster.LogMessage) error {
	if ro.failed.Load() {
		return errClientGone
	}

	s.mu.Lock()
	s.replaySeen = max(s.replaySeen, m.Seq)
	_, skip := s.skip[m.Seq]
	if !skip && m.Seq <= s.skipThrough {
		_, again := s.redo[m.Seq]
		skip = !again
	}
	s.mu.Unlock()

	if !skip {
		if ro.pending.Load() >= replayBuffer {
			return errBackpressure
		}

		pk, send, err := b.deliverDirect(s, cl, m)
		if err != nil {
			return err
		}

		if send {
			ro.pending.Add(1)
			ro.q <- pk // room: pending < replayBuffer, one producer
			b.cluster.replayed.Add(1)
		}
	}

	s.mu.Lock()
	s.pos = max(s.pos, m.Seq)
	s.mu.Unlock()

	return nil
}

// deliverDirect prepares one replayed message for cl, the way mochi's
// publishToClient would for the session's matching subscriptions: No Local,
// the read ACL, the lower QoS, subscription identifiers, message expiry, a
// packet ID and an inflight entry for QoS 1 and 2. send is false when nothing
// is owed. It returns errBackpressure when the client has no room (nothing is
// prepared) and errClientGone when the connection has ended. The caller's
// writer sends the packet.
func (b *Broker) deliverDirect(s *session, cl *mqtt.Client, m cluster.LogMessage) (pk packets.Packet, send bool, err error) {
	n := b.cluster
	srv := n.mochi

	st := b.state.get(cl)
	if st == nil || cl.Closed() {
		return pk, false, errClientGone
	}

	if m.Tenant != s.tenantID || topic.Valid(m.Topic, false, subject.MaxTopicBytes) != nil || hasSysPrefix(m.Topic) ||
		m.QoS < 1 || m.QoS > 2 {
		return pk, false, nil
	}

	sub, ids, ok := s.match(cl, m.Topic)
	if !ok {
		return pk, false, nil
	}

	origin := ""
	if m.ClientID != "" {
		origin = namespacedID(m.Tenant, m.ClientID)
	}

	ns := topic.Namespace(s.tenantID, m.Topic)

	if sub.NoLocal && origin == cl.ID || !b.canRead(s.tenantID, st.acl(), ns) {
		return pk, false, nil
	}

	now := time.Now()
	out := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish, Qos: min(m.QoS, sub.Qos, srv.Options.Capabilities.MaximumQos),
			// [MQTT-3.3.1-12], [MQTT-3.3.1-13]: the flag as published only
			// for a 5.0 subscription with Retain As Published.
			Retain: m.Retain && sub.RetainAsPublished && cl.Properties.ProtocolVersion == 5,
		},
		TopicName:  ns,
		Payload:    m.Payload,
		Properties: packetProps(m.Props),
		Origin:     origin,
		Created:    now.Unix(),
	}

	if len(ids) > 0 {
		out.Properties.SubscriptionIdentifier = ids
	}

	if m.Props.MessageExpiry > 0 && !m.PublishedAt.IsZero() {
		exp := m.PublishedAt.Add(time.Duration(m.Props.MessageExpiry) * time.Second)
		if !exp.After(now) {
			return pk, false, nil // expired: the client is owed nothing
		}

		out.Expiry = exp.Unix()
	}

	tagSeq(&out, m.Seq)

	if out.FixedHeader.Qos > 0 {
		if b.windowFull(s, cl) {
			return pk, false, errBackpressure
		}

		// The writer takes its packet ID; until then it holds the cursor.
		s.mu.Lock()
		if s.queued == nil {
			s.queued = make(map[uint64]struct{})
		}

		s.queued[m.Seq] = struct{}{}
		s.mu.Unlock()
	}

	return out, true, nil
}

// match merges the subscriptions of s (those on cl and those still held out
// of the index) that match the tenant-relative topic name, as mochi does for
// a delivery: the highest QoS, every subscription identifier, and No Local
// only when every matching subscription has it. Retain As Published is set
// when any matching subscription has it, as for a live delivery
// (retainAsPublished). Shared subscriptions are left to their group.
func (s *session) match(cl *mqtt.Client, name string) (packets.Subscription, []int, bool) {
	all := cl.State.Subscriptions.GetAll()

	s.mu.Lock()
	for f, sub := range s.loaded {
		if _, ok := all[f]; !ok {
			all[f] = sub
		}
	}
	s.mu.Unlock()

	var (
		merged packets.Subscription
		ids    []int
		found  bool
	)

	merged.NoLocal = true

	for f, sub := range all {
		rel, ok := topic.Strip(s.tenantID, f)
		if !ok || withoutShare(rel) != rel || !topic.Match(rel, name) {
			continue
		}

		found = true
		merged.Qos = max(merged.Qos, sub.Qos)
		merged.NoLocal = merged.NoLocal && sub.NoLocal
		merged.RetainAsPublished = merged.RetainAsPublished || sub.RetainAsPublished

		if sub.Identifier > 0 {
			ids = append(ids, sub.Identifier)
		}
	}

	slices.Sort(ids)

	return merged, ids, found
}

// onQosPublish records which log sequence an outbound QoS 1 or 2 PUBLISH to
// a tracked session carries (OnQosPublish: mochi has just put it in the
// client's inflight map).
func (n *clusterNode) onQosPublish(cl *mqtt.Client, pk packets.Packet) {
	if pk.FixedHeader.Type != packets.Publish {
		return
	}

	seq, ok := seqOf(pk)
	if !ok {
		return
	}

	s := n.deliveriesOf(cl)
	if s == nil {
		return
	}

	s.mu.Lock()

	repin := false

	if old, ok := s.out[pk.PacketID]; ok && old != seq {
		// mochi reused a packet ID it had dropped from the map: the older
		// delivery can no longer be told complete.
		repin = s.pinLocked(min(old, seq) - 1)
	}

	s.out[pk.PacketID] = seq

	if s.mode != modeLive {
		// A live delivery that slipped in before holdOut: the replay must
		// not send it again.
		if s.skip == nil {
			s.skip = make(map[uint64]struct{})
		}

		s.skip[seq] = struct{}{}
	}

	s.mu.Unlock()

	if repin {
		go n.b.repin(s)
	}

	n.noteFull(s, cl)
}

// onPublishDropped is OnPublishDropped: mochi could not queue a delivery and
// took it back out of the inflight map. The session's cursor stays below it.
func (n *clusterNode) onPublishDropped(cl *mqtt.Client, pk packets.Packet) {
	seq, ok := seqOf(pk)
	if !ok {
		return
	}

	s := n.deliveriesOf(cl)
	if s == nil {
		return
	}

	s.mu.Lock()
	repin := s.pinLocked(seq - 1)

	if s.out[pk.PacketID] == seq {
		delete(s.out, pk.PacketID)
	}

	// Not sent after all: a replay must not pass over it.
	delete(s.skip, seq)

	s.mu.Unlock()

	if repin {
		go n.b.repin(s)
	}
}

// onQosDropped is OnQosDropped for the session's current connection: mochi
// gave up on a delivery (its message expired). It no longer holds the cursor.
func (n *clusterNode) onQosDropped(cl *mqtt.Client, pk packets.Packet) {
	seq, ok := seqOf(pk)
	if !ok {
		return
	}

	s := n.sessionOf(cl)
	if s == nil {
		return
	}

	s.mu.Lock()

	if s.out[pk.PacketID] == seq {
		delete(s.out, pk.PacketID)
	}

	if or, ok := s.qos2Out[pk.PacketID]; ok && or.Seq == seq {
		delete(s.qos2Out, pk.PacketID)
		s.gen++
	}

	s.mu.Unlock()
}

// onAck handles a PUBACK or PUBCOMP read from a client for one of its
// deliveries: the delivery is complete. A completed QoS 2 delivery is kept
// in qos2Done until the cursor passes it.
func (n *clusterNode) onAck(cl *mqtt.Client, id uint16, comp bool) {
	s := n.sessionOf(cl)
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if seq, ok := s.out[id]; ok && s.pinned && seq > s.pin {
		// Completed after a pin: the repin's replay must not send it again.
		if s.afterPin == nil {
			s.afterPin = make(map[uint64]struct{})
		}

		s.afterPin[seq] = struct{}{}
	}

	if seq, ok := s.out[id]; ok {
		// Dealt with: a resumed record's pending entry is settled.
		delete(s.redo, seq)
	}

	delete(s.out, id)

	if !comp {
		return
	}

	if or, ok := s.qos2Out[id]; ok {
		delete(s.qos2Out, id)
		s.qos2Done[or.Seq] = struct{}{}
		s.gen++
	}
}

// onPubrec handles a PUBREC read from a client for a QoS 2 delivery: before
// mochi answers with PUBREL, the record says so (outPubrel), so a node that
// resumes the session sends PUBREL rather than the message again. It reports
// false when that write failed; the client is then stopped and mochi must
// not answer.
func (n *clusterNode) onPubrec(cl *mqtt.Client, pk packets.Packet) bool {
	s := n.sessionOf(cl)
	if s == nil || !s.tracked {
		return true
	}

	s.mu.Lock()

	or, ok := s.qos2Out[pk.PacketID]

	switch {
	case !ok:
		s.mu.Unlock()

		return true
	case pk.ReasonCode >= packets.ErrUnspecifiedError.Code:
		// The client refused it: the flow ends here.
		delete(s.qos2Out, pk.PacketID)
		delete(s.out, pk.PacketID)
		s.gen++
		s.mu.Unlock()

		return true
	case or.State == outPubrel:
		s.mu.Unlock()

		return true
	}

	or.State = outPubrel
	s.qos2Out[pk.PacketID] = or
	s.gen++
	s.mu.Unlock()

	return n.persistNow(s, cl)
}

// beforeQoS2Send runs in OnPacketEncode for a QoS 2 PUBLISH to a tracked
// session: the delivery goes into the record (QoS2Out, outPublish) before it
// is written, so a node that resumes the session resends it under the same
// packet ID instead of sending the message again. It reports false when that
// write failed; the client is then stopped.
func (n *clusterNode) beforeQoS2Send(cl *mqtt.Client, pk packets.Packet) bool {
	seq, ok := seqOf(pk)
	if !ok {
		return true
	}

	s := n.sessionOf(cl)
	if s == nil || !s.tracked {
		return true
	}

	s.mu.Lock()

	if or, ok := s.qos2Out[pk.PacketID]; ok && or.Seq == seq {
		s.mu.Unlock()

		return true
	}

	s.qos2Out[pk.PacketID] = cluster.OutRecord{Seq: seq, State: outPublish}
	s.gen++
	s.mu.Unlock()

	return n.persistNow(s, cl)
}

// persistNow writes s's record at once, for a QoS 2 step that must be in the
// cell before the packet goes out.
func (n *clusterNode) persistNow(s *session, cl *mqtt.Client) bool {
	ctx, cancel := context.WithTimeout(context.Background(), sessionWriteWait)
	defer cancel()

	err := n.persist(ctx, s, 0)
	if err == nil {
		return true
	}

	if errors.Is(err, errSessionLost) {
		// Another node has the session: the client hears it with 0x8E
		// before this packet, which then fails to go out.
		n.sessMu.Lock()
		deleted := s.deleted
		n.sessMu.Unlock()

		code := packets.ErrSessionTakenOver
		if deleted {
			code = packets.ErrAdministrativeAction
		}

		disconnect(n.mochi, n.log, cl, code)

		return false
	}

	n.log.Warn("tether: qos 2 state not written, closing the client", slog.String("client", s.nsID), slog.Any("error", err))
	cl.Stop(packets.ErrServerUnavailable)

	return false
}

// prunedDelivery is pruneSession dropping a queued delivery the grants no
// longer allow: it no longer holds the session's cursor.
func (n *clusterNode) prunedDelivery(cl *mqtt.Client, pk packets.Packet) {
	n.onQosDropped(cl, pk)
}

// heldOut reports whether s's subscriptions are out of mochi's index.
func (s *session) heldOut() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.mode != modeLive
}

// unloaded drops filters an UNSUBSCRIBE removed from the subscriptions a
// replaying session still holds out of the index.
func (s *session) unloaded(filters packets.Subscriptions) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, f := range filters {
		delete(s.loaded, f.Filter)
	}

	s.gen++
}
