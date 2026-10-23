package broker

import (
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

	// livePos is the sequence of the last message the reader handed over.
	livePos atomic.Uint64

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

// cursorLocked is s's cursor given the tenant's live position lp, read
// before s.mu was taken. s.mu is held.
func (s *session) cursorLocked(lp uint64) uint64 {
	c := s.pos
	if s.mode == modeLive {
		c = max(lp, s.liveFloor)
	}

	if s.pinned {
		c = min(c, s.pin)
	}

	for _, seq := range s.out {
		c = min(c, seq-1)
	}

	return c
}

// freeze fixes s's cursor where it is now, for a session that is ending: it
// no longer follows the tenant's live position. The tenant's lock is held.
func (s *session) freeze() {
	lp := s.tt.livePos.Load()

	s.mu.Lock()
	s.pos = s.cursorLocked(lp)
	s.mode = modeReplay
	s.mu.Unlock()
}

// pinLocked keeps s's cursor at or below c. s.mu is held.
func (s *session) pinLocked(c uint64) {
	if !s.pinned || c < s.pin {
		s.pinned, s.pin = true, c
	}
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

	n.flipWaiting(tt, m.Seq)

	if m.Tenant != tenantID || !validTenant(m.Tenant) || topic.Valid(m.Topic, false, subject.MaxTopicBytes) != nil ||
		hasSysPrefix(m.Topic) || m.QoS < 1 || m.QoS > 2 {
		n.log.Warn("tether: log message dropped, not a tenant topic", slog.String("tenant", m.Tenant),
			slog.String("topic", m.Topic), slog.Uint64("seq", m.Seq))

		return nil
	}

	n.pinFull(tt, m.Seq)

	origin := ""
	if m.ClientID != "" {
		origin = namespacedID(m.Tenant, m.ClientID)
	}

	err := b.inject(m.Tenant, m.Topic, m.Payload, m.QoS, m.Props, origin, m.Seq)
	if err != nil {
		n.pinAll(tt, m.Seq)

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
		s.mu.Lock()
		pos := s.pos
		s.mu.Unlock()

		if pos < seq {
			n.flip(tt, s)
		}
	}
}

// pinFull holds below seq the cursor of every session whose inflight map is
// full, since mochi drops what does not fit. tt.mu is held.
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
		s.pinLocked(seq - 1)
		s.mu.Unlock()
	}
}

// pinAll holds below seq the cursor of every live session of tt. tt.mu is
// held.
func (n *clusterNode) pinAll(tt *tenantTrack, seq uint64) {
	for s := range tt.sessions {
		s.mu.Lock()
		if s.mode == modeLive {
			s.pinLocked(seq - 1)
		}
		s.mu.Unlock()
	}
}

// flip puts s's subscriptions into mochi's topic index: from the next
// message the reader injects, s gets it live. tt.mu is held.
func (n *clusterNode) flip(tt *tenantTrack, s *session) {
	srv := n.mochi
	cl := n.client(s)

	s.mu.Lock()
	s.mode = modeLive
	s.liveFloor = s.pos
	loaded := s.loaded
	s.loaded = nil
	s.skip = nil
	s.mu.Unlock()

	delete(tt.waiting, s)

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

// startReplay starts s's replay for cl, after its CONNACK.
func (b *Broker) startReplay(s *session, cl *mqtt.Client) {
	n := b.cluster

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
	s.mu.Lock()
	cancel, done := s.replayCancel, s.replayDone
	s.replayCancel, s.replayDone = nil, nil
	s.mu.Unlock()

	if cancel == nil {
		return
	}

	cancel()
	<-done
}

// runReplay replays the log for s from its position to the reader's, again
// and again, until it has caught up, and then makes s live (or leaves it
// waiting, out of the index, for the reader to pass it).
func (b *Broker) runReplay(ctx context.Context, s *session, cl *mqtt.Client, done chan<- struct{}) {
	defer close(done)

	n := b.cluster
	tt := s.tt
	delay := replayPoll

	for ctx.Err() == nil {
		tt.mu.Lock()

		lp := tt.livePos.Load()

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

		err := n.Log.Replay(ctx, s.tenantID, pos+1, target, func(m cluster.LogMessage) error {
			return b.replayOne(s, cl, m)
		})

		switch {
		case err == nil:
			s.mu.Lock()
			s.pos = max(s.pos, target)
			s.mu.Unlock()

			delay = replayPoll
		case ctx.Err() != nil || errors.Is(err, errClientGone):
			return
		case errors.Is(err, errBackpressure):
			b.waitWindow(ctx, s, cl)
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

// waitWindow waits until cl has room for another delivery, or ctx ends.
func (b *Broker) waitWindow(ctx context.Context, s *session, cl *mqtt.Client) {
	tick := time.NewTicker(replayPoll)
	defer tick.Stop()

	for b.windowFull(s, cl) && !cl.Closed() {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// windowFull reports whether cl cannot take another QoS 1 or 2 delivery now:
// its inflight map is full, or as many deliveries as its Receive Maximum are
// unacknowledged.
func (b *Broker) windowFull(s *session, cl *mqtt.Client) bool {
	n := b.cluster
	if cl.State.Inflight.Len() >= n.maxInflight {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.recvMax > 0 && len(s.out) >= s.recvMax
}

// replayOne sends m to s if it matches, and moves s's position past it.
func (b *Broker) replayOne(s *session, cl *mqtt.Client, m cluster.LogMessage) error {
	s.mu.Lock()
	_, skip := s.skip[m.Seq]
	s.mu.Unlock()

	if !skip {
		if err := b.deliverDirect(s, cl, m); err != nil {
			return err
		}
	}

	s.mu.Lock()
	s.pos = max(s.pos, m.Seq)
	s.mu.Unlock()

	return nil
}

// deliverDirect sends one replayed message to cl, the way mochi's
// publishToClient would for the session's matching subscriptions: No Local,
// the read ACL, the lower QoS, subscription identifiers, message expiry, a
// packet ID and an inflight entry for QoS 1 and 2. It returns errBackpressure
// when the client has no room (nothing is sent) and errClientGone when the
// connection has ended.
func (b *Broker) deliverDirect(s *session, cl *mqtt.Client, m cluster.LogMessage) error {
	n := b.cluster
	srv := n.mochi

	st := b.state.get(cl)
	if st == nil || cl.Closed() {
		return errClientGone
	}

	if m.Tenant != s.tenantID || topic.Valid(m.Topic, false, subject.MaxTopicBytes) != nil || hasSysPrefix(m.Topic) ||
		m.QoS < 1 || m.QoS > 2 {
		return nil
	}

	sub, ids, ok := s.match(cl, m.Topic)
	if !ok {
		return nil
	}

	origin := ""
	if m.ClientID != "" {
		origin = namespacedID(m.Tenant, m.ClientID)
	}

	ns := topic.Namespace(s.tenantID, m.Topic)

	if sub.NoLocal && origin == cl.ID || !b.canRead(s.tenantID, st.acl(), ns) {
		return nil
	}

	now := time.Now()
	out := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: min(m.QoS, sub.Qos, srv.Options.Capabilities.MaximumQos)},
		TopicName:   ns,
		Payload:     m.Payload,
		Properties:  packetProps(m.Props),
		Origin:      origin,
		Created:     now.Unix(),
	}

	if len(ids) > 0 {
		out.Properties.SubscriptionIdentifier = ids
	}

	if m.Props.MessageExpiry > 0 && !m.PublishedAt.IsZero() {
		exp := m.PublishedAt.Add(time.Duration(m.Props.MessageExpiry) * time.Second)
		if !exp.After(now) {
			return nil // expired: the client is owed nothing
		}

		out.Expiry = exp.Unix()
	}

	tagSeq(&out, m.Seq)

	if out.FixedHeader.Qos > 0 {
		if b.windowFull(s, cl) {
			return errBackpressure
		}

		id, err := cl.NextPacketID()
		if err != nil {
			return errBackpressure
		}

		out.PacketID = uint16(id) //nolint:gosec // NextPacketID stays within the uint16 packet ID space

		if cl.State.Inflight.Set(out) {
			atomic.AddInt64(&srv.Info.Inflight, 1)
		}

		s.mu.Lock()
		s.out[out.PacketID] = m.Seq
		s.mu.Unlock()

		cl.State.Inflight.DecreaseSendQuota()
	}

	if err := cl.WritePacket(out); err != nil {
		return errClientGone
	}

	n.replayed.Add(1)

	return nil
}

// match merges the subscriptions of s (those on cl and those still held out
// of the index) that match the tenant-relative topic name, as mochi does for
// a delivery: the highest QoS, every subscription identifier, and No Local
// only when every matching subscription has it. Shared subscriptions are left
// to their group.
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

	if old, ok := s.out[pk.PacketID]; ok && old != seq {
		// mochi reused a packet ID it had dropped from the map: the older
		// delivery can no longer be told complete.
		s.pinLocked(min(old, seq) - 1)
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

	if cl.State.Inflight.Len() >= n.maxInflight {
		s.tt.fullMu.Lock()
		s.tt.full[s] = struct{}{}
		s.tt.fullMu.Unlock()
	}
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
	s.pinLocked(seq - 1)

	if s.out[pk.PacketID] == seq {
		delete(s.out, pk.PacketID)
	}

	s.mu.Unlock()
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
