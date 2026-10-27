package broker

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/topic"
)

// Shared subscriptions in cluster mode (spec "Shared subscriptions", rulings
// R5, R34, R35).
//
// A "$share/<group>/<filter>" subscription of a network client joins its
// tenant's group through cluster.Shared, which hands this node one message at
// a time for one of its members: QoS 1 and 2 from the group's durable
// consumer on the log, QoS 0 from the group's NATS queue group. mochi's own
// shared selection never runs (OnSelectSubscribers empties it), so neither the
// tenant's log reader nor a routed QoS 0 message reaches a shared subscription
// through mochi, and a shared match plays no part in a session's cursor.
//
// A message for a member is built here the way mochi's publishToClient would
// for the member's shared subscription alone (read ACL, the lower QoS, Retain
// As Published, its subscription identifier, message expiry) and written by
// the session's own writer goroutine, which takes the packet ID and the
// inflight entry, so a client that stops reading holds up only itself. QoS 1
// stays the group's until the member's PUBACK, and a session that ends first
// gives it back, so another member gets it at once. QoS 2 is the session's
// once sent (rulings R37, R39): the session record has it, the group's
// message is acked then, a resume anywhere completes it, and a session that
// ends drops it [MQTT-4.8.2-4], [MQTT-4.8.2-5]. QoS 0 is done once written.

// shareQueue bounds the messages waiting for one session's share writer.
const shareQueue = 64

var (
	// errShareBusy: the member cannot take a message now (no live
	// connection, its window or queue full). The group tries another.
	errShareBusy = fmt.Errorf("tether/broker: shared delivery: %w", cluster.ErrMemberBusy)
	// errShareRefused: the member may not have the message (its grants).
	errShareRefused = errors.New("tether/broker: shared delivery refused")
	// errShareLost: the member's session ended, or its connection, before
	// it was through with the message.
	errShareLost = errors.New("tether/broker: shared delivery lost with its session")
)

// shareMember is one session's membership of one shared group on this node.
type shareMember struct {
	tenantID string
	// sub is the namespaced shared subscription, its options read for each
	// delivery. Under shMu.
	sub packets.Subscription
	// leave is set once Join returned; nil while it runs.
	leave func()
}

// shareDelivery is one QoS 1 shared message in a session's inflight map,
// the group's until the PUBACK. (QoS 2 is the session's once sent: pinShare.)
type shareDelivery struct {
	done func(error)
	qos  byte
}

type shareItem struct {
	pk   packets.Packet
	seq  uint64 // the message's log sequence, 0 for QoS 0
	done func(error)
}

// shareOut is one session's share writer.
type shareOut struct {
	q chan shareItem
	// closed (shMu): nothing more is queued. ended: the session ended, and
	// whatever is still queued is given back.
	closed bool
	ended  atomic.Bool
	queued atomic.Int64 // QoS 1 and 2 items queued, not yet in the inflight map
}

// sharedNode is the broker's side of the shared groups, embedded in
// clusterNode.
type sharedNode struct {
	ownShared bool

	// shares holds, per namespaced client ID, its memberships by namespaced
	// shared filter; shPend its QoS 1 and 2 shared deliveries in flight by
	// packet ID; shOut its writer. All under shMu, which is never held
	// across a Shared call or a write.
	shMu   sync.Mutex
	shares map[string]map[string]*shareMember
	shPend map[string]map[uint16]*shareDelivery
	shOut  map[string]*shareOut

	// Counters for tests: shared messages written, finished by the member,
	// given back as lost, and joins that failed.
	sharedSent, sharedDone, sharedLost, shareErrors atomic.Uint64

	// Test seams, set before Start: shareBeforeSend runs in the session's
	// writer once it has the connection to send to, sharePinned right after
	// a QoS 2 pin enters the session's QoS2Out.
	shareBeforeSend func(nsID string)
	sharePinned     func()
}

func newSharedNode() sharedNode {
	return sharedNode{
		shares: make(map[string]map[string]*shareMember),
		shPend: make(map[string]map[uint16]*shareDelivery),
		shOut:  make(map[string]*shareOut),
	}
}

// splitShared splits a tenant-relative "$share/<group>/<filter>".
func splitShared(rel string) (group, filter string, ok bool) {
	first, rest, ok := strings.Cut(rel, "/")
	if !ok || !strings.EqualFold(first, "$share") {
		return "", "", false
	}

	group, filter, ok = strings.Cut(rest, "/")

	return group, filter, ok && group != "" && filter != ""
}

// sharedParts returns the group and tenant-relative filter of a namespaced
// shared filter of tenantID.
func sharedParts(tenantID, nsFilter string) (group, filter string, ok bool) {
	if !mqtt.IsSharedFilter(nsFilter) {
		return "", "", false
	}

	rel, ok := topic.Strip(tenantID, nsFilter)
	if !ok {
		return "", "", false
	}

	return splitShared(rel)
}

// joinShared makes nsID a member of the group of sub, a namespaced shared
// subscription of tenantID, unless it is one already (a SUBSCRIBE that
// replaces the subscription only updates its options). The Shared call runs
// outside shMu. It returns Join's error, after which nsID is no member.
func (n *clusterNode) joinShared(nsID, tenantID string, sub packets.Subscription) error {
	group, filter, ok := sharedParts(tenantID, sub.Filter)
	if !ok {
		return fmt.Errorf("tether/broker: shared filter %q: %w", sub.Filter, errShareRefused)
	}

	if err := subject.CheckTopic(filter); err != nil {
		return err
	}

	n.shMu.Lock()

	set := n.shares[nsID]
	if m, ok := set[sub.Filter]; ok {
		m.sub = sub
		n.shMu.Unlock()

		return nil
	}

	if set == nil {
		set = make(map[string]*shareMember)
		n.shares[nsID] = set
	}

	m := &shareMember{tenantID: tenantID, sub: sub}
	set[sub.Filter] = m
	n.shMu.Unlock()

	leave, err := n.Shared.Join(tenantID, group, filter, n.shareDeliver(nsID, sub.Filter))

	n.shMu.Lock()

	if n.shares[nsID][sub.Filter] == m {
		if err == nil {
			m.leave = leave
		} else {
			n.forgetShare(nsID, sub.Filter)
		}

		n.shMu.Unlock()

		return err
	}

	n.shMu.Unlock()

	// Left while Join ran: the leave skipped the group because the member
	// was pending, so it leaves here.
	if err == nil {
		leave()
	}

	return err
}

// forgetShare removes one membership. shMu is held.
func (n *clusterNode) forgetShare(nsID, nsFilter string) {
	set := n.shares[nsID]
	delete(set, nsFilter)

	if len(set) == 0 {
		delete(n.shares, nsID)
	}
}

// leaveShared ends nsID's memberships of the namespaced shared filters it no
// longer holds (an UNSUBSCRIBE, a prune). Deliveries already on their way
// still finish: MQTT has the server complete what it started sending.
func (n *clusterNode) leaveShared(nsID string, nsFilters []string) {
	var leaves []func()

	n.shMu.Lock()

	for _, f := range nsFilters {
		m, ok := n.shares[nsID][f]
		if !ok {
			continue
		}

		n.forgetShare(nsID, f)

		if m.leave != nil {
			leaves = append(leaves, m.leave)
		}
	}

	if out := n.shOut[nsID]; out != nil && len(n.shares[nsID]) == 0 {
		// No membership left: the writer sends what it has and ends.
		delete(n.shOut, nsID)
		out.closed = true
		close(out.q)
	}

	n.shMu.Unlock()

	for _, leave := range leaves {
		leave()
	}
}

// endShared ends every membership of nsID, whose session ended on this node,
// and gives its unacked shared QoS 1 deliveries back to their groups, for
// another member. Its shared QoS 2 deliveries are the session's already.
func (n *clusterNode) endShared(nsID string) {
	n.shMu.Lock()

	ms := n.shares[nsID]
	delete(n.shares, nsID)

	pend := n.shPend[nsID]
	delete(n.shPend, nsID)

	if out := n.shOut[nsID]; out != nil {
		delete(n.shOut, nsID)
		out.ended.Store(true)
		out.closed = true
		close(out.q)
	}

	n.shMu.Unlock()

	for _, m := range ms {
		if m.leave != nil {
			m.leave()
		}
	}

	for _, d := range pend {
		d.done(errShareLost)
		n.sharedLost.Add(1)
	}
}

// shareDeliver is the deliver function of nsID's membership of nsFilter.
func (n *clusterNode) shareDeliver(nsID, nsFilter string) cluster.SharedDeliver {
	return func(m cluster.LogMessage, done func(error)) error {
		return n.b.deliverShared(nsID, nsFilter, m, done)
	}
}

// shareClient returns the connection serving nsID's session and its CONNACK
// channel, or false when the node serves no live session under nsID.
func (n *clusterNode) shareClient(nsID string) (*session, *mqtt.Client, <-chan struct{}, bool) {
	n.sessMu.Lock()
	defer n.sessMu.Unlock()

	s := n.sess[nsID]
	if s == nil || s.ended || s.lost || s.cl == nil || s.cl.Closed() {
		return nil, nil, nil, false
	}

	return s, s.cl, s.connacked, true
}

// deliverShared prepares a group's message for the member nsID holds through
// nsFilter and queues it on the session's writer. It refuses (and the group
// tries another member) when the session has no live connection here, the
// connection's window or the writer's queue is full, or the member may not
// read the topic. An expired message is finished at once.
func (b *Broker) deliverShared(nsID, nsFilter string, m cluster.LogMessage, done func(error)) error {
	n := b.cluster

	n.gate.RLock()
	srv := n.srv
	n.gate.RUnlock()

	if srv == nil {
		return errShareBusy
	}

	s, cl, acked, ok := n.shareClient(nsID)
	if !ok {
		return errShareBusy
	}

	select {
	case <-acked:
	default:
		return errShareBusy // its CONNACK is not out yet
	}

	st := b.state.get(cl)
	if st == nil {
		return errShareBusy
	}

	n.shMu.Lock()
	mb := n.shares[nsID][nsFilter]

	var sub packets.Subscription
	if mb != nil {
		sub = mb.sub
	}
	n.shMu.Unlock()

	if mb == nil {
		return errShareBusy // left meanwhile: another member
	}

	if m.Tenant != mb.tenantID || topic.Valid(m.Topic, false, subject.MaxTopicBytes) != nil || hasSysPrefix(m.Topic) || m.QoS > 2 {
		return errShareRefused
	}

	ns := topic.Namespace(mb.tenantID, m.Topic)
	if !b.canRead(mb.tenantID, st.acl(), ns) {
		return errShareRefused
	}

	origin := ""
	if m.ClientID != "" {
		origin = namespacedID(m.Tenant, m.ClientID)
	}

	now := time.Now()
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish, Qos: min(m.QoS, sub.Qos, srv.Options.Capabilities.MaximumQos),
			// [MQTT-3.3.1-12], [MQTT-3.3.1-13].
			Retain: m.Retain && sub.RetainAsPublished && cl.Properties.ProtocolVersion == 5,
		},
		TopicName:  ns,
		Payload:    m.Payload,
		Properties: packetProps(m.Props),
		Origin:     origin,
		Created:    now.Unix(),
		// mochi reads it to expire the inflight entry.
		ProtocolVersion: cl.Properties.ProtocolVersion,
	}

	if sub.Identifier > 0 {
		pk.Properties.SubscriptionIdentifier = []int{sub.Identifier}
	}

	if m.Props.MessageExpiry > 0 {
		at := m.PublishedAt
		if at.IsZero() {
			at = now
		}

		exp := at.Add(time.Duration(m.Props.MessageExpiry) * time.Second)
		if !exp.After(now) {
			done(nil) // expired: nobody is owed it

			return nil
		}

		pk.Expiry = exp.Unix()
	}

	return n.enqueueShare(nsID, nsFilter, s, cl, pk, m.Seq, done)
}

// enqueueShare queues pk for nsID's writer, starting the writer on first use.
func (n *clusterNode) enqueueShare(nsID, nsFilter string, s *session, cl *mqtt.Client, pk packets.Packet, seq uint64, done func(error)) error {
	if pk.FixedHeader.Qos > 0 && !n.outboundRoom(s, cl, n.sharedQueued(nsID)) {
		return errShareBusy
	}

	n.shMu.Lock()
	defer n.shMu.Unlock()

	if n.shares[nsID][nsFilter] == nil {
		return errShareBusy
	}

	out := n.shOut[nsID]
	if out == nil {
		out = &shareOut{q: make(chan shareItem, shareQueue)}
		n.shOut[nsID] = out

		go n.runShareOut(nsID, out)
	}

	select {
	case out.q <- shareItem{pk: pk, seq: seq, done: done}:
	default:
		return errShareBusy
	}

	if pk.FixedHeader.Qos > 0 {
		out.queued.Add(1)
	}

	return nil
}

// sharedQueued counts nsID's QoS 1 and 2 shared messages queued for its
// writer, not yet in the inflight map.
func (n *clusterNode) sharedQueued(nsID string) int {
	n.shMu.Lock()
	defer n.shMu.Unlock()

	if out := n.shOut[nsID]; out != nil {
		return int(out.queued.Load())
	}

	return 0
}

// sharedOutstanding counts nsID's shared messages the group still owns and
// the client has not finished: QoS 1 in flight and QoS 1 and 2 queued. The
// replay's window counts them (windowFull), with the pinned QoS 2 ones from
// the session's QoS2Out.
func (n *clusterNode) sharedOutstanding(nsID string) int {
	n.shMu.Lock()
	defer n.shMu.Unlock()

	c := len(n.shPend[nsID])
	if out := n.shOut[nsID]; out != nil {
		c += int(out.queued.Load())
	}

	return c
}

// outboundRoom reports whether cl can be sent one more QoS 1 or 2 message,
// with extra more already promised to it and not yet in its inflight map:
// its inflight map stays under MaxInflight, and its unacknowledged outbound
// messages under the client's Receive Maximum [MQTT-3.3.4-9]. Outbound are
// the inflight map's PUBLISH and PUBREL entries (whoever sent them: mochi,
// the replay, a group; a PUBREC entry is the client's own QoS 2 publish)
// plus what the replay has queued. It walks the inflight map, at most
// MaxInflight entries.
func (n *clusterNode) outboundRoom(s *session, cl *mqtt.Client, extra int) bool {
	s.mu.Lock()
	queued := len(s.queued)
	s.mu.Unlock()

	if cl.State.Inflight.Len()+queued+extra >= n.maxInflight {
		return false
	}

	rm := int(cl.Properties.Props.ReceiveMaximum)
	if rm == 0 {
		return true
	}

	outbound := queued + extra

	for _, pk := range cl.State.Inflight.GetAll(false) {
		if t := pk.FixedHeader.Type; t == packets.Publish || t == packets.Pubrel {
			outbound++
		}
	}

	return outbound < rm
}

// runShareOut writes nsID's shared messages until its queue is closed.
func (n *clusterNode) runShareOut(nsID string, out *shareOut) {
	for it := range out.q {
		n.sendShare(nsID, out, it)

		if it.pk.FixedHeader.Qos > 0 {
			out.queued.Add(-1)
		}
	}
}

// sendShare writes one shared message to the connection serving nsID, once
// its CONNACK is out (a takeover on this node switches connections) and it
// has room. A QoS 1 or 2 message takes a packet ID and an inflight entry
// first, as mochi's own deliveries do, so mochi resends it to a connection
// that takes the session over.
//
// QoS 1 stays the group's until the PUBACK (shareAcked): a session that ends
// first gives it back for another member. QoS 2 is pinned to the session
// when it is sent (ruling R39): the record gets it (QoS2Out, Seq 0, Shared =
// its log sequence) before the PUBLISH goes out, and the group's message is
// acked then. A connection that breaks before the flow completes gets it
// again on resume, on any node, under the same packet ID [MQTT-4.8.2-4]; a
// session that ends drops it, and no other member gets it [MQTT-4.8.2-5].
func (n *clusterNode) sendShare(nsID string, out *shareOut, it shareItem) {
	if out.ended.Load() {
		it.done(errShareLost)
		n.sharedLost.Add(1)

		return
	}

	pk := it.pk
	qos := pk.FixedHeader.Qos

	s, cl, ok := n.shareTarget(nsID, out, qos)
	if !ok {
		it.done(errShareLost)
		n.sharedLost.Add(1)

		return
	}

	if hook := n.shareBeforeSend; hook != nil {
		hook(nsID)
	}

	if qos > 0 {
		id, err := cl.NextPacketID()
		if err != nil {
			it.done(errShareLost)
			n.sharedLost.Add(1)

			return
		}

		pk.PacketID = uint16(id) //nolint:gosec // NextPacketID stays within the uint16 packet ID space

		tracked := n.claimID(s, pk.PacketID)

		if qos == 1 {
			if !n.registerShare(nsID, out, pk.PacketID, it.done) {
				return
			}
		} else if !n.pinShare(s, cl, out, tracked, pk.PacketID, it) {
			return
		}

		var lp uint64
		if tracked {
			lp = s.tt.livePos.Load()
		}

		if cl.State.Inflight.Set(pk) {
			atomic.AddInt64(&n.mochi.Info.Inflight, 1)
			cl.State.Inflight.DecreaseSendQuota()
		}

		if tracked {
			// A full map makes mochi drop the session's next log delivery
			// silently: the cursor stays below it (as for an inbound QoS 2
			// reservation).
			n.pinIfFull(s, cl, lp)
		}
	}

	err := cl.WritePacket(pk)

	if qos > 0 && n.followShare(nsID, cl, pk, it.seq) {
		// The session moved to another connection of this node meanwhile:
		// the delivery went with it.
		n.sharedSent.Add(1)

		return
	}

	if err != nil {
		n.shareWriteFailed(nsID, s, cl, pk, it, err)

		return
	}

	n.sharedSent.Add(1)

	if qos == 0 {
		it.done(nil)
		n.sharedDone.Add(1)
	}
}

// followShare handles a same-node takeover that raced a QoS 1 or 2 shared
// delivery: mochi clones the old connection's inflight map into the new one
// and clears the old, so an entry set on the old connection after the clone
// is in neither, and a takeover is not a resume from the record, so nothing
// would send it again. When the session's connection is no longer cl, the
// packet goes into the current connection's inflight map and is written to
// it with DUP once its CONNACK is out (a copy mochi already resent from the
// clone is the same packet ID: the client takes it once). If the new
// connection gave that ID to another delivery meanwhile, this one moves to a
// fresh ID (renumberShare). It reports whether the session had moved; false
// leaves the outcome to cl's write.
func (n *clusterNode) followShare(nsID string, cl *mqtt.Client, pk packets.Packet, seq uint64) bool {
	moved := false

	for {
		s, cur, acked, ok := n.shareClient(nsID)
		if !ok || cur == cl {
			// The same connection, or the session ended or went to another
			// node: the record (QoS 2) or the session's end (QoS 1) has it.
			return moved
		}

		if !moved && cl.IsTakenOver() && cl.State.Inflight.Delete(pk.PacketID) {
			// The orphan on the connection that was taken over.
			atomic.AddInt64(&n.mochi.Info.Inflight, -1)
		}

		moved = true

		if held, taken := cur.State.Inflight.Get(pk.PacketID); taken && !samePublish(held, pk) {
			id, err := cur.NextPacketID()
			if err != nil {
				n.renumberShare(nsID, s, pk, 0, seq)

				return true
			}

			n.renumberShare(nsID, s, pk, uint16(id), seq) //nolint:gosec // NextPacketID stays within the uint16 packet ID space
			pk.PacketID = uint16(id)                      //nolint:gosec // as above
		}

		if cur.State.Inflight.Set(pk) {
			atomic.AddInt64(&n.mochi.Info.Inflight, 1)
			cur.State.Inflight.DecreaseSendQuota()
		}

		select {
		case <-acked:
		case <-time.After(connackWait):
			return true // it stays in cur's map; cur's own end handles it
		}

		pk.FixedHeader.Dup = true

		if cur.WritePacket(pk) == nil {
			return true
		}

		cl = cur
	}
}

// samePublish reports whether an inflight entry is this shared PUBLISH.
func samePublish(held, pk packets.Packet) bool {
	return held.FixedHeader.Type == packets.Publish && held.TopicName == pk.TopicName &&
		bytes.Equal(held.Payload, pk.Payload)
}

// renumberShare moves a shared delivery that followed a takeover to the
// packet ID id, because the new connection used its old one for another
// delivery. QoS 1: its pending entry moves (id 0: no ID left, it goes back
// to its group). QoS 2: the session's pin moves, re-added if the other
// delivery's record entry replaced it; the record is written by the next
// flush.
func (n *clusterNode) renumberShare(nsID string, s *session, pk packets.Packet, id uint16, seq uint64) {
	old := pk.PacketID

	if pk.FixedHeader.Qos == 1 {
		n.shMu.Lock()
		d := n.shPend[nsID][old]

		if d != nil {
			delete(n.shPend[nsID], old)

			if id != 0 {
				n.shPend[nsID][id] = d
			}
		}
		n.shMu.Unlock()

		if d != nil && id == 0 {
			d.done(errShareLost)
			n.sharedLost.Add(1)
		}

		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if or, ok := s.qos2Out[old]; ok && or.Seq == 0 && or.Shared == seq {
		delete(s.qos2Out, old)
	}

	if id != 0 {
		s.qos2Out[id] = cluster.OutRecord{State: outPublish, Shared: seq}
	}

	s.gen++
}

// claimID clears what s's books still hold under a packet ID mochi just
// handed out for a shared delivery: mochi gives out only IDs that are not in
// the inflight map, so a delivery the books still have under it was lost
// from the map (a client PUBLISH reusing the ID deletes it). A log delivery
// lost that way can no longer be told complete, so the session is pinned
// below it and replays it (as onQosPublish does). It reports whether s is
// tracked.
func (n *clusterNode) claimID(s *session, id uint16) bool {
	s.mu.Lock()

	tracked := s.tracked
	repin := false

	if seq, ok := s.out[id]; ok {
		repin = s.pinLocked(seq - 1)
		delete(s.out, id)
	}

	if _, ok := s.qos2Out[id]; ok {
		delete(s.qos2Out, id)
		s.gen++
	}

	s.mu.Unlock()

	if repin {
		go n.b.repin(s)
	}

	return tracked
}

// registerShare records a QoS 1 shared delivery until its PUBACK. A stale
// one under the same ID (mochi lost its entry) goes back to its group. It
// reports false, after giving the message back, when the session ended.
func (n *clusterNode) registerShare(nsID string, out *shareOut, id uint16, done func(error)) bool {
	n.shMu.Lock()

	if out.ended.Load() {
		n.shMu.Unlock()
		done(errShareLost)
		n.sharedLost.Add(1)

		return false
	}

	set := n.shPend[nsID]
	if set == nil {
		set = make(map[uint16]*shareDelivery)
		n.shPend[nsID] = set
	}

	stale := set[id]
	set[id] = &shareDelivery{done: done, qos: 1}
	n.shMu.Unlock()

	if stale != nil {
		stale.done(errShareLost)
		n.sharedLost.Add(1)
	}

	return true
}

// pinShare makes a QoS 2 shared delivery the session's (ruling R39): for a
// tracked session the record gets it before the PUBLISH goes out; then the
// group's message is acked. When that write fails (persistNow then
// disconnects or stops the client), or the session's end got there first
// (its last record was written without the pin), nothing
// was sent, so the pin is removed, the message goes back to its group, and it
// reports false. A pin the session's last record holds stays: the resume
// sends it. An untracked session ends with its connection, so its delivery
// is never sent again anywhere (an ended one gives the message back).
func (n *clusterNode) pinShare(s *session, cl *mqtt.Client, out *shareOut, tracked bool, id uint16, it shareItem) bool {
	if tracked {
		s.mu.Lock()
		s.qos2Out[id] = cluster.OutRecord{State: outPublish, Shared: it.seq}
		s.gen++
		gen := s.gen
		s.mu.Unlock()

		if hook := n.sharePinned; hook != nil {
			hook()
		}

		// persist reports success without writing for a session whose last
		// record is in, so the pin counts only once a written record had
		// it: the session's last record snapshotted after the insert holds
		// it (and the resume sends it), one snapshotted before does not.
		ok := n.persistNow(s, cl)
		if ok {
			s.mu.Lock()
			ok = s.savedGen >= gen
			s.mu.Unlock()
		}

		if !ok {
			s.mu.Lock()
			if or, found := s.qos2Out[id]; found && or.Seq == 0 && or.Shared == it.seq {
				delete(s.qos2Out, id)
			}
			s.mu.Unlock()

			it.done(errShareLost)
			n.sharedLost.Add(1)

			return false
		}
	} else if out.ended.Load() {
		it.done(errShareLost)
		n.sharedLost.Add(1)

		return false
	}

	it.done(nil)
	n.sharedDone.Add(1)

	return true
}

// shareWriteFailed handles a shared message whose write failed. QoS 1 comes
// out of the inflight map (send quota restored, as mochi's own rollback
// does) and goes back to its group when the connection is gone. QoS 2 is the
// session's already: on a lost connection it stays in the inflight map and
// the record, for the resume. A write that failed on a live connection (over
// the client's Maximum Packet Size, say) never will succeed: the message is
// dropped, as mochi drops such a delivery.
func (n *clusterNode) shareWriteFailed(nsID string, s *session, cl *mqtt.Client, pk packets.Packet, it shareItem, err error) {
	qos := pk.FixedHeader.Qos
	closed := cl.Closed()

	if qos == 2 && closed {
		return
	}

	if qos > 0 {
		if qos == 1 {
			n.shMu.Lock()
			if d := n.shPend[nsID][pk.PacketID]; d != nil {
				delete(n.shPend[nsID], pk.PacketID)
			}
			n.shMu.Unlock()
		} else {
			s.mu.Lock()
			if or, ok := s.qos2Out[pk.PacketID]; ok && or.Seq == 0 {
				delete(s.qos2Out, pk.PacketID)
				s.gen++
			}
			s.mu.Unlock()
		}

		if cl.State.Inflight.Delete(pk.PacketID) {
			atomic.AddInt64(&n.mochi.Info.Inflight, -1)
			cl.State.Inflight.IncreaseSendQuota()
		}
	}

	if !closed {
		n.log.Info("tether: shared message not sent to its member, dropped", slog.String("client", nsID), slog.Any("error", err))

		if qos != 2 { // QoS 2 was finished when it was pinned
			it.done(nil)
			n.sharedDone.Add(1)
		}

		return
	}

	it.done(errShareLost)
	n.sharedLost.Add(1)
}

// shareTarget waits until nsID's session has a connection whose CONNACK is
// out and, for QoS 1 and 2, room for one more outbound message: the room
// counted at enqueue can be gone by the time the writer gets to the item
// (the replay, mochi or other shared messages took it, or the connection
// changed). It blocks only this session's writer. False when the session or
// its connection is gone, or its CONNACK does not come within connackWait.
func (n *clusterNode) shareTarget(nsID string, out *shareOut, qos byte) (*session, *mqtt.Client, bool) {
	for {
		if out.ended.Load() {
			return nil, nil, false
		}

		s, cl, acked, ok := n.shareClient(nsID)
		if !ok {
			return nil, nil, false
		}

		select {
		case <-acked:
		case <-time.After(connackWait):
			return nil, nil, false
		}

		if cl.Closed() {
			return nil, nil, false
		}

		if qos == 0 || n.outboundRoom(s, cl, 0) {
			return s, cl, true
		}

		time.Sleep(replayPoll)
	}
}

// shareAcked handles a PUBACK or PUBCOMP read from nsID's connection for one
// of its shared deliveries: the member is through with it.
func (n *clusterNode) shareAcked(nsID string, id uint16, typ byte) {
	n.shMu.Lock()

	d := n.shPend[nsID][id]
	if d == nil || (typ == packets.Puback) != (d.qos == 1) {
		n.shMu.Unlock()

		return
	}

	delete(n.shPend[nsID], id)
	n.shMu.Unlock()

	d.done(nil)
	n.sharedDone.Add(1)
}

// shareDropped handles mochi dropping one of cl's shared deliveries from
// the inflight map: expired (finished, nobody is owed it), or pruned at a
// CONNACK because the grants no longer allow it (lost: another member may).
//
// Only for the connection that serves the session: a same-node takeover
// clones the inflight map into the new connection and then clears the old
// one, which fires OnQosDropped for every entry; those deliveries live on in
// the new connection.
func (n *clusterNode) shareDropped(cl *mqtt.Client, pk packets.Packet, lost bool) {
	if _, tagged := seqOf(pk); tagged || pk.FixedHeader.Type != packets.Publish {
		return
	}

	if cl.IsTakenOver() || n.sessionOf(cl) == nil {
		return
	}

	nsID := cl.ID

	if s := n.sessionOf(cl); s != nil {
		// A pinned QoS 2 shared delivery leaves the record: expired, or no
		// longer allowed; it goes to nobody else either way.
		s.mu.Lock()
		if or, ok := s.qos2Out[pk.PacketID]; ok && or.Seq == 0 {
			delete(s.qos2Out, pk.PacketID)
			s.gen++
		}
		s.mu.Unlock()
	}

	n.shMu.Lock()

	d := n.shPend[nsID][pk.PacketID]
	if d == nil {
		n.shMu.Unlock()

		return
	}

	delete(n.shPend[nsID], pk.PacketID)
	n.shMu.Unlock()

	if lost {
		d.done(errShareLost)
		n.sharedLost.Add(1)

		return
	}

	d.done(nil)
	n.sharedDone.Add(1)
}

// joinLoaded joins the groups of a resumed session's shared subscriptions
// (loadSession). A failure is logged and counted: the subscription stays in
// the session's record, and the group's messages wait in its consumer.
func (n *clusterNode) joinLoaded(s *session, sub packets.Subscription) {
	if err := n.joinShared(s.nsID, s.tenantID, sub); err != nil {
		n.shareErrors.Add(1)
		n.log.Warn("tether: shared group of a resumed subscription not joined", slog.String("client", s.nsID),
			slog.String("filter", sub.Filter), slog.Any("error", err))
	}
}

// sharedOf lists nsID's memberships by namespaced filter, for tests.
func (n *clusterNode) sharedOf(nsID string) []string {
	n.shMu.Lock()
	defer n.shMu.Unlock()

	out := make([]string, 0, len(n.shares[nsID]))
	for f := range n.shares[nsID] {
		out = append(out, f)
	}

	return out
}

// pendingShares counts nsID's shared deliveries in flight, for tests.
func (n *clusterNode) pendingShares(nsID string) int {
	n.shMu.Lock()
	defer n.shMu.Unlock()

	return len(n.shPend[nsID])
}

// joinShared is OnSubscribed's handling of one accepted shared filter: cl's
// session joins the group. It reports false when the join failed, after
// removing mochi's subscription (the SUBACK then refuses it with 0x80), as a
// failed route does. A filter a prune removed while the join ran is left
// again.
func (h *hook) joinShared(cl *mqtt.Client, tenantID string, sub packets.Subscription) bool {
	n := h.b.cluster

	if held, ok := cl.State.Subscriptions.Get(sub.Filter); ok {
		sub = held // the options mochi stored
	}

	err := n.joinShared(cl.ID, tenantID, sub)
	if err == nil {
		if _, held := cl.State.Subscriptions.Get(sub.Filter); !held {
			n.leaveShared(cl.ID, []string{sub.Filter})
		}

		return true
	}

	n.shareErrors.Add(1)
	n.log.Warn("tether: shared subscription refused, its group could not be joined", slog.String("client", cl.ID),
		slog.String("tenant", tenantID), slog.String("filter", sub.Filter), slog.Any("error", err))

	if h.server.Topics.Unsubscribe(sub.Filter, cl.ID) {
		atomic.AddInt64(&h.server.Info.Subscriptions, -1)
	}

	cl.State.Subscriptions.Delete(sub.Filter)

	return false
}
