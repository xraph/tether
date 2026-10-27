package broker

import (
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
// inflight entry, so a client that stops reading holds up only itself. The
// group's message is acked once the member is through with it: its PUBACK for
// QoS 1, its PUBCOMP for QoS 2, the write for QoS 0. A session that ends here
// gives back what it had not finished, so another member gets it at once;
// one past its PUBREC counts as finished (it was received, and sending it to
// another member would deliver it twice).

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

// shareDelivery is one QoS 1 or 2 shared message in a session's inflight map.
type shareDelivery struct {
	done   func(error)
	qos    byte
	pubrec bool
}

type shareItem struct {
	pk   packets.Packet
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
// and gives its unfinished shared deliveries back to their groups: those past
// their PUBREC count as finished, the rest go to another member.
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
		if d.pubrec {
			d.done(nil)
			n.sharedDone.Add(1)

			continue
		}

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

	_, cl, acked, ok := n.shareClient(nsID)
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

	return n.enqueueShare(nsID, nsFilter, cl, pk, done)
}

// enqueueShare queues pk for nsID's writer, starting the writer on first use.
func (n *clusterNode) enqueueShare(nsID, nsFilter string, cl *mqtt.Client, pk packets.Packet, done func(error)) error {
	limit := n.maxInflight
	if rm := int(cl.Properties.Props.ReceiveMaximum); rm > 0 {
		limit = min(limit, rm)
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

	if pk.FixedHeader.Qos > 0 && cl.State.Inflight.Len()+int(out.queued.Load()) >= limit {
		return errShareBusy
	}

	select {
	case out.q <- shareItem{pk: pk, done: done}:
	default:
		return errShareBusy
	}

	if pk.FixedHeader.Qos > 0 {
		out.queued.Add(1)
	}

	return nil
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
// its CONNACK is out (a takeover on this node switches connections). A QoS 1
// or 2 message takes a packet ID and an inflight entry first, as mochi's own
// deliveries do, so mochi resends it to a connection that takes the session
// over; its completion comes back through OnPacketRead (shareAcked).
func (n *clusterNode) sendShare(nsID string, out *shareOut, it shareItem) {
	if out.ended.Load() {
		it.done(errShareLost)
		n.sharedLost.Add(1)

		return
	}

	s, cl, acked, ok := n.shareClient(nsID)
	if ok {
		select {
		case <-acked:
		case <-time.After(connackWait):
			ok = false
		}
	}

	if !ok || cl.Closed() {
		it.done(errShareLost)
		n.sharedLost.Add(1)

		return
	}

	pk := it.pk
	qos := pk.FixedHeader.Qos

	if qos > 0 {
		id, err := cl.NextPacketID()
		if err != nil {
			it.done(errShareLost)
			n.sharedLost.Add(1)

			return
		}

		pk.PacketID = uint16(id) //nolint:gosec // NextPacketID stays within the uint16 packet ID space

		var stale *shareDelivery

		n.shMu.Lock()

		if out.ended.Load() {
			n.shMu.Unlock()
			it.done(errShareLost)
			n.sharedLost.Add(1)

			return
		}

		set := n.shPend[nsID]
		if set == nil {
			set = make(map[uint16]*shareDelivery)
			n.shPend[nsID] = set
		}

		// mochi handed out an ID we still track: it lost that entry (a
		// client PUBLISH reusing the ID deletes it), so the older delivery
		// can no longer be told complete and goes to another member.
		stale = set[pk.PacketID]
		set[pk.PacketID] = &shareDelivery{done: it.done, qos: qos}
		n.shMu.Unlock()

		if stale != nil {
			stale.done(errShareLost)
			n.sharedLost.Add(1)
		}

		s.mu.Lock()
		tracked := s.tracked
		s.mu.Unlock()

		var lp uint64
		if tracked {
			lp = s.tt.livePos.Load()
		}

		if cl.State.Inflight.Set(pk) {
			atomic.AddInt64(&n.mochi.Info.Inflight, 1)
		}

		cl.State.Inflight.DecreaseSendQuota()

		if tracked {
			// A full map makes mochi drop the session's next log delivery
			// silently: the cursor stays below it (as for an inbound QoS 2
			// reservation).
			n.pinIfFull(s, cl, lp)
		}
	}

	if err := cl.WritePacket(pk); err != nil {
		if qos > 0 {
			n.shMu.Lock()
			if d := n.shPend[nsID][pk.PacketID]; d != nil && d.qos == qos {
				delete(n.shPend[nsID], pk.PacketID)
			}
			n.shMu.Unlock()

			if cl.State.Inflight.Delete(pk.PacketID) {
				atomic.AddInt64(&n.mochi.Info.Inflight, -1)
			}
		}

		if !cl.Closed() {
			// Not a lost connection: the packet itself could not go out
			// (over the client's Maximum Packet Size, say). It never will
			// to this member, so it is finished rather than offered again,
			// as mochi drops such a delivery.
			n.log.Info("tether: shared message not sent to its member, dropped", slog.String("client", nsID), slog.Any("error", err))
			it.done(nil)
			n.sharedDone.Add(1)

			return
		}

		it.done(errShareLost)
		n.sharedLost.Add(1)

		return
	}

	n.sharedSent.Add(1)

	if qos == 0 {
		it.done(nil)
		n.sharedDone.Add(1)
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

// sharePubrec handles a PUBREC for a QoS 2 shared delivery: the member has
// the message, so losing the session from here on finishes it rather than
// giving it to another member. A PUBREC with a failure code ends the flow.
func (n *clusterNode) sharePubrec(nsID string, id uint16, code byte) {
	n.shMu.Lock()

	d := n.shPend[nsID][id]
	if d == nil || d.qos != 2 {
		n.shMu.Unlock()

		return
	}

	if code < packets.ErrUnspecifiedError.Code {
		d.pubrec = true
		n.shMu.Unlock()

		return
	}

	delete(n.shPend[nsID], id)
	n.shMu.Unlock()

	d.done(nil)
	n.sharedDone.Add(1)
}

// shareDropped handles mochi dropping one of nsID's shared deliveries from
// the inflight map: expired (finished, nobody is owed it), or pruned at a
// CONNACK because the grants no longer allow it (lost: another member may).
func (n *clusterNode) shareDropped(nsID string, pk packets.Packet, lost bool) {
	if _, tagged := seqOf(pk); tagged || pk.FixedHeader.Type != packets.Publish {
		return
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
