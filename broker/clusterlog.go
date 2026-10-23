package broker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"slices"
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

// QoS 1 and 2 in cluster mode (spike 1, async ack).
//
// A network client's QoS 1 or 2 PUBLISH is not delivered by mochi. OnPublish
// checks it as always, hands it to the connection's pubPipe and returns
// packets.ErrRejectPacket, so mochi neither acks nor fans it out. The pipe
// appends it to the cell's log and writes the PUBACK (QoS 1) or PUBREC (QoS 2)
// once JetStream has committed it, in the order the PUBLISHes arrived. Every
// node, this one included, delivers the message to its own sessions from the
// log: one Reader per tenant the node serves, injecting each message through
// the injector at its original QoS.

const (
	// defaultReaderIdle is how long a tenant's reader keeps running after
	// the node's last connection of the tenant has gone.
	defaultReaderIdle = time.Minute
	// defaultReaderSweep is how often idle readers are looked for.
	defaultReaderSweep = 10 * time.Second
	// defaultAppendAttempt bounds one wait for a commit before the append
	// is sent again (with the same message ID, so it is stored once).
	defaultAppendAttempt = 10 * time.Second
	// defaultAppendRetry and defaultAppendRetryMax pace the retries of a
	// failed append, doubling, while the connection is open (ruling R23).
	defaultAppendRetry    = 100 * time.Millisecond
	defaultAppendRetryMax = 5 * time.Second
	// closedPoll is how often a 3.1.1 publish waiting for a receive slot
	// checks that its connection is still open.
	closedPoll = 250 * time.Millisecond
)

// tenantReader is the node's reader of one tenant's log. The fields marked
// so are guarded by clusterNode.readersMu.
type tenantReader struct {
	tenant string

	// ready is closed when the first start has finished; err is its error,
	// set before.
	ready chan struct{}
	err   error

	// quit is closed when the node stops the reader.
	quit chan struct{}

	r          *cluster.Reader // readersMu; nil while the first start runs
	lastActive time.Time       // readersMu
	stopped    bool            // readersMu
}

// ensureReader makes sure this node reads tenantID's log, starting a reader
// from "new" when none runs. A CONNECT of the tenant calls it before its
// CONNACK (ruling R24), so no SUBSCRIBE of the connection can come before the
// node's read position. Concurrent callers for one tenant share one start.
// The Log call runs under no lock.
func (b *Broker) ensureReader(tenantID string) error {
	n := b.cluster

	n.readersMu.Lock()

	if n.readersClosed {
		n.readersMu.Unlock()

		return errNotInjecting
	}

	if e := n.readers[tenantID]; e != nil {
		e.lastActive = time.Now()
		n.readersMu.Unlock()
		<-e.ready

		return e.err
	}

	e := &tenantReader{tenant: tenantID, ready: make(chan struct{}), quit: make(chan struct{}), lastActive: time.Now()}
	n.readers[tenantID] = e
	n.readersMu.Unlock()

	r, err := n.Log.Reader(tenantID, 0, b.logDeliver(tenantID))

	n.readersMu.Lock()

	switch {
	case err != nil:
		e.err = fmt.Errorf("tether/broker: start log reader: %w", err)
		e.stopped = true

		if n.readers[tenantID] == e {
			delete(n.readers, tenantID)
		}
	case e.stopped:
		// closeReaders ran meanwhile.
		e.err = errNotInjecting
	default:
		e.r = r
	}

	n.readersMu.Unlock()
	close(e.ready)

	if err != nil {
		return e.err
	}

	if e.err != nil {
		r.Stop()

		return e.err
	}

	go b.watchReader(e, r)

	return nil
}

// watchReader restarts e's reader when it ends without the node stopping it
// (a closed connection, an iterator that gave up), from the message after
// the last one it finished, so nothing is skipped. Restarts that fail are
// retried with backoff until they work or the reader is stopped.
func (b *Broker) watchReader(e *tenantReader, r *cluster.Reader) {
	n := b.cluster

	for {
		select {
		case <-e.quit:
			return
		case <-r.Done():
		}

		n.readersMu.Lock()
		stopped := e.stopped
		n.readersMu.Unlock()

		if stopped {
			return
		}

		from := r.Position() + 1
		n.readerRestarts.Add(1)
		n.log.Warn("tether: log reader ended by itself, restarting it", slog.String("tenant", e.tenant), slog.Uint64("from", from))

		delay := n.appendRetry

		for {
			nr, err := n.Log.Reader(e.tenant, from, b.logDeliver(e.tenant))
			if err == nil {
				n.readersMu.Lock()

				if e.stopped {
					n.readersMu.Unlock()
					nr.Stop()

					return
				}

				e.r = nr
				n.readersMu.Unlock()

				r = nr

				break
			}

			n.log.Warn("tether: log reader restart failed", slog.String("tenant", e.tenant), slog.Uint64("from", from), slog.Any("error", err))

			select {
			case <-e.quit:
				return
			case <-time.After(delay):
			}

			delay = min(2*delay, n.appendRetryMax)
		}
	}
}

// runReaderSweep stops readers of tenants with no connection on this node
// for readerIdle, until stop is closed.
func (b *Broker) runReaderSweep(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	tick := time.NewTicker(b.cluster.readerSweep)
	defer tick.Stop()

	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			b.sweepReaders()
		}
	}
}

// sweepReaders stops each reader whose tenant has had no live connection on
// this node for readerIdle. A connection counts from its CONNECT on; one
// that comes while the reader stops starts a new reader.
func (b *Broker) sweepReaders() {
	n := b.cluster
	live := b.state.liveTenants()
	now := time.Now()

	var idle []*cluster.Reader

	n.readersMu.Lock()

	for tenantID, e := range n.readers {
		if e.r == nil {
			continue // still starting
		}

		if live[tenantID] > 0 {
			e.lastActive = now

			continue
		}

		if now.Sub(e.lastActive) < n.readerIdle {
			continue
		}

		e.stopped = true
		close(e.quit)
		delete(n.readers, tenantID)
		idle = append(idle, e.r)
	}

	n.readersMu.Unlock()

	for _, r := range idle {
		r.Stop()
	}
}

// closeReaders stops the sweep and every reader, for Stop. No reader starts
// after it.
func (b *Broker) closeReaders() {
	n := b.cluster

	n.readersMu.Lock()

	if n.readersClosed {
		n.readersMu.Unlock()

		return
	}

	n.readersClosed = true
	stop, done := n.sweepStop, n.sweepDone

	var all []*cluster.Reader

	for tenantID, e := range n.readers {
		e.stopped = true
		close(e.quit)
		delete(n.readers, tenantID)

		if e.r != nil {
			all = append(all, e.r)
		}
	}

	n.readersMu.Unlock()

	if stop != nil {
		close(stop)
		<-done
	}

	for _, r := range all {
		r.Stop()
	}
}

// startReaderSweep starts the idle sweep. Called once, from Start.
func (b *Broker) startReaderSweep() {
	n := b.cluster

	n.readersMu.Lock()
	n.sweepStop, n.sweepDone = make(chan struct{}), make(chan struct{})
	stop, done := n.sweepStop, n.sweepDone
	n.readersMu.Unlock()

	go b.runReaderSweep(stop, done)
}

// readerTenants lists the tenants this node reads, for tests.
func (n *clusterNode) readerTenants() []string {
	n.readersMu.Lock()
	defer n.readersMu.Unlock()

	out := make([]string, 0, len(n.readers))
	for t, e := range n.readers {
		if e.r != nil {
			out = append(out, t)
		}
	}

	slices.Sort(out)

	return out
}

// logDeliver is the function a reader of tenantID hands each log message to.
// The tenant is the reader's, which only reads "d.<tenant>.>"; a message
// claiming another tenant, an invalid topic or a $SYS one is dropped and
// logged. The rest is injected at its QoS on the tenant's namespaced topic,
// so mochi delivers it to the matching local sessions at the lower of that
// and their subscription's QoS, with the ACL check on every delivery and the
// outbound stripping. The publisher's client ID becomes the packet's origin
// for No Local.
func (b *Broker) logDeliver(tenantID string) func(cluster.LogMessage) error {
	return func(m cluster.LogMessage) error {
		n := b.cluster

		if m.Tenant != tenantID || !validTenant(m.Tenant) || topic.Valid(m.Topic, false, subject.MaxTopicBytes) != nil ||
			hasSysPrefix(m.Topic) || m.QoS < 1 || m.QoS > 2 {
			n.log.Warn("tether: log message dropped, not a tenant topic", slog.String("tenant", m.Tenant),
				slog.String("topic", m.Topic), slog.Uint64("seq", m.Seq))

			return nil
		}

		origin := ""
		if m.ClientID != "" {
			origin = namespacedID(m.Tenant, m.ClientID)
		}

		err := b.inject(m.Tenant, m.Topic, m.Payload, m.QoS, m.Props, origin)
		if errors.Is(err, errNotInjecting) {
			return nil
		}

		if err == nil {
			n.logDelivered.Add(1)
		}

		return err
	}
}

// pubPipe carries one connection's QoS 1 and 2 publishes to the log and
// writes their acks. Two goroutines, started with the first such publish:
// the starter sends each to the log in arrival order (so one client's
// messages are committed in the order it sent them), the writer waits for
// each commit in that same order and writes the ack, retrying a failed
// append while the connection is open. When the connection ends (close, from
// lastHook.OnDisconnect) whatever is pending is abandoned without an ack;
// the client sends it again when it reconnects.
type pubPipe struct {
	b   *Broker
	srv *mqtt.Server
	cl  *mqtt.Client
	st  *clientState

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	toStart   fifo[*pubItem]
	toAck     fifo[*pubItem]

	// pending counts publishes whose ack is not written yet, held the QoS 2
	// packet IDs whose PUBREC is written and whose PUBREL has not come.
	// Together they are what counts against a 5.0 client's receive maximum.
	// changed is closed (and replaced) whenever either shrinks.
	//
	// reserved holds the QoS 2 publishes whose PUBREC sits in the client's
	// inflight map (reserve) and whose commit has not been seen. abandoned
	// is set by close; after it nothing is reserved or marked committed, so
	// every reservation either belongs to a committed publish or is removed.
	mu        sync.Mutex
	pending   int
	held      map[uint16]struct{}
	changed   chan struct{}
	reserved  map[uint16]*pubItem
	abandoned bool
}

// pubItem is one publish in a pipe.
type pubItem struct {
	id   uint16
	qos  byte
	code byte // non-zero: ack with this reason now, nothing to append

	msg   cluster.LogMessage
	msgID string

	// retained is the namespaced packet to keep as this node's retained
	// message once committed, nil when the publish is not retained.
	retained *packets.Packet

	rec packets.Packet // the PUBREC reserved in the inflight map (QoS 2)

	started chan struct{} // closed when the starter is done with it
	pend    *cluster.PendingAppend
	err     error
}

func newPubPipe(b *Broker, srv *mqtt.Server, cl *mqtt.Client, st *clientState) *pubPipe {
	ctx, cancel := context.WithCancel(context.Background())

	return &pubPipe{
		b: b, srv: srv, cl: cl, st: st,
		ctx: ctx, cancel: cancel,
		toStart: newFifo[*pubItem](), toAck: newFifo[*pubItem](),
		changed: make(chan struct{}),
	}
}

// close abandons what is pending and ends the goroutines: the PUBREC
// reservation of every QoS 2 publish whose commit was not seen is removed
// from the client's inflight map, so neither this connection nor one that
// inherits the session (mochi clones the inflight map at a takeover, which
// is why OnSessionEstablish closes the old connection's pipe first) ever
// sends a PUBREC for a message that may not be in the log. The client sends
// those publishes again. Idempotent.
func (p *pubPipe) close() {
	p.mu.Lock()

	if !p.abandoned {
		p.abandoned = true

		for id := range p.reserved {
			if pk, ok := p.cl.State.Inflight.Get(id); ok && pk.FixedHeader.Type == packets.Pubrec && p.cl.State.Inflight.Delete(id) {
				atomic.AddInt64(&p.srv.Info.Inflight, -1)
			}
		}

		p.reserved = nil
	}

	p.mu.Unlock()
	p.cancel()
}

// reserve puts it's PUBREC into the client's inflight map as soon as the
// PUBLISH is read, the way mochi's processPublish does, so mochi's
// NextPacketID never gives that packet ID to an outbound delivery while the
// commit runs (mochi keeps both directions in one map). The writer only
// writes it. A same-connection DUP of a QoS 2 publish still pending then
// gets mochi's own PUBREC 0x91 (packet identifier in use); conforming
// clients resend only after a reconnect. False when the pipe is abandoned.
func (p *pubPipe) reserve(it *pubItem) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.abandoned {
		return false
	}

	it.rec = packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Pubrec},
		PacketID:    it.id,
		Created:     time.Now().Unix(),
	}

	if p.cl.State.Inflight.Set(it.rec) {
		atomic.AddInt64(&p.srv.Info.Inflight, 1)
	}

	if p.reserved == nil {
		p.reserved = make(map[uint16]*pubItem)
	}

	p.reserved[it.id] = it

	return true
}

// committed records that it is in the log, so its reservation stays (a
// connection that inherits the session resends that PUBREC, rightly). False
// when the pipe was abandoned first: close already removed the reservation.
func (p *pubPipe) committed(it *pubItem) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.abandoned {
		return false
	}

	if p.reserved[it.id] == it {
		delete(p.reserved, it.id)
	}

	return true
}

// outstanding is the count against the receive maximum, and the channel
// that closes when it next shrinks. For a 3.1.1 client only the publishes
// whose ack is not written count: its QoS 2 slots free only when a PUBREL is
// read, and the read loop is the one waiting in admit.
func (p *pubPipe) outstanding() (n int, changed <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cl.Properties.ProtocolVersion < 5 {
		return p.pending, p.changed
	}

	return p.pending + len(p.held), p.changed
}

// notifyLocked wakes everyone waiting for a slot. p.mu must be held.
func (p *pubPipe) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// admit decides whether a new QoS 1 or 2 publish fits under the server's
// receive maximum (ruling R22). A 5.0 client over it is disconnected with
// 0x93; a 3.1.1 client, which has no receive maximum, waits until a slot
// frees. False means drop the publish: the client was disconnected, or its
// connection ended while it waited.
func (p *pubPipe) admit() bool {
	limit := int(p.srv.Options.Capabilities.ReceiveMaximum)
	if limit == 0 {
		limit = 65535
	}

	var tick *time.Ticker

	defer func() {
		if tick != nil {
			tick.Stop()
		}
	}()

	for {
		out, changed := p.outstanding()
		if out < limit {
			return true
		}

		if p.cl.Properties.ProtocolVersion == 5 {
			p.b.log.Info("tether: receive maximum exceeded", slog.String("client", p.cl.ID), slog.Int("outstanding", out), slog.Int("limit", limit))
			disconnect(p.srv, p.b.log, p.cl, packets.ErrReceiveMaximum)

			return false
		}

		if tick == nil {
			tick = time.NewTicker(closedPoll)
		}

		select {
		case <-changed:
		case <-p.ctx.Done():
			return false
		case <-tick.C:
			if p.cl.Closed() {
				return false
			}
		}
	}
}

// push queues it behind the connection's earlier publishes.
func (p *pubPipe) push(it *pubItem) {
	p.mu.Lock()
	p.pending++
	p.mu.Unlock()

	p.toStart.push(it)
	p.toAck.push(it)

	p.startOnce.Do(func() {
		go p.runStarter()
		go p.runWriter()
	})
}

// ackNow queues an ack with a failure reason and nothing to append, so it
// still goes out in arrival order.
func (p *pubPipe) ackNow(id uint16, qos byte, code packets.Code) {
	p.push(&pubItem{id: id, qos: qos, code: code.Code, started: make(chan struct{})})
}

// released is a PUBREL for id read from the client: its QoS 2 slot frees.
func (p *pubPipe) released(id uint16) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.held[id]; ok {
		delete(p.held, id)
		p.notifyLocked()
	}
}

// done drops one pending publish, written or abandoned.
func (p *pubPipe) done() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pending--
	p.notifyLocked()
}

func (p *pubPipe) runStarter() {
	for {
		it, ok := p.toStart.pop(p.ctx.Done())
		if !ok {
			return
		}

		if it.code == 0 {
			p.start(it)
		}

		close(it.started)
	}
}

// start sends it to the log. A QoS 2 publish's pending entry is persisted
// first (the Task 9 seam, persistQoS2), so a resend after a crash finds the
// message ID it was appended under.
func (p *pubPipe) start(it *pubItem) {
	n := p.b.cluster

	if it.qos == 2 && n.persistQoS2 != nil {
		if err := n.persistQoS2(p.st.tenantID, p.st.rawID, it.id, it.msgID); err != nil {
			it.err = err

			return
		}
	}

	ctx, cancel := context.WithTimeout(p.ctx, n.appendAttempt)
	defer cancel()

	it.pend, it.err = n.Log.AppendAsync(ctx, it.msg, it.msgID)
}

func (p *pubPipe) runWriter() {
	for {
		it, ok := p.toAck.pop(p.ctx.Done())
		if !ok {
			return
		}

		select {
		case <-it.started:
		case <-p.ctx.Done():
			return
		}

		if it.code == 0 && !p.commit(it) {
			return
		}

		if !p.ack(it) {
			return
		}
	}
}

// commit waits until it is in the log, sending it again (same message ID,
// so JetStream stores it once) after a failure or a wait that ran out, with
// backoff, for as long as the connection is open. It reports false when the
// connection ended first: the publish is abandoned with no ack.
func (p *pubPipe) commit(it *pubItem) bool {
	n := p.b.cluster
	delay := n.appendRetry

	for {
		if it.err == nil {
			ctx, cancel := context.WithTimeout(p.ctx, n.appendAttempt)
			_, _, err := it.pend.Wait(ctx)

			cancel()

			if err == nil {
				n.appends.Add(1)

				if it.retained != nil {
					retainLocal(p.srv, it.retained)
				}

				return it.qos != 2 || p.committed(it)
			}

			it.err = err
		}

		if errors.Is(it.err, tether.ErrInvalidInput) {
			// Every publish was checked before it got here, so this is a
			// bug. The client would wait for this ack for good; close it.
			n.log.Error("tether: publish refused by the log", slog.String("client", p.cl.ID), slog.Any("error", it.err))
			p.cl.Stop(packets.ErrImplementationSpecificError)

			return false
		}

		if p.ctx.Err() != nil || p.cl.Closed() {
			return false
		}

		n.log.Warn("tether: append failed, retrying", slog.String("client", p.cl.ID), slog.Uint64("packet_id", uint64(it.id)),
			slog.Duration("in", delay), slog.Any("error", it.err))

		select {
		case <-p.ctx.Done():
			return false
		case <-time.After(delay):
		}

		delay = min(2*delay, n.appendRetryMax)

		p.start(it)
	}
}

// ack writes the PUBACK or PUBREC of it. A successful PUBREC is the one
// reserve put into the client's inflight map, where mochi's own PUBREL
// handling finds it and answers PUBCOMP (spike 1, TestAsyncAckQoS2). It
// reports false when the connection has ended.
func (p *pubPipe) ack(it *pubItem) bool {
	defer p.done()

	if p.ctx.Err() != nil || p.cl.Closed() {
		return false
	}

	ack := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Puback},
		PacketID:    it.id,
		ReasonCode:  it.code,
		Created:     time.Now().Unix(),
	}

	if it.qos == 2 {
		ack.FixedHeader.Type = packets.Pubrec

		if it.code == 0 {
			ack = it.rec

			p.mu.Lock()

			if p.held == nil {
				p.held = make(map[uint16]struct{})
			}

			p.held[it.id] = struct{}{}
			p.mu.Unlock()
		}
	}

	if err := p.cl.WritePacket(ack); err != nil {
		p.b.log.Debug("tether: ack not written", slog.String("client", p.cl.ID), slog.Any("error", err))

		return false
	}

	p.b.cluster.acks.Add(1)

	return true
}

// commitPublish hands a network client's accepted QoS 1 or 2 publish to its
// pipe. The tenant is the connection's and the topic tenant-relative; the
// payload is copied. A QoS 2 publish takes its message ID from the session's
// pending entry (qos2MsgID), so a DUP resend is stored once, and reserves its
// packet ID (pubPipe.reserve). Nothing is queued once the pipe is abandoned.
func (b *Broker) commitPublish(cl *mqtt.Client, st *clientState, rel string, pk *packets.Packet) {
	n := b.cluster
	qos := pk.FixedHeader.Qos

	msgID := newMsgID()
	if qos == 2 {
		msgID = n.qos2MsgID(cl.ID, pk.PacketID, pk.FixedHeader.Dup)
	}

	it := &pubItem{
		id: pk.PacketID, qos: qos, msgID: msgID,
		msg: cluster.LogMessage{
			Tenant:   st.tenantID,
			Topic:    rel,
			Payload:  slices.Clone(pk.Payload),
			QoS:      qos,
			Retain:   pk.FixedHeader.Retain,
			Props:    clusterProps(pk.Properties),
			Origin:   n.Config.NodeID,
			ClientID: st.rawID,
		},
		started: make(chan struct{}),
	}

	if pk.FixedHeader.Retain {
		ret := pk.Copy(false)
		ret.Payload = it.msg.Payload
		it.retained = &ret
	}

	if qos == 2 && !st.pipe.reserve(it) {
		return
	}

	st.pipe.push(it)
}

// appendHost appends a host publish (Publish, DevicePublish) at QoS 1 or 2
// to the log and waits for the commit; every node, this one included,
// delivers it from its reader. ns is the namespaced name, for the local
// retained copy.
func (b *Broker) appendHost(ctx context.Context, srv *mqtt.Server, tenantID, name, ns string, payload []byte, qos byte, retain bool) error {
	n := b.cluster

	_, _, err := n.Log.Append(ctx, cluster.LogMessage{
		Tenant: tenantID, Topic: name, Payload: payload, QoS: qos, Retain: retain, Origin: n.Config.NodeID,
	}, newMsgID())
	if err != nil {
		return err
	}

	n.appends.Add(1)

	if retain {
		retainLocal(srv, &packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos, Retain: true},
			TopicName:   ns,
			Payload:     payload,
			Created:     time.Now().Unix(),
		})
	}

	return nil
}

// retainLocal keeps pk (namespaced) as this node's retained message on its
// topic, or clears it for an empty payload, as mochi does for a retained
// publish it delivers itself. Until retained messages live in the cell
// (Task 11), a retained QoS 1 or 2 message is retained on the node it was
// published on, like a QoS 0 one.
func retainLocal(srv *mqtt.Server, pk *packets.Packet) {
	if srv.Options.Capabilities.RetainAvailable == 0 {
		return
	}

	srv.Topics.RetainMessage(pk.Copy(false))
	atomic.StoreInt64(&srv.Info.Retained, int64(srv.Topics.Retained.Len()))
}

// qos2MsgID returns the message ID a QoS 2 publish of session nsID with
// packet ID id is appended under. A DUP resend takes the one its pending
// entry holds; anything else gets a fresh one and becomes the entry. The
// entry lives until the PUBREL (clearQoS2) or the session's end (dropQoS2).
//
// The entries are this node's memory for now. Task 9 persists each into
// cluster.SessionRecord.QoS2In before the append (persistQoS2) and loads
// them with the session, so a resend reaching another node finds it too.
func (n *clusterNode) qos2MsgID(nsID string, id uint16, dup bool) string {
	n.qos2Mu.Lock()
	defer n.qos2Mu.Unlock()

	set := n.qos2In[nsID]

	if dup {
		if msgID, ok := set[id]; ok {
			return msgID
		}
	}

	if set == nil {
		set = make(map[uint16]string)
		n.qos2In[nsID] = set
	}

	msgID := newMsgID()
	set[id] = msgID

	return msgID
}

// clearQoS2 drops the pending entry of nsID's packet id, on its PUBREL.
func (n *clusterNode) clearQoS2(nsID string, id uint16) {
	n.qos2Mu.Lock()
	defer n.qos2Mu.Unlock()

	set := n.qos2In[nsID]
	delete(set, id)

	if len(set) == 0 {
		delete(n.qos2In, nsID)
	}
}

// dropQoS2 drops every pending entry of a session that ended (clean start,
// expiry, deletion).
func (n *clusterNode) dropQoS2(nsID string) {
	n.qos2Mu.Lock()
	defer n.qos2Mu.Unlock()

	delete(n.qos2In, nsID)
}

// qos2Entries returns nsID's pending entries, for tests.
func (n *clusterNode) qos2Entries(nsID string) map[uint16]string {
	n.qos2Mu.Lock()
	defer n.qos2Mu.Unlock()

	out := make(map[uint16]string, len(n.qos2In[nsID]))
	for k, v := range n.qos2In[nsID] {
		out[k] = v
	}

	return out
}

// pubrel handles a PUBREL from a network client: its QoS 2 slot frees and
// the pending entry goes.
func (b *Broker) pubrel(cl *mqtt.Client, id uint16) {
	if st := b.state.get(cl); st != nil && st.pipe != nil {
		st.pipe.released(id)
	}

	b.cluster.clearQoS2(cl.ID, id)
}

// newMsgID is a fresh Nats-Msg-Id: 26 base32 characters, 128 random bits.
func newMsgID() string { return rand.Text() }

// fifo is an unbounded queue with one consumer.
type fifo[T any] struct {
	mu    sync.Mutex
	items []T
	wake  chan struct{}
}

func newFifo[T any]() fifo[T] { return fifo[T]{wake: make(chan struct{}, 1)} }

func (q *fifo[T]) push(x T) {
	q.mu.Lock()
	q.items = append(q.items, x)
	q.mu.Unlock()

	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// pop returns the oldest item, waiting for one, or false once done closes.
func (q *fifo[T]) pop(done <-chan struct{}) (T, bool) {
	for {
		q.mu.Lock()

		if len(q.items) > 0 {
			x := q.items[0]

			var zero T

			q.items[0] = zero
			q.items = q.items[1:]

			if len(q.items) == 0 {
				q.items = nil
			}

			q.mu.Unlock()

			return x, true
		}

		q.mu.Unlock()

		select {
		case <-q.wake:
		case <-done:
			var zero T

			return zero, false
		}
	}
}
