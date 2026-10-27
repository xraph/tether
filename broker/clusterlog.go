package broker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	// defaultAppendAttempt bounds the send of one append (the publisher
	// holds a send back while too many acks are outstanding) and a QoS 2
	// publish's session write before it.
	defaultAppendAttempt = 10 * time.Second
	// defaultAppendLate is how long the writer waits for the commit of an
	// append before it takes the ack as lost and sends the append again,
	// with the same message ID, so it is stored once. A stream leader that
	// dies takes the acks of the appends it had with it, and those never
	// come; a commit normally takes milliseconds.
	defaultAppendLate = time.Second
	// defaultAppendRetry and defaultAppendRetryMax pace the retries of a
	// failed append while the log is down, doubling, while the connection is
	// open (ruling R23). The pace is per connection and starts over once an
	// append commits; any commit on the node cuts a wait short.
	defaultAppendRetry    = 100 * time.Millisecond
	defaultAppendRetryMax = time.Second
	// defaultLogProbe is how often a node whose pipes wait for an
	// unreachable log asks the log stream whether it has a leader again
	// (cluster.Log.Ping), and twice that bounds one ask.
	defaultLogProbe = 200 * time.Millisecond
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
		// The tenant's live position starts where the reader does, before
		// any CONNECT of the tenant reads it.
		n.advanceLive(tenantID, r.Position())
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

	// The tenants' retained caches follow the same rule.
	n.sweepRetained(live, now)
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
// for No Local. deliverLog (cursor.go) does the work, under the tenant's
// delivery lock, and keeps the sessions' cursors.
func (b *Broker) logDeliver(tenantID string) func(cluster.LogMessage) error {
	return func(m cluster.LogMessage) error { return b.deliverLog(tenantID, m) }
}

// pubPipe carries one connection's QoS 1 and 2 publishes to the log and
// writes their acks. Two goroutines, started with the first such publish:
// the starter sends each to the log in arrival order (so one client's
// messages are committed in the order it sent them), the writer waits for
// each commit in that same order and writes the ack, retrying a failed
// append while the connection is open. When the connection stops owning its
// client ID (close, from the registry's closePipe: bind, remove,
// sweepClosed) whatever is pending is abandoned without an ack; the client
// sends it again when it reconnects.
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

	// delay is the wait before the next retry while the log is down; 0 when
	// the last append went through. late is how long the writer waits for a
	// commit before it takes the ack as lost: appendLate, doubled after each
	// late ack up to appendAttempt, and halved back towards appendLate after
	// a commit that came within appendLate, so a log that commits slower
	// than appendLate is waited out instead of flooded with resends. Both
	// are the writer's own.
	delay time.Duration
	late  time.Duration

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

	// retained is the write to the cell's retained store that follows the
	// commit, nil when the publish is not retained.
	retained *retainWrite

	rec packets.Packet // the PUBREC reserved in the inflight map (QoS 2)

	started chan struct{} // closed when the starter is done with it
	pend    *cluster.PendingAppend
	err     error

	// sentAt is when the latest attempt was started; persisted says the
	// QoS 2 pending entry is in the session's record, so a retry does not
	// write it again. Set by the starter before started closes, then by the
	// writer only.
	sentAt    time.Time
	persisted bool

	// refused, set before the starter sees it, fails every attempt
	// without sending: a retained publish the retained store cannot hold
	// (cluster.Retained.Check).
	refused error
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
// is why bind closes the old connection's pipe first) ever
// sends a PUBREC for a message that may not be in the log. The client sends
// those publishes again. The registry calls it (clientState.closePipe)
// before the state stops owning its client ID. Idempotent.
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

	n.appendAttempts.Add(1)

	it.sentAt = time.Now()
	it.pend = nil

	if it.refused != nil {
		it.err = it.refused

		return
	}

	if it.qos == 2 && n.persistQoS2 != nil && !it.persisted {
		if err := n.persistQoS2(p.st.tenantID, p.st.rawID, it.id, it.msgID); err != nil {
			it.err = err

			return
		}

		it.persisted = true
	}

	if hold := n.holdAppend; hold != nil {
		if err := hold(p.ctx); err != nil {
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
// so JetStream stores it once) after a failure or an ack that is late, for as
// long as the connection is open. It reports false when the connection ended
// first: the publish is abandoned with no ack.
//
// Recovery after an outage must be prompt (chaos finding CHAOS-1): every
// publish that was in flight while the log was down has a failed attempt,
// and retrying them one at a time behind a backoff acked about ten a second
// for minutes. So the wait before a retry ends as soon as the log is seen
// back (pace), and once a retried publish commits, the publishes queued
// behind it whose attempt failed, or whose ack was lost when the head
// committed promptly, are sent again at once (resendQueued), so they commit
// together and their acks follow in order.
func (p *pubPipe) commit(it *pubItem) bool {
	n := p.b.cluster
	retried := false

	for {
		if it.err == nil {
			err := p.wait(it)
			if err == nil {
				n.appends.Add(1)
				n.logUp.up()

				p.delay = 0

				if retried {
					p.resendQueued(time.Since(it.sentAt))
				}

				if it.qos == 2 && !p.committed(it) {
					return false
				}

				return it.retained == nil || p.retain(it)
			}

			it.err = err
		}

		if errors.Is(it.err, cluster.ErrTooLarge) {
			// Over what NATS carries (fitPacketSize keeps that rare): it
			// never commits, so it is refused, not retried.
			return p.refuseTooLarge(it)
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

		if !p.pace(it) {
			return false
		}

		retried = true

		p.start(it)
	}
}

// errTooLargeCode is the reason code of a PUBACK or PUBREC refusing a
// publish the cell cannot carry. MQTT 5 has no "packet too large" code for
// them (0x95 is for CONNACK and DISCONNECT), so it is 0x83, implementation
// specific error: the PUBLISH is valid but the server will not take it.
var errTooLargeCode = packets.Code{Code: packets.ErrImplementationSpecificError.Code, Reason: "message too large for the cell"}

// refuseTooLarge ends it's commit on a publish the log refused as too large
// (cluster.ErrTooLarge). A 5.0 client gets its PUBACK or PUBREC with
// errTooLargeCode, which ends a QoS 2 flow (its pending entry goes); a 3.1.1
// client has no failure code to read, so it is disconnected with 0x95. It
// reports whether the pipe goes on.
func (p *pubPipe) refuseTooLarge(it *pubItem) bool {
	p.b.log.Warn("tether: publish too large for the cell, refused", slog.String("client", p.cl.ID),
		slog.Int("payload_bytes", len(it.msg.Payload)), slog.Any("error", it.err))

	if p.cl.Properties.ProtocolVersion < 5 {
		p.cl.Stop(packets.ErrPacketTooLarge)

		return false
	}

	it.code = errTooLargeCode.Code
	it.retained = nil

	if it.qos == 2 {
		p.unreserve(it)
		p.b.cluster.clearQoS2(p.cl.ID, it.id)
	}

	return true
}

// unreserve removes the PUBREC reservation of a QoS 2 publish that is
// refused rather than committed: nothing of its flow may be resent.
func (p *pubPipe) unreserve(it *pubItem) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.reserved[it.id] != it {
		return
	}

	delete(p.reserved, it.id)

	if held, ok := p.cl.State.Inflight.Get(it.id); ok && held.FixedHeader.Type == packets.Pubrec &&
		p.cl.State.Inflight.Delete(it.id) {
		atomic.AddInt64(&p.srv.Info.Inflight, -1)
	}
}

// errAckLate is a commit wait that ran past the pipe's late bound.
var errAckLate = fmt.Errorf("tether/broker: append ack is late: %w", cluster.ErrUnavailable)

// wait waits for the commit of it's latest attempt, for the pipe's late
// bound (appendLate at first). An ack that has not come by then is taken as
// lost (errAckLate) and the append is sent again, and the bound doubles for
// the publishes after it too, up to appendAttempt: a leader that died took
// the ack with it, or the log is slower than the bound, and in that case
// every later publish would be late as well. A commit that came within
// appendLate halves it back.
func (p *pubPipe) wait(it *pubItem) error {
	n := p.b.cluster

	if p.late == 0 {
		p.late = n.appendLate
	}

	// A commit that was already in when the wait began says nothing about
	// how long commits take.
	before, earlyErr := it.pend.Settled()
	if before && earlyErr != nil {
		return earlyErr
	}

	ctx, cancel := context.WithTimeout(p.ctx, p.late)
	defer cancel()

	// An attempt sent while the log is down most likely went nowhere (a
	// stream member without the leadership drops it, with no answer). When
	// the node sees the log back, stop waiting for it and send it again,
	// rather than wait out a late bound that grew during the outage.
	back := n.logUp.whileDown()
	if back != nil {
		stop := make(chan struct{})
		defer close(stop)

		go func() {
			select {
			case <-back:
				cancel()
			case <-stop:
			}
		}()
	}

	_, _, err := it.pend.Wait(ctx)

	switch {
	case err == nil:
		if !before && time.Since(it.sentAt) <= n.appendLate {
			p.late = max(n.appendLate, p.late/2)
		}
	case p.ctx.Err() == nil && isClosed(back):
		return fmt.Errorf("%w: the log came back while waiting: %w", errAckLate, err)
	case p.ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded):
		p.late = min(2*p.late, max(n.appendAttempt, n.appendLate))

		return fmt.Errorf("%w: %w", errAckLate, err)
	}

	return err
}

// pace waits before it is sent again, and reports false when the connection
// ended first. The wait is the pipe's backoff, appendRetry doubling to
// appendRetryMax, starting over once the pipe commits something. When the
// failure was the log being unreachable (an error wrapping
// cluster.ErrUnavailable, a late ack included) the wait also ends as soon as
// the node sees the log back (logHealth), and there is none at all when it
// came back after the failed attempt went out: that attempt failed during
// the outage, which is over.
func (p *pubPipe) pace(it *pubItem) bool {
	n := p.b.cluster
	unreachable := errors.Is(it.err, cluster.ErrUnavailable)

	if unreachable && n.logUp.cameBackSince(it.sentAt) {
		n.log.Debug("tether: append failed while the log was down, sending it again", slog.String("client", p.cl.ID),
			slog.Uint64("packet_id", uint64(it.id)), slog.Any("error", it.err))

		return true
	}

	if p.delay == 0 {
		p.delay = n.appendRetry
	}

	n.log.Warn("tether: append failed, retrying", slog.String("client", p.cl.ID), slog.Uint64("packet_id", uint64(it.id)),
		slog.Duration("in", p.delay), slog.Any("error", it.err))

	var back <-chan struct{}

	if unreachable {
		var release func()

		back, release = n.logDown()
		defer release()
	}

	timer := time.NewTimer(p.delay)
	defer timer.Stop()

	p.delay = min(2*p.delay, n.appendRetryMax)

	select {
	case <-p.ctx.Done():
		return false
	case <-back:
	case <-timer.C:
	}

	return true
}

// lostAckFloor is the least time an attempt must have gone unanswered,
// after a retried publish committed promptly, before resendQueued takes its
// ack as lost.
const lostAckFloor = 50 * time.Millisecond

// resendQueued runs when a publish that had to be retried has committed, in
// headLatency from its last send. It sends again, in order and without
// waiting, the started publishes queued behind it whose latest attempt
// failed, and, when the head committed within appendLate (the log answers
// promptly again), those still unanswered after more than twice that
// latency (at least lostAckFloor): the leader they went to died with their
// acks, and a client that sends nothing more would otherwise wait out a late
// bound for each of them in turn. When the head was slow the log is slow,
// and an unanswered publish is most likely just in flight: only failures go
// again, so a slow log is not flooded. Same message IDs, so whatever did
// commit is stored once. A publish the starter has not reached yet goes out
// fresh from there.
func (p *pubPipe) resendQueued(headLatency time.Duration) {
	n := p.b.cluster
	prompt := headLatency <= n.appendLate
	lostAfter := max(2*headLatency, lostAckFloor)
	sent := 0

	for _, x := range p.toAck.snapshot() {
		// The starter goes in order: nothing after this one has started.
		if !isClosed(x.started) {
			break
		}

		if x.code != 0 {
			continue
		}

		resend := x.err != nil

		if !resend {
			done, err := x.pend.Settled()
			resend = (done && err != nil) || (!done && prompt && time.Since(x.sentAt) > lostAfter)
		}

		if !resend {
			continue
		}

		if p.ctx.Err() != nil {
			return
		}

		p.start(x)

		sent++
	}

	if sent > 0 {
		n.log.Info("tether: the log is back, sending the queued publishes that failed or lost their ack again",
			slog.String("client", p.cl.ID), slog.Int("publishes", sent))
	}
}

// isClosed reports whether ch is closed, without waiting.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// logHealth is what a node's pipes know about the log. A pipe whose append
// failed because the log could not be reached marks it down (logDown) and
// waits for back. While any pipe waits, one prober asks the log stream for a
// leader every logProbe; when one answers, or an append commits, the log is
// up again and back closes, so every waiting pipe retries at once instead of
// at the end of its backoff. Its zero value is ready.
type logHealth struct {
	mu      sync.Mutex
	down    bool          // found unreachable and not seen back since
	back    chan struct{} // closed when it comes back; set while down
	upAt    time.Time     // when it last came back
	waiting int           // pipes waiting on back
	probing bool          // the prober runs
}

// up records that the log took an append or answered the prober.
func (h *logHealth) up() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.down {
		h.down = false
		h.upAt = time.Now()
		close(h.back)
		h.back = nil
	}
}

// whileDown returns the channel that closes when the log comes back, or nil
// when it is not down.
func (h *logHealth) whileDown() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.back
}

// cameBackSince reports whether the log came back after t.
func (h *logHealth) cameBackSince(t time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.upAt.After(t)
}

// logDown records that an append failed because the log could not be
// reached, starts the prober when none runs, and returns the channel that
// closes when the log is back and the func to call when done waiting.
func (n *clusterNode) logDown() (back <-chan struct{}, release func()) {
	h := &n.logUp

	h.mu.Lock()

	if !h.down {
		h.down = true
		h.back = make(chan struct{})
	}

	h.waiting++
	back = h.back
	start := !h.probing
	h.probing = true

	h.mu.Unlock()

	if start {
		go n.probeLog()
	}

	return back, func() {
		h.mu.Lock()
		h.waiting--
		h.mu.Unlock()
	}
}

// probeLog asks the log stream for a leader every logProbe, each ask bounded
// by twice that, until the log is up or no pipe waits for it. It waits
// before its first ask too, so a pipe that fails for a reason the probe
// cannot see retries at most about once per logProbe.
func (n *clusterNode) probeLog() {
	h := &n.logUp

	for {
		time.Sleep(n.logProbe)

		h.mu.Lock()

		if !h.down || h.waiting == 0 {
			h.probing = false
			h.mu.Unlock()

			return
		}

		h.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 2*n.logProbe)
		err := n.Log.Ping(ctx)

		cancel()

		if err == nil {
			h.up()
		}
	}
}

// retain writes the retained message of a committed publish to the cell's
// store before its ack, so a PUBACK or PUBREC says the message is in the log
// and retained in the cell (clusterretained.go). The write goes through its
// topic's shard, whose writer retries for the retry window; this waits while
// the connection is open. When the connection ends first the write goes on
// (the message is in the log, and a QoS 2 client will not send it again) and
// it reports false: no ack. When the writer gives up first the publish is
// acked anyway (in an outage that long the node has fenced itself and closed
// the connection).
func (p *pubPipe) retain(it *pubItem) bool {
	err := p.b.cluster.storeRetained(p.ctx, it.retained, true)
	if err == nil {
		return true
	}

	return p.ctx.Err() == nil && !p.cl.Closed()
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
// packet ID (pubPipe.reserve); a QoS 2 publish read after the pipe was
// abandoned is neither reserved nor queued. A QoS 1 one is queued and never
// acked, since the abandoned pipe's goroutines have ended.
func (b *Broker) commitPublish(cl *mqtt.Client, st *clientState, rel string, pk *packets.Packet) {
	n := b.cluster
	qos := pk.FixedHeader.Qos

	msgID := newMsgID()
	if qos == 2 {
		msgID = n.qos2MsgID(cl.ID, pk.PacketID, pk.FixedHeader.Dup, qos2Digest(rel, qos, pk.Payload))
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
		if err := n.Retained.Check(it.msg); err != nil {
			// The log could take it but the retained store could not: it
			// is refused whole (start, refuseTooLarge), so a client is never
			// told a message is retained when it is not.
			it.refused = err
		} else {
			it.retained = n.newRetainWrite(st.tenantID, rel, it.msg.Payload, qos, it.msg.Props, st.rawID)
		}
	}

	if qos == 2 {
		// The reservation shares the inflight map with the session's
		// deliveries and is made outside the tenant's delivery lock: if it
		// fills the map, a message injected right after may be dropped, so
		// the cursor is held at the live position read before (cursor.go).
		s := n.sessionOf(cl)

		var lp uint64
		if s != nil && s.tracked {
			lp = s.tt.livePos.Load()
		}

		if !st.pipe.reserve(it) {
			return
		}

		if s != nil && s.tracked {
			n.pinIfFull(s, cl, lp)
		}
	}

	st.pipe.push(it)
}

// appendHost appends a host publish (Publish, DevicePublish) at QoS 1 or 2
// to the log and waits for the commit; every node, this one included,
// delivers it from its reader. A retained one is then written to the cell's
// retained store (clusterretained.go) under the same ctx; its error is
// returned, though the message is in the log by then.
func (b *Broker) appendHost(ctx context.Context, tenantID, name string, payload []byte, qos byte, retain bool) error {
	n := b.cluster

	_, _, err := n.Log.Append(ctx, cluster.LogMessage{
		Tenant: tenantID, Topic: name, Payload: payload, QoS: qos, Retain: retain, Origin: n.Config.NodeID,
	}, newMsgID())
	if err != nil {
		return err
	}

	n.appends.Add(1)

	if retain {
		if err := n.storeRetained(ctx, n.newRetainWrite(tenantID, name, payload, qos, cluster.Props{}, ""), false); err != nil {
			return fmt.Errorf("in the log, not retained: %w", err)
		}
	}

	return nil
}

// qos2MsgID returns the message ID a QoS 2 publish of session nsID with
// packet ID id is appended under. A DUP resend takes the one its pending
// entry holds when the entry was made for the same message; anything else
// gets a fresh one and becomes the entry. The entry lives until the PUBREL
// (clearQoS2) or the session's end (dropQoS2).
//
// The entries are this node's memory of the session's record: each is
// written into cluster.SessionRecord.QoS2In before the append (persistQoS2,
// for a session that has a record to resume) and loaded with the session, so
// a resend reaching another node finds it too.
//
// A message ID ends in digest, the message's qos2Digest, so the entry says
// which message it was made for. A PUBREL clears the entry in memory only
// until the next record write, so a node that resumes the session can load
// an entry of a message already completed; the client may have reused the
// packet ID for a new message by then [MQTT-2.2.1-3]. Reusing the old ID for
// the new message would make JetStream discard it as a duplicate after it
// was acknowledged. A fresh ID is safe: the entry is persisted before the
// append.
func (n *clusterNode) qos2MsgID(nsID string, id uint16, dup bool, digest string) string {
	n.qos2Mu.Lock()
	defer n.qos2Mu.Unlock()

	set := n.qos2In[nsID]

	if dup {
		if msgID, ok := set[id]; ok && strings.HasSuffix(msgID, "."+digest) {
			return msgID
		}
	}

	if set == nil {
		set = make(map[uint16]string)
		n.qos2In[nsID] = set
	}

	msgID := newMsgID() + "." + digest
	set[id] = msgID

	return msgID
}

// qos2Digest identifies a QoS 2 publish by its topic, QoS and payload, for
// qos2MsgID: 16 hex digits of their SHA-256.
func qos2Digest(topic string, qos byte, payload []byte) string {
	h := sha256.New()

	var n [9]byte

	binary.BigEndian.PutUint64(n[:8], uint64(len(topic)))
	n[8] = qos

	h.Write(n[:])
	h.Write([]byte(topic))
	h.Write(payload)

	return hex.EncodeToString(h.Sum(nil)[:8])
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

	if s := b.cluster.sessionOf(cl); s != nil && s.tracked {
		s.touch()
	}
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

// snapshot returns the queued items, oldest first.
func (q *fifo[T]) snapshot() []T {
	q.mu.Lock()
	defer q.mu.Unlock()

	return slices.Clone(q.items)
}

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
