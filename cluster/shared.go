package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether/subject"
	mqtttopic "github.com/xraph/tether/topic"
)

// Shared subscriptions across the cell (spec "Shared subscriptions").
//
// A group, "$share/<group>/<filter>" of one tenant, has two halves, because
// a member receives at min(publish QoS, granted QoS) and a group receives
// both kinds of publish:
//
//   - QoS 1 and 2: one durable pull consumer on the log per (tenant, group,
//     filter), shared by the whole cell. Every node with members of the group
//     pulls from it and hands each message to one of its own members; the
//     message is acked once that member is through with it, so a member or a
//     node that is lost leaves its messages to be redelivered (ruling R34).
//   - QoS 0: a NATS queue group on the filter's QoS 0 subjects, on the
//     connection without NoEcho (ruling R5), so the publishing node's own
//     members are in the pick.

const (
	// sharedPrefix starts every consumer and queue group name.
	sharedPrefix = "tether_sh_"
	// defaultSharedAckWait is how long a message handed to a node stays
	// unacked before JetStream gives it to another puller. A node keeps the
	// messages its members hold alive with InProgress every third of it, so
	// only a lost node waits this long.
	defaultSharedAckWait = 30 * time.Second
	// sharedMaxAckPending bounds a group's unacked messages across the cell.
	sharedMaxAckPending = 1024
	// sharedPullBatch is how many messages a node asks for at a time. Small,
	// so one node does not hoard a burst the other nodes' members could take.
	sharedPullBatch = 16
	// sharedBusyDelay is how long a message waits before redelivery when
	// every member on the node that pulled it was busy.
	sharedBusyDelay = 250 * time.Millisecond
	// sharedJoinWait bounds the NATS work of a first Join on a node.
	sharedJoinWait = 5 * time.Second
	// sharedRetryMax caps the backoff of a pull loop that lost its consumer.
	sharedRetryMax = 5 * time.Second
	// defaultSharedFetchWait is how long one fetch waits for messages. It
	// bounds how long a stopped pull loop goes on reading its last fetch.
	defaultSharedFetchWait = 2 * time.Second
)

// ErrMemberBusy is what a member's deliver function returns when it cannot
// take a message now but may later (its window or queue is full, its
// connection is going away). The group tries its other members on the node,
// and a QoS 1 or 2 message nobody on the node could take is redelivered after
// a short delay, possibly to another node.
var ErrMemberBusy = errors.New("tether/cluster: shared subscription member is busy")

// errSharedClosed is returned by Join after Close.
var errSharedClosed = fmt.Errorf("tether/cluster: shared subscriptions are closed: %w", ErrUnavailable)

// SharedDeliver hands one message to one member of a group. It returns nil
// once the member has taken it, and then calls done exactly once: nil when
// the member is through with it (for QoS 1 its PUBACK, for QoS 2 its PUBCOMP,
// for a QoS 0 delivery the send), an error when the member was lost before
// that, which hands the message to another member. A non-nil return means the
// member did not take it, and done must not be called. done may be called
// before deliver returns, from any goroutine. QoS 0 messages (msg.QoS 0) have
// nothing to ack and ignore done.
type SharedDeliver func(msg LogMessage, done func(error)) error

type sharedKey struct{ tenant, group, filter string }

// Shared runs this node's side of the cell's shared subscription groups. It
// is safe for concurrent use.
type Shared struct {
	c      *Conn
	log    *Log
	logger *slog.Logger
	dedup  *dedup

	mu     sync.Mutex // guards groups, closed and every group's pins
	groups map[sharedKey]*sharedGroup
	closed bool

	// pend holds the messages handed to members and not yet done, which the
	// ticker keeps alive with InProgress.
	pmu      sync.Mutex
	pend     map[*sharedPending]struct{}
	ticking  bool
	tickStop chan struct{}
	tickDone chan struct{}

	// ackWait is the consumers' AckWait, fetchWait how long one fetch waits,
	// heartbeat the fetch heartbeat (0: none; the fetch wait bounds a dead
	// one). Tests change them before the first Join.
	ackWait   time.Duration
	fetchWait time.Duration
	heartbeat time.Duration

	// quit closes on Close: QoS 2 acks still being retried (ackPinned)
	// stop.
	quit chan struct{}

	// wrapMsg, when a test sets it before the first Join, wraps every
	// message a fetch brings (to lose its acks, say).
	wrapMsg func(jetstream.Msg) jetstream.Msg

	// Counters for tests: messages handed to a member (both halves), log
	// messages acked after a member was through, naked for redelivery,
	// acked at once because they did not match, QoS 0 messages no member on
	// the node took, consumers made again by a pull loop.
	delivered, acked, naked, skipped, dropped, recreated atomic.Uint64

	// ackRetries counts QoS 2 acks sent again because JetStream did not
	// confirm one (ackPinned).
	ackRetries atomic.Uint64
}

// NewShared returns this node's shared subscriptions over c, on the log l
// reads (the log stream must exist: Provision first). cfg names the node.
func NewShared(c *Conn, cfg Config, l *Log) *Shared {
	return &Shared{
		c:         c,
		log:       l,
		logger:    c.logger.With("shared", cfg.NodeID),
		dedup:     newDedup(dedupMaxEntries, dedupWindow),
		groups:    make(map[sharedKey]*sharedGroup),
		pend:      make(map[*sharedPending]struct{}),
		ackWait:   defaultSharedAckWait,
		fetchWait: defaultSharedFetchWait,
		quit:      make(chan struct{}),
	}
}

// sharedGroup is one group with members on this node. mu serialises its
// first Join and last leave, so the NATS work for one group never overlaps;
// it is never held while Shared.mu is wanted. pins is guarded by Shared.mu.
type sharedGroup struct {
	s    *Shared
	key  sharedKey
	name string // consumer and queue group name

	mu    sync.Mutex
	refs  int
	dead  bool
	qsubs []*nats.Subscription
	pull  *sharedPull

	pins int

	mmu     sync.Mutex
	members []*sharedMember
	next    int
}

type sharedMember struct {
	deliver SharedDeliver
}

// sharedName returns the name of the durable consumer and of the QoS 0 queue
// group of (tenant, group, filter): a fixed prefix and a hash, so any group
// and filter give a valid, distinct name.
func sharedName(tenant, group, filter string) string {
	h := sha256.Sum256([]byte(tenant + "\x00" + group + "\x00" + filter))

	return sharedPrefix + hex.EncodeToString(h[:20])
}

// checkShared validates a Join.
func checkShared(tenant, group, filter string) error {
	if tenant == "" {
		return invalidInput("empty tenant")
	}

	if group == "" || strings.ContainsAny(group, "/+#\x00") {
		return invalidInput("share group %q is not a single level without wildcards", group)
	}

	if err := subject.CheckTopic(filter); err != nil {
		return fmt.Errorf("tether/cluster: filter: %w", err)
	}

	if err := subject.CheckTopic(group); err != nil {
		return fmt.Errorf("tether/cluster: share group: %w", err)
	}

	if err := mqtttopic.Valid(filter, true, subject.MaxTopicBytes); err != nil {
		return fmt.Errorf("tether/cluster: filter %q: %w", filter, err)
	}

	if first, _, _ := strings.Cut(filter, "/"); strings.EqualFold(first, "$share") {
		return invalidInput("shared filter %q nests $share", filter)
	}

	return nil
}

// Join adds a member to the group (tenant, group, filter), the shared
// subscription "$share/<group>/<filter>" of tenant, on this node, and returns
// the function that takes it out again (idempotent).
//
// The first member on the node starts the node's part of the group: it
// subscribes the filter's QoS 0 subjects in the group's queue group, creates
// or opens the group's durable consumer on the log (one per group in the
// cell, created by whichever node needs it first) and starts pulling from it.
// The last leave stops both; the consumer stays on the server, keeping the
// group's place in the log for its members on other nodes and for persistent
// sessions that come back, until the log's MaxAge passes with nobody pulling.
//
// Each message goes to one member on the node, picked in turn (deliver's
// contract is under SharedDeliver). Messages of the consumer that do not
// match filter with MQTT rules are acked at once. deliver runs on the
// group's pull goroutine for QoS 1 and 2 (one message at a time, in the
// order the node received them) and on NATS dispatch goroutines for QoS 0;
// keep it short.
//
// The filter goes through subject.CheckTopic and topic.Valid, the group must
// be one level without wildcards, and a "$share" filter is refused; these
// wrap tether.ErrInvalidInput. NATS failures wrap ErrUnavailable when NATS is
// unreachable, and after Close Join fails with ErrUnavailable.
func (s *Shared) Join(tenant, group, filter string, deliver SharedDeliver) (leave func(), err error) {
	if cerr := checkShared(tenant, group, filter); cerr != nil {
		return nil, cerr
	}

	if deliver == nil {
		return nil, invalidInput("nil deliver")
	}

	key := sharedKey{tenant, group, filter}

	g, err := s.pin(key)
	if err != nil {
		return nil, err
	}
	defer s.unpin(key, g)

	if g.dead {
		return nil, errSharedClosed
	}

	if g.refs == 0 {
		if err := g.start(); err != nil {
			return nil, err
		}
	}

	g.refs++

	m := &sharedMember{deliver: deliver}

	g.mmu.Lock()
	g.members = append(g.members, m)
	g.mmu.Unlock()

	var once sync.Once

	return func() { once.Do(func() { s.leave(key, m) }) }, nil
}

// leave takes m out of its group and stops the node's part of the group when
// it was the last member.
func (s *Shared) leave(key sharedKey, m *sharedMember) {
	g, err := s.pin(key)
	if err != nil {
		return // closed: everything stopped already
	}
	defer s.unpin(key, g)

	g.mmu.Lock()

	found := false

	for i, x := range g.members {
		if x == m {
			g.members = append(g.members[:i], g.members[i+1:]...)
			found = true

			break
		}
	}
	g.mmu.Unlock()

	if !found || g.dead || g.refs == 0 {
		return
	}

	g.refs--
	if g.refs == 0 {
		g.stop()
	}
}

// pin returns the group of key, creating it, and takes its lock. Every pin is
// paired with an unpin. The lock order is sharedGroup.mu, then Shared.mu, as
// for the Router's routes.
func (s *Shared) pin(key sharedKey) (*sharedGroup, error) {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()

		return nil, errSharedClosed
	}

	g := s.groups[key]
	if g == nil {
		g = &sharedGroup{s: s, key: key, name: sharedName(key.tenant, key.group, key.filter)}
		s.groups[key] = g
	}

	g.pins++
	s.mu.Unlock()

	g.mu.Lock()

	return g, nil
}

// unpin releases g and forgets it when nobody holds or waits for it and it has
// no members.
func (s *Shared) unpin(key sharedKey, g *sharedGroup) {
	s.mu.Lock()

	g.pins--
	if g.pins == 0 && g.refs == 0 && s.groups[key] == g {
		delete(s.groups, key)
	}

	s.mu.Unlock()
	g.mu.Unlock()
}

// start subscribes the group's QoS 0 queue group and starts pulling from its
// consumer. g.mu is held. On an error nothing is left behind.
func (g *sharedGroup) start() error {
	s := g.s

	qnc, err := s.c.queueConn()
	if err != nil {
		return err
	}

	subjects := subject.QoS0Filters(g.key.tenant, g.key.filter)
	if len(subjects) == 0 {
		return invalidInput("no subjects for filter %q", g.key.filter)
	}

	subs := make([]*nats.Subscription, 0, len(subjects))

	undo := func() {
		for _, sub := range subs {
			if uerr := sub.Unsubscribe(); uerr != nil {
				s.logger.Debug("shared: undoing a queue subscribe failed", "subject", sub.Subject, "err", uerr)
			}
		}
	}

	for _, subj := range subjects {
		sub, serr := qnc.QueueSubscribe(subj, g.name, g.onQoS0)
		if serr != nil {
			undo()

			return wrapNATS(fmt.Sprintf("queue subscribe %q", subj), serr)
		}

		subs = append(subs, sub)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sharedJoinWait)
	defer cancel()

	// The node's own server has the queue interest when Join returns, so a
	// publish of this node right after reaches the group.
	if err = qnc.FlushWithContext(ctx); err != nil {
		undo()

		return wrapNATS("queue subscribe flush", err)
	}

	cons, err := g.consumer(ctx)
	if err != nil {
		undo()

		return err
	}

	g.qsubs = subs
	g.pull = newSharedPull(g, cons)

	go g.pull.run()

	s.startTicker()

	return nil
}

// stop ends the node's part of the group: the queue subscriptions go and the
// pull loop stops. g.mu is held. It makes no call that waits on NATS.
func (g *sharedGroup) stop() {
	for _, sub := range g.qsubs {
		if err := sub.Unsubscribe(); err != nil &&
			!errors.Is(err, nats.ErrBadSubscription) && !errors.Is(err, nats.ErrConnectionClosed) {
			g.s.logger.Debug("shared: queue unsubscribe failed", "group", g.name, "err", err)
		}
	}

	g.qsubs = nil

	if g.pull != nil {
		g.pull.stop()
		g.pull = nil
	}
}

// consumer creates the group's durable consumer, or opens it with the same
// configuration when another node made it first.
func (g *sharedGroup) consumer(ctx context.Context) (jetstream.Consumer, error) {
	s := g.s

	st, err := s.log.stream(ctx)
	if err != nil {
		return nil, err
	}

	var inactive time.Duration
	if info := st.CachedInfo(); info != nil {
		inactive = info.Config.MaxAge
	}

	cons, err := st.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:           g.name,
		FilterSubject:     subject.LogFilter(g.key.tenant, g.key.filter),
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           s.ackWait,
		MaxAckPending:     sharedMaxAckPending,
		MaxDeliver:        -1,
		InactiveThreshold: inactive,
	})
	if err != nil {
		return nil, s.log.wrapErr("shared consumer", err)
	}

	return cons, nil
}

// offer hands m to one member of the group on this node, starting with the
// next in turn. It reports whether one took it, and when none did, whether
// any of them was busy (rather than refusing it for good).
func (g *sharedGroup) offer(m LogMessage, done func(error)) (taken, busy bool, members int) {
	g.mmu.Lock()
	ms := append([]*sharedMember(nil), g.members...)
	start := g.next
	g.next++
	g.mmu.Unlock()

	for i := range ms {
		mb := ms[(start+i)%len(ms)]

		err := mb.deliver(m, done)
		if err == nil {
			g.s.delivered.Add(1)

			return true, false, len(ms)
		}

		if errors.Is(err, ErrMemberBusy) {
			busy = true
		}
	}

	return false, busy, len(ms)
}

// onQoS0 is the handler of the group's queue subscriptions: NATS picked this
// node for a QoS 0 message, and one local member gets it. A message that
// arrived twice (two overlapping subjects) is dropped by its Tether-Id, as
// in the Router.
func (g *sharedGroup) onQoS0(m *nats.Msg) {
	s := g.s

	tenant, name, err := subject.DecodeQoS0(m.Subject)
	if err != nil || tenant != g.key.tenant || !mqtttopic.Match(g.key.filter, name) {
		s.logger.Warn("shared: dropped a qos 0 message that is not the group's", "subject", m.Subject, "group", g.name)

		return
	}

	if id := m.Header.Get(hdrID); id != "" && s.dedup.duplicate(g.name+"|"+id) {
		return
	}

	var props Props
	if err := props.decode(m.Header); err != nil {
		s.logger.Warn("shared: properties did not decode, delivering without them", "subject", m.Subject, "err", err)

		props = Props{}
	}

	msg := LogMessage{
		Tenant:  tenant,
		Topic:   name,
		Payload: m.Data,
		Retain:  m.Header.Get(hdrRetain) == "1",
		Props:   props,
	}

	if taken, _, _ := g.offer(msg, func(error) {}); !taken {
		s.dropped.Add(1)
	}
}

// sharedPull is one run of a node's pull loop on a group's consumer.
//
// It pulls with one Fetch at a time (sharedPullBatch messages, sharedFetchWait
// at most) and reads each fetch to its end, also after stop. A continuous
// pull iterator would leave its pull request on the server when it stops, and
// a message the server sent to the abandoned inbox would wait the whole ack
// wait for redelivery; a fetch read to its end hands every message it got to
// handle, which naks it once the loop is stopped.
type sharedPull struct {
	g    *sharedGroup
	cons jetstream.Consumer

	stopped atomic.Bool
	stopCh  chan struct{}
	once    sync.Once
}

func newSharedPull(g *sharedGroup, cons jetstream.Consumer) *sharedPull {
	return &sharedPull{g: g, cons: cons, stopCh: make(chan struct{})}
}

// stop ends the loop. It does not wait: the fetch in progress runs to its
// end (sharedFetchWait at most) on the loop's goroutine, and what it still
// brings is naked for another member. So stop is prompt while NATS is
// unreachable.
func (p *sharedPull) stop() {
	p.once.Do(func() {
		p.stopped.Store(true)
		close(p.stopCh)
	})
}

// run fetches until stopped. A consumer that went away (deleted after a long
// time with nobody pulling, say) is made again, with backoff, and so is
// every other failure retried.
func (p *sharedPull) run() {
	g := p.g
	delay := consumerRetry

	for !p.stopped.Load() {
		err := p.fetch()
		if err == nil {
			delay = consumerRetry

			continue
		}

		if p.stopped.Load() {
			return
		}

		g.s.logger.Warn("shared: pull failed", "group", g.name, "err", err)

		select {
		case <-p.stopCh:
			return
		case <-time.After(delay):
		}

		delay = min(2*delay, sharedRetryMax)

		if consumerGone(err) {
			ctx, cancel := context.WithTimeout(context.Background(), sharedJoinWait)
			cons, cerr := g.consumer(ctx)

			cancel()

			if cerr != nil {
				g.s.logger.Warn("shared: consumer not opened", "group", g.name, "err", cerr)

				continue
			}

			p.cons = cons
			g.s.recreated.Add(1)
		}
	}
}

// consumerGone reports whether a fetch failed because its consumer no
// longer exists. A pull to a consumer that is gone gets no responders (503)
// from the server, not a not-found error; one deleted under a running fetch
// says so.
func consumerGone(err error) bool {
	return errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) ||
		errors.Is(err, nats.ErrNoResponders)
}

// fetch runs one fetch and hands what it brings to handle. A fetch that
// ended with nothing to say (no messages, its wait over) is no error.
func (p *sharedPull) fetch() error {
	opts := []jetstream.FetchOpt{jetstream.FetchMaxWait(p.g.s.fetchWait)}
	if hb := p.g.s.heartbeat; hb > 0 && hb < p.g.s.fetchWait/2 {
		opts = append(opts, jetstream.FetchHeartbeat(hb))
	}

	batch, err := p.cons.Fetch(sharedPullBatch, opts...)
	if err != nil {
		return err
	}

	for msg := range batch.Messages() {
		if w := p.g.s.wrapMsg; w != nil {
			msg = w(msg)
		}

		p.handle(msg)
	}

	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
		return err
	}

	return nil
}

// handle decides one log message: not the group's, acked at once; otherwise
// handed to a member, and acked when the member is through with it.
func (p *sharedPull) handle(msg jetstream.Msg) {
	g := p.g
	s := g.s

	if p.stopped.Load() {
		// A little later, so it does not come straight back to the fetch
		// that is running out.
		s.nak(msg, sharedBusyDelay)

		return
	}

	md, err := msg.Metadata()
	if err != nil {
		s.logger.Warn("shared: message without metadata", "group", g.name, "err", err)

		return
	}

	lm, err := decodeLogMessage(msg.Subject(), msg.Headers(), msg.Data())
	if err != nil {
		s.logger.Warn("shared: skipping an entry that does not decode", "group", g.name, "seq", md.Sequence.Stream, "err", err)

		if terr := msg.Term(); terr != nil {
			s.logger.Debug("shared: term failed", "group", g.name, "err", terr)
		}

		return
	}

	lm.Seq = md.Sequence.Stream

	if lm.Tenant != g.key.tenant || !mqtttopic.Match(g.key.filter, lm.Topic) {
		s.skipped.Add(1)
		s.ack(msg)

		return
	}

	pd := &sharedPending{msg: msg, qos: lm.QoS}
	s.track(pd)

	taken, busy, members := g.offer(lm, pd.done(s))
	if taken {
		return
	}

	s.untrack(pd)

	switch {
	case members == 0 || busy:
		// Nobody here could take it now: another member, maybe on another
		// node, gets it a little later.
		s.nak(msg, sharedBusyDelay)
	default:
		// Every member here refused it for good (its grants): dropped, as
		// mochi drops a shared delivery the chosen client may not read.
		s.logger.Debug("shared: message refused by every member on the node, dropped", "group", g.name, "seq", lm.Seq)
		s.ack(msg)
	}
}

// sharedPending is a message handed to a member and not yet done.
type sharedPending struct {
	msg  jetstream.Msg
	qos  byte
	once sync.Once
}

// done returns the function a member calls when it is through with the
// message (nil, acked) or lost it (an error, naked for another member).
// Only the first call counts.
//
// A QoS 2 message is the member's once done(nil) is called (the broker pins
// it to the session first), so an ack JetStream never gets would let it
// redeliver the message to another member after the ack wait: two deliveries
// of one QoS 2 message [MQTT-4.8.2-5]. So its ack waits for the server's
// answer (ackPinned), and the message stays pending, kept alive with
// InProgress, until the ack is in. QoS 1 may arrive twice, so its ack is
// fire and forget.
func (pd *sharedPending) done(s *Shared) func(error) {
	return func(err error) {
		pd.once.Do(func() {
			switch {
			case err != nil:
				s.untrack(pd)
				s.nak(pd.msg, 0)
			case pd.qos == 2:
				go s.ackPinned(pd)
			default:
				s.untrack(pd)
				s.ack(pd.msg)
			}
		})
	}
}

// sharedAckRetry is the first wait before a QoS 2 ack is sent again;
// sharedAckRetryMax caps the doubling, sharedAckRetryFor bounds the whole.
const (
	sharedAckRetry    = 100 * time.Millisecond
	sharedAckRetryMax = 2 * time.Second
	sharedAckRetryFor = 10 * time.Minute
)

// ackPinned acks a QoS 2 message and waits for JetStream to confirm it
// (DoubleAck), sending it again with backoff until it does, Close, or
// sharedAckRetryFor passes. The message stays pending meanwhile, so the
// ticker keeps it from being redelivered.
func (s *Shared) ackPinned(pd *sharedPending) {
	defer s.untrack(pd)

	start := time.Now()
	delay := sharedAckRetry

	for {
		ctx, cancel := context.WithTimeout(context.Background(), max(s.ackWait/3, sharedAckRetryMax))
		err := pd.msg.DoubleAck(ctx)

		cancel()

		if err == nil || errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			s.acked.Add(1)

			return
		}

		if time.Since(start) > sharedAckRetryFor {
			s.logger.Warn("shared: qos 2 ack not confirmed, giving up; the message may be redelivered", "err", err)

			return
		}

		s.ackRetries.Add(1)
		s.logger.Debug("shared: qos 2 ack not confirmed, sending it again", "err", err)

		select {
		case <-s.quit:
			return
		case <-time.After(delay):
		}

		delay = min(2*delay, sharedAckRetryMax)
	}
}

func (s *Shared) ack(msg jetstream.Msg) {
	if err := msg.Ack(); err != nil {
		s.logger.Debug("shared: ack failed, the message will be redelivered", "err", err)

		return
	}

	s.acked.Add(1)
}

func (s *Shared) nak(msg jetstream.Msg, delay time.Duration) {
	var err error
	if delay > 0 {
		err = msg.NakWithDelay(delay)
	} else {
		err = msg.Nak()
	}

	if err != nil {
		s.logger.Debug("shared: nak failed, the message will be redelivered after the ack wait", "err", err)

		return
	}

	s.naked.Add(1)
}

func (s *Shared) track(pd *sharedPending) {
	s.pmu.Lock()
	s.pend[pd] = struct{}{}
	s.pmu.Unlock()
}

func (s *Shared) untrack(pd *sharedPending) {
	s.pmu.Lock()
	delete(s.pend, pd)
	s.pmu.Unlock()
}

// startTicker starts the goroutine that keeps the pending messages alive,
// once.
func (s *Shared) startTicker() {
	s.pmu.Lock()
	defer s.pmu.Unlock()

	if s.ticking {
		return
	}

	s.ticking = true
	s.tickStop, s.tickDone = make(chan struct{}), make(chan struct{})

	go s.tick(s.tickStop, s.tickDone)
}

// tick sends InProgress for every pending message every third of the ack
// wait, so a member that takes its time keeps its message and only a lost
// node lets JetStream redeliver.
func (s *Shared) tick(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	t := time.NewTicker(max(s.ackWait/3, 10*time.Millisecond))
	defer t.Stop()

	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}

		s.pmu.Lock()
		msgs := make([]jetstream.Msg, 0, len(s.pend))

		for pd := range s.pend {
			msgs = append(msgs, pd.msg)
		}
		s.pmu.Unlock()

		for _, m := range msgs {
			if err := m.InProgress(); err != nil {
				s.logger.Debug("shared: in progress failed", "err", err)
			}
		}
	}
}

// Close stops every group of the node and makes later Joins fail. Messages
// members still hold are naked, best effort, so another member gets them
// without waiting for the ack wait. It does not close the Conn, and does not
// delete the consumers. Idempotent.
func (s *Shared) Close() {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()

		return
	}

	s.closed = true
	close(s.quit)

	all := make([]*sharedGroup, 0, len(s.groups))
	for _, g := range s.groups {
		all = append(all, g)
	}

	s.groups = make(map[sharedKey]*sharedGroup)
	s.mu.Unlock()

	for _, g := range all {
		g.mu.Lock()
		g.dead = true
		g.refs = 0
		g.stop()
		g.mu.Unlock()

		g.mmu.Lock()
		g.members = nil
		g.mmu.Unlock()
	}

	s.pmu.Lock()
	ticking, stop, done := s.ticking, s.tickStop, s.tickDone
	s.ticking = false

	pend := make([]*sharedPending, 0, len(s.pend))
	for pd := range s.pend {
		pend = append(pend, pd)
	}
	s.pmu.Unlock()

	if ticking {
		close(stop)
		<-done
	}

	for _, pd := range pend {
		pd.done(s)(errSharedClosed)
	}
}
