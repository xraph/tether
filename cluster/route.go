package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/xraph/tether"
	"github.com/xraph/tether/subject"
	mqtttopic "github.com/xraph/tether/topic"
)

// hdrID is the header that names one publish. The router sets it from the node
// ID and a counter, and the receiving side deduplicates on it.
const hdrID = "Tether-Id"

// orderStripes is how many locks onMessage spreads subjects over (ruling
// R19). Messages on one subject always share a lock.
const orderStripes = 64

// errRouterClosed is returned by Subscribe and Publish after Close.
var errRouterClosed = fmt.Errorf("tether/cluster: router is closed: %w", ErrUnavailable)

type routeKey struct{ tenant, filter string }

// route is one (tenant, filter) with local subscribers. mu serializes the
// transitions of this key (first subscribe, last unsubscribe) so the NATS calls
// for one key never overlap, and never run under the router's map lock. refs and
// subs are guarded by mu; pins is guarded by the router's lock.
type route struct {
	mu   sync.Mutex
	refs int
	subs []*nats.Subscription
	dead bool // set by Close; a Subscribe that was waiting on mu must not revive the route
	pins int
}

// Router moves QoS 0 messages between the nodes of a cell by NATS subject
// interest. The node holds one core subscription per distinct (tenant, filter)
// that has local subscribers, reference counted, and publishes each message once
// on its subject; NATS delivers it to every node that has interest. Delivery to
// the publishing node's own clients stays with the local broker: the connection
// uses NoEcho, so a node never receives its own publishes.
//
// A message that matches two of a node's subscriptions (the filters "a/+" and
// "a/#" both match "a/b") arrives twice; the router drops the second by its
// Tether-Id header within a 2 s window.
//
// A Router is safe for concurrent use.
type Router struct {
	c       *Conn
	nodeID  string
	deliver func(tenant, topic string, payload []byte, props Props)
	dedup   *dedup
	counter atomic.Uint64

	// order holds a lock per stripe of subjects, held across the dedup claim
	// and deliver. Without it, of two subscriptions matching one topic, the
	// goroutine that loses message N's claim could deliver N+1 while the
	// winner is still delivering N.
	order [orderStripes]sync.Mutex

	mu     sync.Mutex // guards routes, closed and every route's pins
	routes map[routeKey]*route
	closed bool

	// testUnpin, when set by a test before the router is used, runs at the top
	// of unpin with the route's mutex held.
	testUnpin func(key routeKey, e *route)
}

// NewRouter returns a router on c for the node nodeID. deliver is called for
// every message that arrives from another node, after deduplication.
//
// deliver runs on NATS's dispatch goroutines, one per subscription. Calls for
// one topic never overlap and come in publish order, even when several
// subscriptions match it; calls for different topics can overlap. deliver
// runs under a lock shared by one stripe of subjects, so a slow deliver
// delays the messages behind it on its subscription and the messages of
// other subjects in its stripe (a subscription whose backlog passes the
// nats.go pending limits drops messages: QoS 0 allows that). Keep deliver
// short: hand the message to the broker's own queues and return. payload and
// props belong to deliver.
func NewRouter(c *Conn, nodeID string, deliver func(tenant, topic string, payload []byte, props Props)) *Router {
	r := &Router{
		c:       c,
		nodeID:  nodeID,
		deliver: deliver,
		dedup:   newDedup(dedupMaxEntries, dedupWindow),
		routes:  make(map[routeKey]*route),
	}

	// Seed the counter with the clock so a node that restarts under the same ID
	// does not repeat the ids of its last run inside the dedup window.
	r.counter.Store(uint64(time.Now().UnixNano()))

	return r
}

func invalidInput(format string, args ...any) error {
	return fmt.Errorf("tether/cluster: %s: %w", fmt.Sprintf(format, args...), tether.ErrInvalidInput)
}

// checkFilter validates a filter for Subscribe and Unsubscribe.
func checkFilter(tenant, filter string) error {
	if tenant == "" {
		return invalidInput("empty tenant")
	}

	if err := subject.CheckTopic(filter); err != nil {
		return fmt.Errorf("tether/cluster: filter: %w", err)
	}

	if err := mqtttopic.Valid(filter, true, subject.MaxTopicBytes); err != nil {
		return fmt.Errorf("tether/cluster: filter %q: %w", filter, err)
	}

	if first, _, _ := strings.Cut(filter, "/"); strings.EqualFold(first, "$share") {
		return invalidInput("shared filter %q is not routed here", filter)
	}

	return nil
}

// checkTopicName validates a topic name for Publish: no wildcards, no NUL,
// not empty.
func checkTopicName(name string) error {
	if err := mqtttopic.Valid(name, false, subject.MaxTopicBytes); err != nil {
		return fmt.Errorf("tether/cluster: topic %q: %w", name, err)
	}

	return nil
}

// pin returns the route for key, creating it, and takes its lock. Every pin is
// paired with an unpin.
func (r *Router) pin(key routeKey) (*route, error) {
	r.mu.Lock()

	if r.closed {
		r.mu.Unlock()

		return nil, errRouterClosed
	}

	e := r.routes[key]
	if e == nil {
		e = &route{}
		r.routes[key] = e
	}

	e.pins++
	r.mu.Unlock()

	e.mu.Lock()

	return e, nil
}

// unpin releases e and forgets it when nobody holds or waits for it and it has
// no references. It takes the router's lock while still holding e.mu, so refs
// cannot change between the check and the delete: a Subscribe that pinned e
// before this point shows in pins and keeps it, and one that pins after the
// delete makes a new route. The lock order is e.mu then Router.mu; no code waits
// for e.mu while holding Router.mu.
func (r *Router) unpin(key routeKey, e *route) {
	if r.testUnpin != nil {
		r.testUnpin(key, e)
	}

	r.mu.Lock()

	e.pins--
	if e.pins == 0 && e.refs == 0 && r.routes[key] == e {
		delete(r.routes, key)
	}

	r.mu.Unlock()
	e.mu.Unlock()
}

// Subscribe adds one reference to (tenant, filter). The first reference
// subscribes to the filter's QoS 0 subjects; later ones only count. The filter
// must be a valid MQTT filter; a shared filter ("$share/...") is refused. Errors
// for invalid input wrap tether.ErrInvalidInput, and a NATS failure wraps
// ErrUnavailable when NATS is unreachable.
func (r *Router) Subscribe(tenant, filter string) error {
	if err := checkFilter(tenant, filter); err != nil {
		return err
	}

	key := routeKey{tenant, filter}

	e, err := r.pin(key)
	if err != nil {
		return err
	}
	defer r.unpin(key, e)

	if e.dead {
		return errRouterClosed
	}

	if e.refs > 0 {
		e.refs++

		return nil
	}

	subjects := subject.QoS0Filters(tenant, filter)
	if len(subjects) == 0 {
		return invalidInput("no subjects for filter %q", filter)
	}

	subs := make([]*nats.Subscription, 0, len(subjects))

	for _, subj := range subjects {
		sub, err := r.c.nc.Subscribe(subj, r.onMessage)
		if err != nil {
			for _, s := range subs {
				if uerr := s.Unsubscribe(); uerr != nil {
					r.c.logger.Debug("router: undoing a partial subscribe failed", "subject", s.Subject, "err", uerr)
				}
			}

			return wrapNATS(fmt.Sprintf("subscribe %q", subj), err)
		}

		subs = append(subs, sub)
	}

	e.subs = subs
	e.refs = 1

	return nil
}

// Unsubscribe drops one reference to (tenant, filter), and unsubscribes when it
// was the last. An Unsubscribe with no matching Subscribe is a caller bug and
// returns an error wrapping tether.ErrNotFound; it changes nothing.
func (r *Router) Unsubscribe(tenant, filter string) error {
	if err := checkFilter(tenant, filter); err != nil {
		return err
	}

	key := routeKey{tenant, filter}

	e, err := r.pin(key)
	if err != nil {
		return err
	}
	defer r.unpin(key, e)

	if e.dead {
		return errRouterClosed
	}

	if e.refs == 0 {
		return fmt.Errorf("tether/cluster: unsubscribe %q: not subscribed: %w", filter, tether.ErrNotFound)
	}

	e.refs--
	if e.refs > 0 {
		return nil
	}

	return e.drop()
}

// drop unsubscribes every subscription of e. e.mu must be held.
func (e *route) drop() error {
	var errs []error

	for _, s := range e.subs {
		// A subscription that nats.go already closed (the connection is gone)
		// has nothing left to remove.
		if err := s.Unsubscribe(); err != nil &&
			!errors.Is(err, nats.ErrBadSubscription) && !errors.Is(err, nats.ErrConnectionClosed) {
			errs = append(errs, err)
		}
	}

	e.subs = nil

	if len(errs) > 0 {
		return wrapNATS("unsubscribe", errors.Join(errs...))
	}

	return nil
}

// Close unsubscribes everything and makes later Subscribe, Unsubscribe and
// Publish calls fail. It is idempotent. It does not close the Conn.
//
// Close is not a barrier for deliver: a message NATS had already handed to a
// dispatch goroutine can still reach deliver briefly after Close returns.
func (r *Router) Close() {
	r.mu.Lock()

	if r.closed {
		r.mu.Unlock()

		return
	}

	r.closed = true

	all := make([]*route, 0, len(r.routes))
	for _, e := range r.routes {
		all = append(all, e)
	}

	r.routes = make(map[routeKey]*route)
	r.mu.Unlock()

	for _, e := range all {
		e.mu.Lock()

		e.dead = true
		e.refs = 0

		if err := e.drop(); err != nil {
			r.c.logger.Debug("router: unsubscribe on close failed", "err", err)
		}

		e.mu.Unlock()
	}
}

// Publish sends one QoS 0 message to the cell: one core NATS publish on
// subject.QoS0(tenant, topic), with a Tether-Id header and props in headers. It
// does not wait for anyone; a message nobody has interest in is dropped by NATS.
// topic must be a topic name (no wildcards) within the size limit and tenant
// non-empty, or the error wraps tether.ErrInvalidInput.
func (r *Router) Publish(tenant, topic string, payload []byte, props Props) error {
	if tenant == "" {
		return invalidInput("empty tenant")
	}

	if err := subject.CheckTopic(topic); err != nil {
		return fmt.Errorf("tether/cluster: topic: %w", err)
	}

	if err := checkTopicName(topic); err != nil {
		return err
	}

	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()

	if closed {
		return errRouterClosed
	}

	subj := subject.QoS0(tenant, topic)
	if subj == "" {
		return invalidInput("no subject for tenant %q topic %q", tenant, topic)
	}

	h := make(nats.Header, 2)
	h.Set(hdrID, r.nodeID+":"+strconv.FormatUint(r.counter.Add(1), 10))
	props.encode(h)

	if err := r.c.nc.PublishMsg(&nats.Msg{Subject: subj, Header: h, Data: payload}); err != nil {
		return wrapNATS("publish", err)
	}

	return nil
}

// onMessage is the handler of every subscription.
//
// The dedup claim and deliver run under the subject's order lock, so a
// message reaches deliver only after every earlier message on its subject
// has been delivered or found a duplicate: each subscription hands its
// messages over in publish order, and a goroutine that finds message N a
// duplicate waits until the claim's deliver of N has returned.
func (r *Router) onMessage(m *nats.Msg) {
	tenant, topic, err := subject.DecodeQoS0(m.Subject)
	if err != nil {
		r.c.logger.Warn("router: dropped a message on an undecodable subject", "subject", m.Subject, "err", err)

		return
	}

	mu := &r.order[stripe(m.Subject)]
	mu.Lock()
	defer mu.Unlock()

	// A publisher that is not a Tether node sends no id. Deliver it once, without
	// deduplication: the message is real, and a client of an overlapping filter
	// seeing it twice beats losing it.
	if id := m.Header.Get(hdrID); id == "" {
		r.c.logger.Debug("router: message without a Tether-Id, delivered without deduplication", "subject", m.Subject)
	} else if r.dedup.duplicate(id) {
		return
	}

	var props Props
	if err := props.decode(m.Header); err != nil {
		// QoS 0 is best effort. Losing the properties of a damaged header beats
		// losing the message.
		r.c.logger.Warn("router: properties did not decode, delivering without them", "subject", m.Subject, "err", err)

		props = Props{}
	}

	r.deliver(tenant, topic, m.Data, props)
}

// stripe picks the order lock of a subject (FNV-1a).
func stripe(subj string) int {
	h := uint32(2166136261)

	for i := range len(subj) {
		h ^= uint32(subj[i])
		h *= 16777619
	}

	return int(h % orderStripes)
}

// wrapNATS wraps err for the caller, marking it ErrUnavailable when the cause is
// a network or connection failure.
func wrapNATS(op string, err error) error {
	if isUnavailable(err) || errors.Is(err, nats.ErrReconnectBufExceeded) {
		return fmt.Errorf("tether/cluster: %s: %w: %w", op, ErrUnavailable, err)
	}

	return fmt.Errorf("tether/cluster: %s: %w", op, err)
}
