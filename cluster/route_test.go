package cluster

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/subject"
)

type routed struct {
	tenant, topic string
	payload       string
	props         Props
	retain        bool
}

// sink collects what a Router's deliver callback is given.
type sink struct {
	mu   sync.Mutex
	msgs []routed

	// pause, when set, runs at the start of every deliver, outside mu, so a
	// test can make deliver slow.
	pause atomic.Pointer[func(payload string)]
}

func (s *sink) deliver(tenant, topic string, payload []byte, props Props, retain bool) {
	if p := s.pause.Load(); p != nil {
		(*p)(string(payload))
	}

	s.mu.Lock()
	s.msgs = append(s.msgs, routed{tenant, topic, string(payload), props, retain})
	s.mu.Unlock()
}

func (s *sink) all() []routed {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]routed(nil), s.msgs...)
}

// count is the number of deliveries for tenant and topic so far.
func (s *sink) count(tenant, topic string) int {
	n := 0

	for _, m := range s.all() {
		if m.tenant == tenant && m.topic == topic {
			n++
		}
	}

	return n
}

// waitCount waits until count(tenant, topic) is at least want.
func (s *sink) waitCount(t *testing.T, tenant, topic string, want int) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for s.count(tenant, topic) < want {
		if time.Now().After(deadline) {
			t.Fatalf("%s/%s delivered %d times, want at least %d", tenant, topic, s.count(tenant, topic), want)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// settle is how long a test waits before it calls a message not delivered.
const settle = 300 * time.Millisecond

// routeNode is one simulated broker node: its own NATS connection, its own
// Router and the sink behind its deliver callback.
type routeNode struct {
	conn *Conn
	r    *Router
	sink *sink
	base int // subscriptions nats.go holds on its own, before any route
}

// held is the number of subscriptions the router holds on the connection.
func (n *routeNode) held() int { return n.conn.NC().NumSubscriptions() - n.base }

// startRouteNodes starts a three-server cluster and n nodes on it, node i on
// server i%3, each with its own connection (the connections use NoEcho).
func startRouteNodes(t *testing.T, n int) []*routeNode {
	t.Helper()

	urls := clustertest.Start(t, 3)
	out := make([]*routeNode, 0, n)

	for i := range n {
		cfg := DefaultConfig()
		cfg.Enabled = true
		cfg.NodeID = fmt.Sprintf("n%d", i+1)
		cfg.Cell = "c1"
		cfg.Mode = ModeExternal
		cfg.URLs = []string{urls[i%len(urls)]}
		cfg.Replicas = 3

		c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(c.Close)

		s := &sink{}
		r := NewRouter(c, cfg.NodeID, s.deliver)

		t.Cleanup(r.Close)

		out = append(out, &routeNode{conn: c, r: r, sink: s, base: c.NC().NumSubscriptions()})
	}

	return out
}

var probeSeq atomic.Int64

// ready blocks until a's server knows b's interest. Interest travels over a
// route in the order it was created, so a probe subscription made after the
// ones under test arrives after them: once a probe publish from a reaches b,
// every earlier subscription of b is in place.
func ready(t *testing.T, a, b *routeNode) {
	t.Helper()

	tenant := fmt.Sprintf("probe%d", probeSeq.Add(1))
	if err := b.r.Subscribe(tenant, "p"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)

	for b.sink.count(tenant, "p") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("interest did not reach the publisher's server")
		}

		if err := a.r.Publish(tenant, "p", nil, Props{}, false); err != nil {
			t.Fatal(err)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func mustSub(t *testing.T, n *routeNode, tenant, filter string) {
	t.Helper()

	if err := n.r.Subscribe(tenant, filter); err != nil {
		t.Fatalf("subscribe %s %q: %v", tenant, filter, err)
	}
}

func mustPub(t *testing.T, n *routeNode, tenant, topic, payload string) {
	t.Helper()

	if err := n.r.Publish(tenant, topic, []byte(payload), Props{}, false); err != nil {
		t.Fatalf("publish %s %q: %v", tenant, topic, err)
	}
}

func TestRouteAcrossNodes(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "sensors/+/temp")
	ready(t, a, b)

	mustPub(t, a, "t1", "sensors/1/temp", "21.5")
	b.sink.waitCount(t, "t1", "sensors/1/temp", 1)

	// Not matching: a different depth, a different leaf.
	mustPub(t, a, "t1", "sensors/1/humidity", "x")
	mustPub(t, a, "t1", "sensors/1/2/temp", "x")
	mustPub(t, a, "t1", "sensors/1/temp", "again")
	b.sink.waitCount(t, "t1", "sensors/1/temp", 2)
	time.Sleep(settle)

	got := b.sink.all()
	if len(got) < 2 {
		t.Fatalf("deliveries = %v", got)
	}

	var seen []routed

	for _, m := range got {
		if !strings.HasPrefix(m.tenant, "probe") {
			seen = append(seen, m)
		}
	}

	want := []routed{{"t1", "sensors/1/temp", "21.5", Props{}, false}, {"t1", "sensors/1/temp", "again", Props{}, false}}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("deliveries = %+v, want %+v", seen, want)
	}
}

func TestOverlapDedup(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "a/+")
	mustSub(t, b, "t1", "a/#")
	mustSub(t, b, "t1", "#")
	ready(t, a, b)

	for i := range 20 {
		mustPub(t, a, "t1", fmt.Sprintf("a/%d", i), "m")
	}

	for i := range 20 {
		b.sink.waitCount(t, "t1", fmt.Sprintf("a/%d", i), 1)
	}

	time.Sleep(settle)

	for i := range 20 {
		if n := b.sink.count("t1", fmt.Sprintf("a/%d", i)); n != 1 {
			t.Fatalf("a/%d delivered %d times through three overlapping filters, want 1", i, n)
		}
	}
}

// Ruling R19: a topic matched by several of a node's subscriptions, each
// with its own NATS dispatch goroutine, still reaches deliver in publish
// order and once per message. A slow deliver on some messages widens the
// window where one goroutine could skip a duplicate and run ahead.
func TestOverlapKeepsTopicOrder(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "o/+")
	mustSub(t, b, "t1", "o/#")
	mustSub(t, b, "t1", "+/x")
	mustSub(t, b, "t1", "#")
	ready(t, a, b)

	pause := func(payload string) {
		if strings.HasSuffix(payload, "3") || strings.HasSuffix(payload, "7") {
			time.Sleep(200 * time.Microsecond)
		}
	}
	b.sink.pause.Store(&pause)

	const n = 2000

	for i := range n {
		mustPub(t, a, "t1", "o/x", fmt.Sprint(i))
	}

	b.sink.waitCount(t, "t1", "o/x", n)
	time.Sleep(settle)

	var got []string

	for _, m := range b.sink.all() {
		if m.topic == "o/x" {
			got = append(got, m.payload)
		}
	}

	if len(got) != n {
		t.Fatalf("o/x delivered %d times, want %d", len(got), n)
	}

	for i, p := range got {
		if p != fmt.Sprint(i) {
			t.Fatalf("delivery %d is message %s: out of order", i, p)
		}
	}
}

func TestHashMatchesParent(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "a/#")
	ready(t, a, b)

	for _, topic := range []string{"a", "a/b", "a/b/c"} {
		mustPub(t, a, "t1", topic, "m")
		b.sink.waitCount(t, "t1", topic, 1)
	}

	mustPub(t, a, "t1", "ab", "m")
	time.Sleep(settle)

	if n := b.sink.count("t1", "ab"); n != 0 {
		t.Fatalf("ab matched a/#: %d deliveries", n)
	}
}

func TestDollarRule(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "#")
	mustSub(t, b, "t1", "+/x")
	ready(t, a, b)

	mustPub(t, a, "t1", "$SYS/x", "m")
	// A plain topic through the same filters proves they work.
	mustPub(t, a, "t1", "plain/x", "m")
	b.sink.waitCount(t, "t1", "plain/x", 1)
	time.Sleep(settle)

	if n := b.sink.count("t1", "$SYS/x"); n != 0 {
		t.Fatalf("$SYS/x reached # or +/x: %d deliveries", n)
	}

	mustSub(t, b, "t1", "$SYS/#")
	ready(t, a, b)

	mustPub(t, a, "t1", "$SYS/x", "m")
	b.sink.waitCount(t, "t1", "$SYS/x", 1)
	time.Sleep(settle)

	if n := b.sink.count("t1", "$SYS/x"); n != 1 {
		t.Fatalf("$SYS/x delivered %d times to $SYS/#, want 1", n)
	}

	// The topic comes back as the publisher wrote it, "$" and all.
	for _, m := range b.sink.all() {
		if m.tenant == "t1" && m.topic == "$SYS/x" && m.payload != "m" {
			t.Fatalf("payload = %q", m.payload)
		}
	}
}

func TestTenantIsolation(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "x/#")
	mustSub(t, b, "t2", "x/#")
	ready(t, a, b)

	mustPub(t, a, "t1", "x/y", "m")
	b.sink.waitCount(t, "t1", "x/y", 1)
	time.Sleep(settle)

	if n := b.sink.count("t2", "x/y"); n != 0 {
		t.Fatalf("tenant t2 received %d messages published in t1", n)
	}

	if n := b.sink.count("t1", "x/y"); n != 1 {
		t.Fatalf("tenant t1 received %d, want 1", n)
	}

	// A tenant that is not a plain token still keeps its own space.
	odd := "tenant.one*>"
	mustSub(t, b, odd, "x/#")
	ready(t, a, b)

	mustPub(t, a, odd, "x/y", "m")
	b.sink.waitCount(t, odd, "x/y", 1)
	time.Sleep(settle)

	if n := b.sink.count("t1", "x/y"); n != 1 {
		t.Fatalf("t1 received %d after a publish in %q, want still 1", n, odd)
	}
}

func TestRefcount(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "r/#")
	mustSub(t, b, "t1", "r/#")

	if got := b.held(); got != 2 {
		t.Fatalf("two references to r/# hold %d nats subscriptions, want 2 (r and r.>)", got)
	}

	ready(t, a, b)

	if err := b.r.Unsubscribe("t1", "r/#"); err != nil {
		t.Fatal(err)
	}

	mustPub(t, a, "t1", "r/1", "m")
	b.sink.waitCount(t, "t1", "r/1", 1)

	if err := b.r.Unsubscribe("t1", "r/#"); err != nil {
		t.Fatal(err)
	}

	// Interest withdrawal reaches a's server asynchronously, and this publish may
	// still be routed; the subscription itself is gone, so nothing is delivered.
	mustPub(t, a, "t1", "r/2", "m")
	time.Sleep(settle)

	if n := b.sink.count("t1", "r/2"); n != 0 {
		t.Fatalf("delivered %d after the last Unsubscribe", n)
	}

	// Only the probe's subscription is left, on the one subject.
	if got := b.held(); got != 1 {
		t.Fatalf("nats subscriptions = %d after the last Unsubscribe, want 1 (the probe)", got)
	}

	err := b.r.Unsubscribe("t1", "r/#")
	if !errors.Is(err, tether.ErrNotFound) {
		t.Fatalf("a third Unsubscribe = %v, want ErrNotFound", err)
	}

	// The key is usable again.
	mustSub(t, b, "t1", "r/#")
	ready(t, a, b)
	mustPub(t, a, "t1", "r/3", "m")
	b.sink.waitCount(t, "t1", "r/3", 1)
}

func TestNoEcho(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, a, "t1", "e/#")
	mustSub(t, b, "t1", "e/#")
	ready(t, a, b)

	mustPub(t, a, "t1", "e/1", "m")
	b.sink.waitCount(t, "t1", "e/1", 1)
	time.Sleep(settle)

	if n := a.sink.count("t1", "e/1"); n != 0 {
		t.Fatalf("the publishing node received its own message %d times", n)
	}
}

func TestRouteProps(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "p/#")
	ready(t, a, b)

	format := byte(1)
	props := Props{
		User:            []UserProperty{{"k", "v1"}, {"k", "v2"}, {"crlf", "a\r\nb"}},
		ContentType:     "application/json",
		ResponseTopic:   "reply/me",
		CorrelationData: []byte{0, 1, 2, 255},
		MessageExpiry:   60,
		PayloadFormat:   &format,
	}

	if err := a.r.Publish("t1", "p/1", []byte("body"), props, false); err != nil {
		t.Fatal(err)
	}

	b.sink.waitCount(t, "t1", "p/1", 1)

	for _, m := range b.sink.all() {
		if m.topic != "p/1" {
			continue
		}

		if !reflect.DeepEqual(m.props, props) || m.payload != "body" || m.retain {
			t.Fatalf("delivered %+v, want props %+v and no retain flag", m, props)
		}
	}

	// The retain flag travels too.
	if err := a.r.Publish("t1", "p/2", []byte("kept"), Props{}, true); err != nil {
		t.Fatal(err)
	}

	b.sink.waitCount(t, "t1", "p/2", 1)

	for _, m := range b.sink.all() {
		if m.topic == "p/2" && !m.retain {
			t.Fatalf("delivered %+v, want the retain flag", m)
		}
	}
}

// A message from a publisher that is not a Tether node has no Tether-Id. It is
// delivered, once per matching subscription.
func TestForeignPublisherWithoutID(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	b := nodes[1]

	mustSub(t, b, "t1", "f/+")

	raw, err := nats.Connect(b.conn.NC().ConnectedUrl())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(raw.Close)

	if err := raw.Publish(subject.QoS0("t1", "f/1"), []byte("m")); err != nil {
		t.Fatal(err)
	}

	b.sink.waitCount(t, "t1", "f/1", 1)
	time.Sleep(settle)

	if n := b.sink.count("t1", "f/1"); n != 1 {
		t.Fatalf("delivered %d times, want 1", n)
	}

	// A subject the router cannot decode is dropped, not delivered.
	if err := raw.Publish("f.t1.f.a!b", []byte("m")); err != nil {
		t.Fatal(err)
	}

	if err := raw.Publish(subject.QoS0("t1", "f/2"), []byte("m")); err != nil {
		t.Fatal(err)
	}

	b.sink.waitCount(t, "t1", "f/2", 1)

	if got := len(b.sink.all()); got != 2 {
		t.Fatalf("%d deliveries, want 2 (the undecodable subject is dropped)", got)
	}
}

func TestRouterInvalidInput(t *testing.T) {
	nodes := startRouteNodes(t, 1)
	n := nodes[0]

	long := strings.Repeat("a", subject.MaxTopicBytes+1)

	for _, tc := range []struct {
		name, tenant, filter string
	}{
		{"empty filter", "t1", ""},
		{"empty tenant", "", "a"},
		{"hash not last", "t1", "a/#/b"},
		{"hash inside a level", "t1", "a/b#"},
		{"plus inside a level", "t1", "a+/b"},
		{"nul", "t1", "a\x00b"},
		{"over the size limit", "t1", long},
		{"shared", "t1", "$share/g/a/b"},
		{"shared upper case", "t1", "$SHARE/g/a/b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := n.r.Subscribe(tc.tenant, tc.filter); !errors.Is(err, tether.ErrInvalidInput) {
				t.Errorf("Subscribe = %v, want ErrInvalidInput", err)
			}

			if err := n.r.Unsubscribe(tc.tenant, tc.filter); !errors.Is(err, tether.ErrInvalidInput) {
				t.Errorf("Unsubscribe = %v, want ErrInvalidInput", err)
			}
		})
	}

	if got := n.held(); got != 0 {
		t.Fatalf("refused filters left %d nats subscriptions", got)
	}

	for _, tc := range []struct {
		name, tenant, topic string
	}{
		{"empty tenant", "", "a"},
		{"empty topic", "t1", ""},
		{"plus", "t1", "a/+/b"},
		{"hash", "t1", "a/#"},
		{"over the size limit", "t1", long},
	} {
		t.Run("publish "+tc.name, func(t *testing.T) {
			if err := n.r.Publish(tc.tenant, tc.topic, nil, Props{}, false); !errors.Is(err, tether.ErrInvalidInput) {
				t.Errorf("Publish = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestRouterClose(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	mustSub(t, b, "t1", "c/#")
	mustSub(t, b, "t2", "c/+")

	b.r.Close()
	b.r.Close()

	if got := b.held(); got != 0 {
		t.Fatalf("Close left %d nats subscriptions", got)
	}

	if err := b.r.Subscribe("t1", "c/#"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Subscribe after Close = %v, want ErrUnavailable", err)
	}

	if err := b.r.Publish("t1", "c/1", nil, Props{}, false); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Publish after Close = %v, want ErrUnavailable", err)
	}

	mustPub(t, a, "t1", "c/1", "m")
	time.Sleep(settle)

	if n := b.sink.count("t1", "c/1"); n != 0 {
		t.Fatalf("delivered %d after Close", n)
	}
}

// Subscribe and Unsubscribe from many goroutines on one key must keep the
// reference count and the NATS subscriptions in step.
func TestRefcountConcurrent(t *testing.T) {
	nodes := startRouteNodes(t, 2)
	a, b := nodes[0], nodes[1]

	const (
		churners = 24
		rounds   = 40
		holders  = 5
	)

	var wg sync.WaitGroup

	for range churners {
		wg.Go(func() {
			for range rounds {
				if err := b.r.Subscribe("t1", "k/#"); err != nil {
					t.Error(err)

					return
				}

				if err := b.r.Unsubscribe("t1", "k/#"); err != nil {
					t.Error(err)

					return
				}
			}
		})
	}

	for range holders {
		wg.Go(func() {
			if err := b.r.Subscribe("t1", "k/#"); err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()

	if t.Failed() {
		return
	}

	// The holders' references are all that remain: one pair of subscriptions.
	if got := b.held(); got != 2 {
		t.Fatalf("nats subscriptions = %d with %d references, want 2", got, holders)
	}

	ready(t, a, b)
	mustPub(t, a, "t1", "k/1", "m")
	b.sink.waitCount(t, "t1", "k/1", 1)

	for range holders {
		if err := b.r.Unsubscribe("t1", "k/#"); err != nil {
			t.Fatal(err)
		}
	}

	// Only the probe is left.
	if got := b.held(); got != 1 {
		t.Fatalf("nats subscriptions = %d after every reference was dropped, want 1 (the probe)", got)
	}

	b.r.mu.Lock()
	routes := len(b.r.routes)
	b.r.mu.Unlock()

	// The probe's route is the only one that may remain.
	if routes != 1 {
		t.Fatalf("router holds %d routes, want 1 (the probe)", routes)
	}

	if err := b.r.Unsubscribe("t1", "k/#"); !errors.Is(err, tether.ErrNotFound) {
		t.Fatalf("extra Unsubscribe = %v, want ErrNotFound", err)
	}
}

// The interleaving that once orphaned a route: Unsubscribe drops the last
// reference and is about to forget the route while a Subscribe has already
// pinned it. The route must survive with the new reference and its
// subscriptions, and a later Unsubscribe and Close must clean it up.
func TestUnpinKeepsRouteSubscribeWon(t *testing.T) {
	n := startRouteNodes(t, 1)[0]
	key := routeKey{"t1", "u/#"}

	mustSub(t, n, "t1", "u/#")

	subscribed := make(chan error, 1)
	armed := true

	n.r.testUnpin = func(k routeKey, _ *route) {
		if !armed || k != key {
			return
		}

		armed = false

		// Unsubscribe has dropped the last reference and holds the route
		// mutex. Start a Subscribe and wait until it has pinned the route.
		go func() { subscribed <- n.r.Subscribe("t1", "u/#") }()

		deadline := time.Now().Add(5 * time.Second)

		for {
			n.r.mu.Lock()
			e := n.r.routes[key]
			pinned := e != nil && e.pins == 2
			n.r.mu.Unlock()

			if pinned {
				break
			}

			if time.Now().After(deadline) {
				t.Error("the second Subscribe never pinned the route")

				return
			}

			time.Sleep(time.Millisecond)
		}

		// Give the Subscribe every chance to run to the end. While Unsubscribe
		// holds the route mutex it cannot; a version that released the mutex
		// before this point would let it finish, and then Unsubscribe would
		// delete a route that holds a reference.
		select {
		case err := <-subscribed:
			subscribed <- err
		case <-time.After(300 * time.Millisecond):
		}
	}

	if err := n.r.Unsubscribe("t1", "u/#"); err != nil {
		t.Fatal(err)
	}

	if err := <-subscribed; err != nil {
		t.Fatal(err)
	}

	n.r.mu.Lock()
	e := n.r.routes[key]
	n.r.mu.Unlock()

	if e == nil {
		t.Fatal("the route was deleted while a Subscribe held a reference to it")
	}

	if got := n.held(); got != 2 {
		t.Fatalf("held %d nats subscriptions, want 2", got)
	}

	if err := n.r.Unsubscribe("t1", "u/#"); err != nil {
		t.Fatalf("Unsubscribe of the surviving reference = %v", err)
	}

	if got := n.held(); got != 0 {
		t.Fatalf("held %d nats subscriptions after the last Unsubscribe, want 0", got)
	}

	// And Close cleans up a route that is still referenced.
	mustSub(t, n, "t1", "u/#")
	n.r.Close()

	if got := n.held(); got != 0 {
		t.Fatalf("Close left %d nats subscriptions", got)
	}
}

// Close racing Subscribe must not leave a subscription behind.
func TestCloseRacesSubscribe(t *testing.T) {
	nodes := startRouteNodes(t, 1)
	n := nodes[0]

	var wg sync.WaitGroup

	for i := range 32 {
		wg.Go(func() {
			_ = n.r.Subscribe("t1", fmt.Sprintf("race/%d/#", i%4))
		})
	}

	wg.Go(func() {
		time.Sleep(time.Millisecond)
		n.r.Close()
	})

	wg.Wait()

	if got := n.held(); got != 0 {
		t.Fatalf("%d nats subscriptions survived Close", got)
	}
}

func TestDedupWindow(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newDedup(1024, 2*time.Second)
	d.now = func() time.Time { return now }

	for range 2 {
		if d.duplicate("") {
			t.Fatal("an empty id is never a duplicate")
		}
	}

	if d.duplicate("a") {
		t.Fatal("first sight is a duplicate")
	}

	now = now.Add(1900 * time.Millisecond)

	if !d.duplicate("a") {
		t.Fatal("second sight inside the window is not a duplicate")
	}

	now = now.Add(200 * time.Millisecond)

	if d.duplicate("a") {
		t.Fatal("an id older than the window is still a duplicate")
	}

	if !d.duplicate("a") {
		t.Fatal("the id was not recorded again")
	}
}

func TestDedupBounded(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newDedup(1<<20, 2*time.Second)
	d.now = func() time.Time { return now }

	// Steady traffic: 100 ids per millisecond for six seconds of simulated time.
	// Memory follows the window (about 200 000 ids), not the total.
	peak := 0

	for ms := range 6_000 {
		now = now.Add(time.Millisecond)

		for j := range 100 {
			d.duplicate(fmt.Sprintf("n:%d:%d", ms, j))
		}

		if ms%1000 == 0 {
			peak = max(peak, d.size())
		}
	}

	if peak > 220_000 || d.size() > 220_000 {
		t.Fatalf("dedup holds %d ids (peak %d) after 600 000 ids at 100 000 per second, want about 200 000", d.size(), peak)
	}

	// Once traffic stops, a later insert drops every expired id.
	now = now.Add(10 * time.Second)

	for i := range dedupShardCount * 8 {
		d.duplicate(fmt.Sprintf("late:%d", i))
	}

	if d.size() > dedupShardCount*8 {
		t.Fatalf("dedup holds %d ids after the window passed, want at most %d", d.size(), dedupShardCount*8)
	}

	for i := range d.shards {
		s := &d.shards[i]
		if live := len(s.order) - s.head; live != len(s.seen) {
			t.Fatalf("shard %d: %d ordered entries, %d in the table", i, live, len(s.seen))
		}
	}
}

func TestDedupCap(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newDedup(64, time.Hour)
	d.now = func() time.Time { return now }

	for i := range 10_000 {
		d.duplicate(fmt.Sprintf("id%d", i))
	}

	if d.size() > 64 {
		t.Fatalf("dedup holds %d ids with a cap of 64", d.size())
	}

	// The newest id is still remembered.
	if !d.duplicate("id9999") {
		t.Fatal("the newest id was evicted")
	}
}
