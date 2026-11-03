package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster/clustertest"
)

// sharedNode is one node of a shared subscription test: its connection, log,
// shared groups and a Router for QoS 0 publishes.
type sharedNode struct {
	conn *Conn
	log  *Log
	sh   *Shared
	r    *Router
}

// startShared starts a three-server cell and n nodes on it, node i on server
// i%3, each with a Shared whose ack wait is ackWait. The nodes start once
// every server can route a write to the cell's streams.
func startShared(t *testing.T, n int, ackWait time.Duration) []*sharedNode {
	t.Helper()

	nc := clustertest.StartCluster(t, 3)
	urls := nc.URLs()
	out := make([]*sharedNode, 0, n)

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

		if i == 0 {
			if err := Provision(tctx(t, 30*time.Second), c, cfg, time.Hour); err != nil {
				t.Fatal(err)
			}

			nc.WaitInterest(t, cellSubjects(cfg)...)
		}

		l := NewLog(c, cfg)
		sh := NewShared(c, cfg, l)
		sh.ackWait = ackWait
		sh.heartbeat = time.Second

		t.Cleanup(sh.Close)

		r := NewRouter(c, cfg.NodeID, func(string, string, []byte, Props, bool) {})
		t.Cleanup(r.Close)

		out = append(out, &sharedNode{conn: c, log: l, sh: sh, r: r})
	}

	return out
}

// member records what a group member is handed. With hold set it keeps QoS 1
// and 2 messages without finishing them; otherwise it finishes each at once.
type member struct {
	mu   sync.Mutex
	got  []LogMessage
	held []func(error)
	hold bool
	busy bool
}

func (m *member) deliver(msg LogMessage, done func(error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.busy {
		return ErrMemberBusy
	}

	m.got = append(m.got, msg)

	if m.hold && msg.QoS > 0 {
		m.held = append(m.held, done)

		return nil
	}

	done(nil)

	return nil
}

func (m *member) payloads() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]string, 0, len(m.got))
	for _, g := range m.got {
		out = append(out, string(g.Payload))
	}

	return out
}

func (m *member) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.got)
}

// release finishes every held message with err and stops holding.
func (m *member) release(err error) {
	m.mu.Lock()
	held := m.held
	m.held, m.hold = nil, false
	m.mu.Unlock()

	for _, done := range held {
		done(err)
	}
}

func mustJoin(t *testing.T, sh *Shared, tenant, group, filter string, m *member) func() {
	t.Helper()

	leave, err := sh.Join(tenant, group, filter, m.deliver)
	if err != nil {
		t.Fatalf("join %s %s %s: %v", tenant, group, filter, err)
	}

	t.Cleanup(leave)

	return leave
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(d)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// tally counts every payload received by the members.
func tally(ms ...*member) map[string]int {
	out := map[string]int{}

	for _, m := range ms {
		for _, p := range m.payloads() {
			out[p]++
		}
	}

	return out
}

func appendN(t *testing.T, l *Log, prefix string, n int) {
	t.Helper()

	for i := range n {
		mustAppend(t, l, LogMessage{Tenant: "t1", Topic: prefix + strconv.Itoa(i), Payload: []byte(prefix + strconv.Itoa(i)), QoS: 1})
	}
}

// Members on A and B of one group: 100 QoS 1 messages go to exactly one
// member each, and both members get some.
func TestSharedGroupOneDeliveryAcrossNodes(t *testing.T) {
	nodes := startShared(t, 3, 5*time.Second)
	a, b := &member{}, &member{}

	mustJoin(t, nodes[0].sh, "t1", "g", "s/#", a)
	mustJoin(t, nodes[1].sh, "t1", "g", "s/#", b)

	appendN(t, nodes[2].log, "s/", 100)

	waitFor(t, "100 deliveries", 20*time.Second, func() bool { return a.count()+b.count() >= 100 })
	time.Sleep(300 * time.Millisecond)

	got := tally(a, b)
	if len(got) != 100 {
		t.Fatalf("got %d distinct messages, want 100", len(got))
	}

	for p, n := range got {
		if n != 1 {
			t.Fatalf("message %s delivered %d times", p, n)
		}
	}

	if a.count() == 0 || b.count() == 0 {
		t.Fatalf("one member got everything: a %d, b %d", a.count(), b.count())
	}

	t.Logf("a %d, b %d", a.count(), b.count())
}

// B's member holds what it is handed. When it is lost (the broker calls done
// with an error), the messages it held go to A, and so does everything after.
// When B's whole node is lost (its connection gone, nothing naked), what B
// held comes back after the ack wait.
func TestSharedMemberLoss(t *testing.T) {
	t.Run("member", func(t *testing.T) {
		nodes := startShared(t, 2, 30*time.Second)
		a, b := &member{}, &member{hold: true}

		mustJoin(t, nodes[0].sh, "t1", "g", "s/#", a)
		leaveB := mustJoin(t, nodes[1].sh, "t1", "g", "s/#", b)

		appendN(t, nodes[0].log, "s/", 50)
		waitFor(t, "50 handed", 20*time.Second, func() bool { return a.count()+b.count() >= 50 })

		if b.count() == 0 {
			t.Fatal("b got nothing to hold")
		}

		heldB := b.payloads()

		leaveB()
		b.release(errors.New("member lost"))

		appendN(t, nodes[0].log, "s/x", 50)

		// Within the ack wait: the naked ones come back at once.
		waitFor(t, "every message at A", 10*time.Second, func() bool { return len(tally(a)) >= 100 })

		got := tally(a)
		for _, p := range heldB {
			if got[p] != 1 {
				t.Fatalf("held %s reached A %d times", p, got[p])
			}
		}

		if len(b.payloads()) != len(heldB) {
			t.Fatalf("b got %d after it left, had %d", len(b.payloads()), len(heldB))
		}
	})

	t.Run("node", func(t *testing.T) {
		nodes := startShared(t, 2, 2*time.Second)
		a, b := &member{}, &member{hold: true}

		mustJoin(t, nodes[0].sh, "t1", "g", "s/#", a)
		mustJoin(t, nodes[1].sh, "t1", "g", "s/#", b)

		appendN(t, nodes[0].log, "s/", 50)
		waitFor(t, "50 handed", 20*time.Second, func() bool { return a.count()+b.count() >= 50 })

		if b.count() == 0 {
			t.Fatal("b got nothing to hold")
		}

		heldB := b.payloads()

		// The node dies: no nak, no ack, no InProgress.
		nodes[1].conn.nc.Close()

		nodes[1].conn.qmu.Lock()
		if q := nodes[1].conn.qnc; q != nil {
			q.Close()
		}
		nodes[1].conn.qmu.Unlock()

		start := time.Now()

		waitFor(t, "B's messages at A", 15*time.Second, func() bool { return len(tally(a)) >= 50 })

		if waited := time.Since(start); waited < time.Second {
			t.Fatalf("redelivered after %v, before the ack wait", waited)
		}

		got := tally(a)
		for _, p := range heldB {
			if got[p] != 1 {
				t.Fatalf("held %s reached A %d times", p, got[p])
			}
		}
	})
}

// A member that holds a message past the ack wait keeps it: its node sends
// InProgress, so nothing is redelivered while the member lives.
func TestSharedInProgressKeepsHeld(t *testing.T) {
	nodes := startShared(t, 2, time.Second)
	a, b := &member{hold: true}, &member{}

	mustJoin(t, nodes[0].sh, "t1", "g", "s/#", a)

	appendN(t, nodes[0].log, "s/", 5)
	waitFor(t, "5 held", 10*time.Second, func() bool { return a.count() >= 5 })

	mustJoin(t, nodes[1].sh, "t1", "g", "s/#", b)

	time.Sleep(3 * time.Second)

	if n := b.count(); n != 0 {
		t.Fatalf("b got %d messages a still holds", n)
	}

	a.release(nil)

	waitFor(t, "acks", 5*time.Second, func() bool { return nodes[0].sh.acked.Load() >= 5 })
}

// QoS 0: a queue group on the filter's subjects. Published from a node with
// no member, each message reaches one member of A and B, both get some. A
// group whose only member is on the publishing node gets the node's own
// publishes (ruling R5).
func TestSharedQoS0QueueGroup(t *testing.T) {
	nodes := startShared(t, 3, 5*time.Second)
	a, b := &member{}, &member{}

	mustJoin(t, nodes[0].sh, "t1", "g", "q/#", a)
	mustJoin(t, nodes[1].sh, "t1", "g", "q/#", b)

	// Interest of both queue subscriptions reaches node 3's server.
	waitFor(t, "warm-up at both members", 10*time.Second, func() bool {
		if err := nodes[2].r.Publish("t1", "q/warm", []byte("warm"), Props{}, false); err != nil {
			t.Fatal(err)
		}

		time.Sleep(5 * time.Millisecond)

		return a.count() > 0 && b.count() > 0
	})

	warm := a.count() + b.count()

	for i := range 100 {
		if err := nodes[2].r.Publish("t1", "q/n/"+strconv.Itoa(i), []byte(strconv.Itoa(i)), Props{}, false); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "100 deliveries", 10*time.Second, func() bool { return a.count()+b.count()-warm >= 100 })
	time.Sleep(300 * time.Millisecond)

	got := tally(a, b)
	delete(got, "warm")

	if len(got) != 100 {
		t.Fatalf("got %d distinct, want 100", len(got))
	}

	for p, n := range got {
		if n != 1 {
			t.Fatalf("message %s delivered %d times", p, n)
		}
	}

	ca, cb := 0, 0
	for _, p := range a.payloads() {
		if p != "warm" {
			ca++
		}
	}

	for _, p := range b.payloads() {
		if p != "warm" {
			cb++
		}
	}

	if ca == 0 || cb == 0 {
		t.Fatalf("one member got every message: a %d, b %d", ca, cb)
	}

	for _, m := range a.got {
		if m.QoS != 0 || m.Tenant != "t1" {
			t.Fatalf("bad delivery %+v", m)
		}
	}

	t.Run("publishing node only", func(t *testing.T) {
		solo := &member{}
		mustJoin(t, nodes[0].sh, "t1", "solo", "own/#", solo)

		if err := nodes[0].r.Publish("t1", "own/x", []byte("mine"), Props{User: []UserProperty{{Key: "k", Value: "v"}}}, true); err != nil {
			t.Fatal(err)
		}

		waitFor(t, "own publish", 5*time.Second, func() bool { return solo.count() == 1 })

		solo.mu.Lock()
		m := solo.got[0]
		solo.mu.Unlock()

		if string(m.Payload) != "mine" || m.Topic != "own/x" || !m.Retain || len(m.Props.User) != 1 {
			t.Fatalf("bad delivery %+v", m)
		}
	})
}

// The consumer's subject filter can be wider than the MQTT filter: what does
// not match is acked at once and never handed to a member, "$" topics
// included for a wildcard-first filter.
func TestSharedMatchesLocally(t *testing.T) {
	nodes := startShared(t, 1, 5*time.Second)
	a := &member{}

	mustJoin(t, nodes[0].sh, "t1", "g", "a/+/c", a)
	mustJoin(t, nodes[0].sh, "t1", "h", "+/x", &member{})

	l := nodes[0].log
	for _, tp := range []string{"a/b/c", "a/b/d", "a/b/c/d", "a/c", "$SYS/x", "b/x"} {
		mustAppend(t, l, LogMessage{Tenant: "t1", Topic: tp, Payload: []byte(tp), QoS: 1})
	}

	mustAppend(t, l, LogMessage{Tenant: "t1", Topic: "a/z/c", Payload: []byte("end"), QoS: 2})

	waitFor(t, "end", 10*time.Second, func() bool { return a.count() >= 2 })
	time.Sleep(200 * time.Millisecond)

	if got := a.payloads(); len(got) != 2 || got[0] != "a/b/c" || got[1] != "end" {
		t.Fatalf("got %v", got)
	}

	if a.got[1].QoS != 2 {
		t.Fatalf("qos %d, want 2", a.got[1].QoS)
	}

	if nodes[0].sh.skipped.Load() == 0 {
		t.Fatal("nothing acked as not matching")
	}
}

// Two tenants with the same group and filter are two groups: neither sees
// the other's messages, QoS 1 or QoS 0.
func TestSharedTenantIsolation(t *testing.T) {
	nodes := startShared(t, 2, 5*time.Second)
	m1, m2 := &member{}, &member{}

	mustJoin(t, nodes[0].sh, "t1", "g", "i/#", m1)
	mustJoin(t, nodes[1].sh, "t2", "g", "i/#", m2)

	mustAppend(t, nodes[0].log, LogMessage{Tenant: "t1", Topic: "i/a", Payload: []byte("one"), QoS: 1})
	mustAppend(t, nodes[1].log, LogMessage{Tenant: "t2", Topic: "i/a", Payload: []byte("two"), QoS: 1})

	waitFor(t, "both", 10*time.Second, func() bool { return m1.count() == 1 && m2.count() == 1 })

	// QoS 0, published from each member's own node.
	_ = nodes[0].r.Publish("t1", "i/b", []byte("one0"), Props{}, false)
	_ = nodes[1].r.Publish("t2", "i/b", []byte("two0"), Props{}, false)

	waitFor(t, "qos 0", 5*time.Second, func() bool { return m1.count() == 2 && m2.count() == 2 })
	time.Sleep(200 * time.Millisecond)

	for _, p := range m1.payloads() {
		if p != "one" && p != "one0" {
			t.Fatalf("t1 got %q", p)
		}
	}

	for _, p := range m2.payloads() {
		if p != "two" && p != "two0" {
			t.Fatalf("t2 got %q", p)
		}
	}
}

// A message nobody on the node can take now is naked and comes back; one
// every member refuses for good is acked and dropped.
func TestSharedBusyAndRefused(t *testing.T) {
	nodes := startShared(t, 1, 5*time.Second)
	a := &member{busy: true}

	mustJoin(t, nodes[0].sh, "t1", "g", "b/#", a)

	mustAppend(t, nodes[0].log, LogMessage{Tenant: "t1", Topic: "b/1", Payload: []byte("1"), QoS: 1})

	waitFor(t, "a nak", 5*time.Second, func() bool { return nodes[0].sh.naked.Load() > 0 })

	a.mu.Lock()
	a.busy = false
	a.mu.Unlock()

	waitFor(t, "redelivery", 5*time.Second, func() bool { return a.count() == 1 })

	refuse := func(LogMessage, func(error)) error { return errors.New("not allowed") }

	leave, err := nodes[0].sh.Join("t1", "r", "r/#", refuse)
	if err != nil {
		t.Fatal(err)
	}
	defer leave()

	before := nodes[0].sh.acked.Load()

	mustAppend(t, nodes[0].log, LogMessage{Tenant: "t1", Topic: "r/1", Payload: []byte("1"), QoS: 1})
	waitFor(t, "refused message acked", 5*time.Second, func() bool { return nodes[0].sh.acked.Load() > before })
}

// A persistent group keeps its place: with no member anywhere, messages wait
// in the consumer and reach the next member to join, on any node.
func TestSharedConsumerKeepsMessages(t *testing.T) {
	nodes := startShared(t, 2, 5*time.Second)
	a := &member{}

	leave := mustJoin(t, nodes[0].sh, "t1", "g", "k/#", a)
	leave()

	appendN(t, nodes[0].log, "k/", 5)

	b := &member{}
	mustJoin(t, nodes[1].sh, "t1", "g", "k/#", b)

	waitFor(t, "5 at b", 10*time.Second, func() bool { return b.count() == 5 })

	if a.count() != 0 {
		t.Fatalf("a got %d after leaving", a.count())
	}
}

func TestSharedNames(t *testing.T) {
	if a, b := sharedName("t1", "g", "a/#"), sharedName("t2", "g", "a/#"); a == b {
		t.Fatal("names collide across tenants")
	}

	if a, b := sharedName("t1", "g", "a/#"), sharedName("t1", "g2", "a/#"); a == b {
		t.Fatal("names collide across groups")
	}
}

func TestSharedInvalidInputAndClose(t *testing.T) {
	nodes := startShared(t, 1, 5*time.Second)
	sh := nodes[0].sh
	ok := (&member{}).deliver

	for _, c := range []struct{ tenant, group, filter string }{
		{"", "g", "a"},
		{"t", "", "a"},
		{"t", "g/h", "a"},
		{"t", "g+", "a"},
		{"t", "g", "a/#/b"},
		{"t", "g", "$share/x/a"},
		{"t", "g", string(make([]byte, 1025))},
	} {
		if _, err := sh.Join(c.tenant, c.group, c.filter, ok); !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("%+v: %v", c, err)
		}
	}

	if _, err := sh.Join("t", "g", "a", nil); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("nil deliver: %v", err)
	}

	leave, err := sh.Join("t", "g", "a", ok)
	if err != nil {
		t.Fatal(err)
	}

	sh.Close()
	sh.Close()
	leave()

	if _, err := sh.Join("t", "g", "a", ok); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("join after close: %v", err)
	}
}

// A consumer deleted between fetches (its inactive threshold, an operator)
// is made again: a pull to it gets no responders, which counts as gone.
func TestSharedConsumerRecreated(t *testing.T) {
	nodes := startShared(t, 1, 5*time.Second)
	a := &member{}

	mustJoin(t, nodes[0].sh, "t1", "g", "r/#", a)

	st, err := nodes[0].log.stream(tctx(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if err := st.DeleteConsumer(tctx(t, 10*time.Second), sharedName("t1", "g", "r/#")); err != nil {
		t.Fatal(err)
	}

	// A message published while the group has no consumer is not the
	// group's (DeliverNew), so wait for the pull loop's recreate.
	waitFor(t, "the consumer made again", 15*time.Second, func() bool { return nodes[0].sh.recreated.Load() == 1 })

	mustAppend(t, nodes[0].log, LogMessage{Tenant: "t1", Topic: "r/x", Payload: []byte("back"), QoS: 1})
	waitFor(t, "delivery through the new consumer", 10*time.Second, func() bool { return a.count() == 1 })
}

// A fetch on a consumer deleted before it started fails in a way the pull
// loop takes as "gone", so it makes the consumer again.
func TestSharedFetchOnDeletedConsumer(t *testing.T) {
	nodes := startShared(t, 1, 5*time.Second)

	st, err := nodes[0].log.stream(tctx(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	cons, err := st.CreateOrUpdateConsumer(tctx(t, 10*time.Second), jetstream.ConsumerConfig{Durable: "gone", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}

	if derr := st.DeleteConsumer(tctx(t, 10*time.Second), "gone"); derr != nil {
		t.Fatal(derr)
	}

	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err == nil {
		for range batch.Messages() {
			t.Fatal("a message from a deleted consumer")
		}

		err = batch.Error()
	}

	if !consumerGone(err) {
		t.Fatalf("fetch on a deleted consumer: %v, not taken as gone", err)
	}
}

// lossyMsg loses its acks on the wire: Ack sends nothing and reports
// success, and the first lost DoubleAcks fail without sending.
type lossyMsg struct {
	jetstream.Msg

	mu   *sync.Mutex
	lost *int
}

func (m lossyMsg) Ack() error { return nil }

func (m lossyMsg) DoubleAck(ctx context.Context) error {
	m.mu.Lock()
	lose := *m.lost > 0
	if lose {
		*m.lost--
	}
	m.mu.Unlock()

	if lose {
		return errors.New("ack lost")
	}

	return m.Msg.DoubleAck(ctx)
}

// A QoS 2 message a member is through with is acked only once JetStream
// confirms it: an ack lost on the way is sent again, and the message is kept
// alive meanwhile, so it never reaches another member after the ack wait
// [MQTT-4.8.2-5] (A-I2).
func TestSharedQoS2AckLostNotRedelivered(t *testing.T) {
	nodes := startShared(t, 2, time.Second)

	var mu sync.Mutex

	lost := 3
	nodes[0].sh.wrapMsg = func(m jetstream.Msg) jetstream.Msg { return lossyMsg{Msg: m, mu: &mu, lost: &lost} }

	a, b := &member{}, &member{}
	mustJoin(t, nodes[0].sh, "t1", "g", "s/#", a)

	for i := range 3 {
		if _, _, err := nodes[0].log.Append(tctx(t, 10*time.Second), LogMessage{
			Tenant: "t1", Topic: "s/x", Payload: fmt.Appendf(nil, "m%d", i), QoS: 2,
		}, ""); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, "3 at A", 10*time.Second, func() bool { return a.count() >= 3 })

	mustJoin(t, nodes[1].sh, "t1", "g", "s/#", b)

	time.Sleep(4 * time.Second)

	if n := b.count(); n != 0 {
		t.Fatalf("b got %d QoS 2 messages a already had", n)
	}

	if n := a.count(); n != 3 {
		t.Fatalf("a got %d deliveries of 3 messages", n)
	}

	waitFor(t, "acks confirmed", 5*time.Second, func() bool { return nodes[0].sh.acked.Load() >= 3 })

	if nodes[0].sh.ackRetries.Load() == 0 {
		t.Fatal("no ack was sent again")
	}
}
