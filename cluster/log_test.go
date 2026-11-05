package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster/clustertest"
)

// startLogs starts a three-server JetStream cluster, provisions a cell on it,
// waits until every server can route a write to the cell's streams, and
// returns one Log per node, node i on server i%3.
func startLogs(t *testing.T, nodes int) []*Log {
	t.Helper()

	nc := clustertest.StartCluster(t, 3)
	urls := nc.URLs()
	out := make([]*Log, 0, nodes)

	for i := range nodes {
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
		// Well under the 30 s contexts these tests give a call, so a
		// consumer that goes quiet shows as stalled, with its position.
		l.stallAfter = 10 * time.Second

		out = append(out, l)
	}

	return out
}

func mustAppend(t *testing.T, l *Log, msg LogMessage) uint64 {
	t.Helper()

	if msg.QoS == 0 {
		msg.QoS = 1
	}

	seq, dup, err := l.Append(tctx(t, 20*time.Second), msg, "")
	if err != nil {
		t.Fatalf("append %s/%s: %v", msg.Tenant, msg.Topic, err)
	}

	if dup {
		t.Fatalf("append %s/%s: unexpected duplicate", msg.Tenant, msg.Topic)
	}

	return seq
}

// collector is a reader or replay handler that records what it gets.
type collector struct {
	mu   sync.Mutex
	msgs []LogMessage
	ch   chan struct{}
}

func newCollector() *collector { return &collector{ch: make(chan struct{}, 4096)} }

func (c *collector) fn(m LogMessage) error {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()

	select {
	case c.ch <- struct{}{}:
	default:
	}

	return nil
}

func (c *collector) snapshot() []LogMessage {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.msgs)
}

func (c *collector) waitFor(t *testing.T, n int) []LogMessage {
	t.Helper()

	deadline := time.After(30 * time.Second)

	for {
		if got := c.snapshot(); len(got) >= n {
			return got
		}

		select {
		case <-c.ch:
		case <-deadline:
			t.Fatalf("got %d messages, want %d", len(c.snapshot()), n)
		}
	}
}

func seqs(ms []LogMessage) []uint64 {
	out := make([]uint64, len(ms))
	for i, m := range ms {
		out[i] = m.Seq
	}

	return out
}

func TestAppendThenReaderInOrder(t *testing.T) {
	logs := startLogs(t, 2)
	a, b := logs[0], logs[1]

	col := newCollector()

	r, err := a.Reader(testTenant, 0, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	// "ttnt_test.x" and "ttnt_tes" are not this tenant, however much their
	// names look like it.
	others := []string{"ttnt_test2", "ttnt_tes", "ttnt_test.x", "other"}

	want := make([]uint64, 0, 100)

	for i := range 100 {
		topic := fmt.Sprintf("dev/%d/state", i%10)

		seq, dup, err := b.Append(tctx(t, 20*time.Second), LogMessage{
			Tenant: testTenant, Topic: topic, Payload: fmt.Appendf(nil, "m%d", i), QoS: byte(1 + i%2), Origin: "n2",
		}, "")
		if err != nil || dup {
			t.Fatalf("append %d: dup=%v err=%v", i, dup, err)
		}

		want = append(want, seq)

		// Another tenant's message after every one of ours.
		mustAppend(t, b, LogMessage{Tenant: others[i%len(others)], Topic: topic, Payload: []byte("other")})
	}

	col.waitFor(t, 100)
	time.Sleep(200 * time.Millisecond) // anything extra would show up now

	got := col.snapshot()
	if len(got) != 100 {
		t.Fatalf("got %d messages, want 100", len(got))
	}

	if !reflect.DeepEqual(seqs(got), want) {
		t.Fatalf("sequences %v, want %v", seqs(got), want)
	}

	for i, m := range got {
		if m.Tenant != testTenant || m.Topic != fmt.Sprintf("dev/%d/state", i%10) ||
			string(m.Payload) != fmt.Sprintf("m%d", i) || m.QoS != byte(1+i%2) || m.Origin != "n2" || m.PublishedAt.IsZero() {
			t.Fatalf("message %d: %+v", i, m)
		}
	}

	if pos := r.Position(); pos != want[len(want)-1] {
		t.Fatalf("Position %d, want %d", pos, want[len(want)-1])
	}
}

func TestDollarTopicUnderTenant(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	col, colOther := newCollector(), newCollector()

	r, err := l.Reader(testTenant, 0, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	r2, err := l.Reader("ttnt_other", 0, colOther.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r2.Stop()

	s1 := mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "$SYS/x", Payload: []byte("sys")})
	mustAppend(t, l, LogMessage{Tenant: "ttnt_other", Topic: "$SYS/x", Payload: []byte("theirs")})
	s3 := mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "plain", Payload: []byte("plain")})

	got := col.waitFor(t, 2)
	if got[0].Seq != s1 || got[0].Topic != "$SYS/x" || got[1].Seq != s3 {
		t.Fatalf("got %+v", got)
	}

	other := colOther.waitFor(t, 1)
	if len(other) != 1 || string(other[0].Payload) != "theirs" {
		t.Fatalf("other tenant got %+v", other)
	}

	time.Sleep(200 * time.Millisecond)

	if n := len(col.snapshot()); n != 2 {
		t.Fatalf("tenant reader got %d messages, want 2", n)
	}
}

func TestQoS2DedupWithinWindow(t *testing.T) {
	logs := startLogs(t, 2)
	a, b := logs[0], logs[1]
	msg := LogMessage{Tenant: testTenant, Topic: "t", Payload: []byte("once"), QoS: 2}

	s1, dup, err := a.Append(tctx(t, 20*time.Second), msg, "pkt-1")
	if err != nil || dup {
		t.Fatalf("first: dup=%v err=%v", dup, err)
	}

	// The retry arrives through another node, as it would after a failover.
	s2, dup, err := b.Append(tctx(t, 20*time.Second), msg, "pkt-1")
	if err != nil || !dup || s2 != s1 {
		t.Fatalf("second: seq=%d dup=%v err=%v, want seq %d dup", s2, dup, err, s1)
	}

	s3, dup, err := a.Append(tctx(t, 20*time.Second), msg, "pkt-2")
	if err != nil || dup || s3 == s1 {
		t.Fatalf("third: seq=%d dup=%v err=%v", s3, dup, err)
	}

	// No ID, no dedup.
	s4 := mustAppend(t, a, LogMessage{Tenant: testTenant, Topic: "t", QoS: 1})
	s5 := mustAppend(t, a, LogMessage{Tenant: testTenant, Topic: "t", QoS: 1})

	if s4 == s5 {
		t.Fatalf("messages without an ID collapsed to seq %d", s4)
	}

	col := newCollector()
	if err := a.Replay(tctx(t, 20*time.Second), testTenant, 1, s5, col.fn); err != nil {
		t.Fatal(err)
	}

	if got := seqs(col.snapshot()); !reflect.DeepEqual(got, []uint64{s1, s3, s4, s5}) {
		t.Fatalf("log holds %v, want %v", got, []uint64{s1, s3, s4, s5})
	}
}

func TestReaderFromSeq(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	var all []uint64

	for i := range 20 {
		tenant := testTenant
		if i%5 == 4 {
			tenant = "ttnt_other"
		}

		s := mustAppend(t, l, LogMessage{Tenant: tenant, Topic: "t", Payload: fmt.Appendf(nil, "%d", i)})
		if tenant == testTenant {
			all = append(all, s)
		}
	}

	// From a sequence in the middle: the first message handed over is the
	// first of the tenant's at or after it.
	from := all[8]
	col := newCollector()

	r, err := l.Reader(testTenant, from, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	got := col.waitFor(t, len(all)-8)
	if !reflect.DeepEqual(seqs(got), all[8:]) {
		t.Fatalf("from %d: got %v, want %v", from, seqs(got), all[8:])
	}

	// Position moves once the handler returns, so the collector can hold the
	// last message a moment before Position reaches it.
	eventually(t, 5*time.Second, fmt.Sprintf("Position to reach %d", all[len(all)-1]), func() bool {
		return r.Position() == all[len(all)-1]
	})

	// From the future: nothing now, then what arrives from that sequence on.
	last := mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	col2 := newCollector()

	r2, err := l.Reader(testTenant, last+3, col2.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r2.Stop()

	if pos := r2.Position(); pos != last+2 {
		t.Fatalf("Position before any message %d, want %d", pos, last+2)
	}

	mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t", Payload: []byte("early")})     // last+1
	mustAppend(t, l, LogMessage{Tenant: "ttnt_other", Topic: "t"})                             // last+2
	want := mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t", Payload: []byte("a")}) // last+3

	got = col2.waitFor(t, 1)
	if got[0].Seq != want || want != last+3 {
		t.Fatalf("got seq %d, want %d", got[0].Seq, last+3)
	}
}

func TestReaderHandlerErrorAndBadEntry(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	var (
		mu    sync.Mutex
		calls []string
	)

	done := make(chan struct{})

	r, err := l.Reader(testTenant, 0, func(m LogMessage) error {
		mu.Lock()
		defer mu.Unlock()

		calls = append(calls, string(m.Payload))
		if string(m.Payload) == "last" {
			close(done)
		}

		if string(m.Payload) == "bad" {
			return errors.New("cannot deliver")
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t", Payload: []byte("bad")})

	// An entry that cannot be decoded: right subject, no headers.
	if _, err := l.c.js.Publish(tctx(t, 10*time.Second), "d."+testTenant+".t", []byte("headerless")); err != nil {
		t.Fatal(err)
	}

	// And one under a subject that is not an encoded topic at all.
	if _, err := l.c.js.Publish(tctx(t, 10*time.Second), "d."+testTenant+".=!!", []byte("junk")); err != nil {
		t.Fatal(err)
	}

	lastSeq := mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t", Payload: []byte("last")})

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("reader stalled")
	}

	mu.Lock()
	defer mu.Unlock()

	if !reflect.DeepEqual(calls, []string{"bad", "last"}) {
		t.Fatalf("handler saw %v", calls)
	}

	deadline := time.Now().Add(5 * time.Second)
	for r.Position() != lastSeq && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if r.Position() != lastSeq {
		t.Fatalf("Position %d, want %d", r.Position(), lastSeq)
	}
}

func TestReaderStop(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	col := newCollector()

	r, err := l.Reader(testTenant, 0, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	// Position is read while messages arrive.
	stopRead := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		var prev uint64

		for {
			select {
			case <-stopRead:
				return
			default:
			}

			if p := r.Position(); p < prev {
				t.Errorf("Position went back from %d to %d", prev, p)
			} else {
				prev = p
			}
		}
	})

	for range 20 {
		mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	}

	col.waitFor(t, 20)

	r.Stop()
	r.Stop() // idempotent

	close(stopRead)
	wg.Wait()

	n := len(col.snapshot())
	mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	time.Sleep(200 * time.Millisecond)

	if got := len(col.snapshot()); got != n {
		t.Fatalf("stopped reader still delivered: %d -> %d", n, got)
	}

	waitNoConsumers(t, l)
}

// waitNoConsumers waits until the log stream has no consumer left on the
// server: readers and replays delete theirs when they end.
func waitNoConsumers(t *testing.T, l *Log) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	var got int

	for time.Now().Before(deadline) {
		st, err := l.stream(tctx(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		info, err := st.Info(tctx(t, 5*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		if got = info.State.Consumers; got == 0 {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("log stream still has %d consumers", got)
}

func TestReplayBatchesConsumers(t *testing.T) {
	logs := startLogs(t, 2)
	writer, l := logs[0], logs[1]

	const total = 300

	var last uint64

	for i := range total {
		last = mustAppend(t, writer, LogMessage{Tenant: testTenant, Topic: fmt.Sprintf("t/%d", i%7), Payload: fmt.Appendf(nil, "%d", i)})
	}

	// The first replay holds the consumer on its first message. The nine
	// others arrive while the consumer has not gone past their fromSeq.
	var (
		gate    = make(chan struct{})
		hold    sync.Once
		results = make([]*collector, 10)
		errs    = make([]error, 10)
		wg      sync.WaitGroup
	)

	for i := range results {
		results[i] = newCollector()
	}

	start := func(i int, from uint64) {
		wg.Go(func() {
			errs[i] = l.Replay(tctx(t, 60*time.Second), testTenant, from, last, func(m LogMessage) error {
				if i == 0 {
					hold.Do(func() { <-gate })
				}

				return results[i].fn(m)
			})
		})
	}

	start(0, 1)

	waitWaiters := func(n int) {
		deadline := time.Now().Add(20 * time.Second)

		for time.Now().Before(deadline) {
			l.rmu.Lock()
			got := 0

			for _, r := range l.replayers[testTenant] {
				r.mu.Lock()
				got += len(r.waiters)
				r.mu.Unlock()
			}

			l.rmu.Unlock()

			if got >= n {
				return
			}

			time.Sleep(5 * time.Millisecond)
		}

		t.Fatalf("waited for %d replay calls to register", n)
	}

	// Wait until the first call's consumer has handed out the first message.
	deadline := time.Now().Add(20 * time.Second)

	for {
		l.rmu.Lock()

		var pos uint64
		for _, r := range l.replayers[testTenant] {
			r.mu.Lock()
			pos = max(pos, r.pos)
			r.mu.Unlock()
		}

		l.rmu.Unlock()

		if pos >= 1 {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("the first replay never started")
		}

		time.Sleep(5 * time.Millisecond)
	}

	// Stream sequences of the tenant's messages are 1..total here, so a
	// fromSeq of i+1 is past the message held and not yet read.
	for i := 1; i < 10; i++ {
		start(i, uint64(i+1))
	}

	waitWaiters(10)
	close(gate)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
	}

	if n := l.consumers.Load(); n != 1 {
		t.Fatalf("10 concurrent replays created %d consumers, want 1", n)
	}

	for i, c := range results {
		from := uint64(1)
		if i > 0 {
			from = uint64(i + 1)
		}

		got := seqs(c.snapshot())

		want := make([]uint64, 0, total)
		for s := from; s <= last; s++ {
			want = append(want, s)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("replay %d: got %d messages (first %v), want %d from %d", i, len(got), got[:min(3, len(got))], len(want), from)
		}
	}

	// The consumer is gone from the server, and from the table.
	waitNoConsumers(t, l)

	l.rmu.Lock()
	defer l.rmu.Unlock()

	if len(l.replayers) != 0 {
		t.Fatalf("replayers left: %v", l.replayers)
	}
}

func TestReplayOwnConsumerWhenPassed(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	var last uint64

	for range 50 {
		last = mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	}

	gate := make(chan struct{})

	var (
		hold sync.Once
		wg   sync.WaitGroup
	)

	first := make(chan struct{})

	wg.Go(func() {
		err := l.Replay(tctx(t, 60*time.Second), testTenant, 10, last, func(LogMessage) error {
			hold.Do(func() {
				close(first)
				<-gate
			})

			return nil
		})
		if err != nil {
			t.Errorf("first: %v", err)
		}
	})

	<-first // the consumer has handed out sequence 10

	// Lower than where the running consumer started: own consumer.
	// At or below where it is: own consumer.
	// Past it: shared.
	for _, tc := range []struct {
		from   uint64
		shared bool
	}{{5, false}, {10, false}, {11, true}} {
		before := l.consumers.Load()
		col := newCollector()

		done := make(chan error, 1)

		go func() { done <- l.Replay(tctx(t, 60*time.Second), testTenant, tc.from, last, col.fn) }()

		if tc.shared {
			// It joins the held consumer, so it cannot finish yet.
			select {
			case err := <-done:
				t.Fatalf("from %d finished while the shared consumer is held: %v", tc.from, err)
			case <-time.After(300 * time.Millisecond):
			}

			if n := l.consumers.Load(); n != before {
				t.Fatalf("from %d: created a consumer, count %d -> %d", tc.from, before, n)
			}

			close(gate)

			if err := <-done; err != nil {
				t.Fatal(err)
			}
		} else {
			if err := <-done; err != nil {
				t.Fatalf("from %d: %v", tc.from, err)
			}

			if n := l.consumers.Load(); n != before+1 {
				t.Fatalf("from %d: consumers %d -> %d, want one more", tc.from, before, n)
			}
		}

		got := seqs(col.snapshot())
		if len(got) != int(last-tc.from)+1 || got[0] != tc.from || got[len(got)-1] != last {
			t.Fatalf("from %d: got %d messages %v...", tc.from, len(got), got[:min(3, len(got))])
		}
	}

	wg.Wait()
	waitNoConsumers(t, l)
}

func TestReplayRanges(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	// Tenant a and b alternate, then b alone: a's last message is early.
	var as []uint64

	for i := range 20 {
		tenant := testTenant
		if i%2 == 1 {
			tenant = "ttnt_other"
		}

		s := mustAppend(t, l, LogMessage{Tenant: tenant, Topic: "t", Payload: fmt.Appendf(nil, "%d", i)})
		if tenant == testTenant {
			as = append(as, s)
		}
	}

	for range 20 {
		mustAppend(t, l, LogMessage{Tenant: "ttnt_other", Topic: "t"})
	}

	replay := func(from, to uint64) ([]uint64, error) {
		col := newCollector()
		err := l.Replay(tctx(t, 30*time.Second), testTenant, from, to, col.fn)

		return seqs(col.snapshot()), err
	}

	cases := []struct {
		name     string
		from, to uint64
		want     []uint64
		consumer bool
	}{
		{"middle", as[2], as[6], as[2:7], true},
		{"single", as[3], as[3], as[3:4], true},
		{"to beyond the log", as[5], 1 << 40, as[5:], true},
		{"from zero", 0, as[1], as[:2], true},
		{"tenant has nothing in range", as[len(as)-1] + 1, 1 << 40, nil, false},
		{"gap between messages", as[1] + 1, as[2] - 1, nil, true},
		{"from above to", 9, 3, nil, false},
		{"above the log", 1000, 2000, nil, false},
	}

	for _, tc := range cases {
		before := l.consumers.Load()

		got, err := replay(tc.from, tc.to)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}

		if !slices.Equal(got, tc.want) {
			t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
		}

		made := l.consumers.Load() - before
		if !tc.consumer && made != 0 {
			t.Fatalf("%s: created %d consumers for an empty range", tc.name, made)
		}
	}

	// An unknown tenant reads nothing and creates nothing.
	before := l.consumers.Load()

	col := newCollector()
	if err := l.Replay(tctx(t, 30*time.Second), "ttnt_nobody", 1, 100, col.fn); err != nil || len(col.snapshot()) != 0 {
		t.Fatalf("unknown tenant: %v, %v", err, col.snapshot())
	}

	if l.consumers.Load() != before {
		t.Fatal("unknown tenant created a consumer")
	}

	waitNoConsumers(t, l)
}

// TestReplaySettle drives the case the clamp to the tenant's last message does
// not cover: a range that reaches past it, so only the consumer's own report
// that it has caught up can end the call.
func TestReplaySettle(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	var last uint64

	for range 5 {
		last = mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	}

	mustAppend(t, l, LogMessage{Tenant: "ttnt_other", Topic: "t"})

	run := func(from, to uint64) []uint64 {
		col := newCollector()
		w := &waiter{ctx: t.Context(), from: from, to: to, fn: col.fn, done: make(chan struct{})}

		r, created := l.join(testTenant, w)
		if !created {
			t.Fatal("expected a new consumer")
		}

		go r.run()

		select {
		case <-w.done:
		case <-time.After(20 * time.Second):
			t.Fatalf("[%d, %d] never finished", from, to)
		}

		if err := w.result(); err != nil {
			t.Fatal(err)
		}

		// The call is done before its consumer closes: until then the next
		// call could join it.
		eventually(t, 10*time.Second, "the replay consumer to close", r.isClosed)

		return seqs(col.snapshot())
	}

	if got := run(1, 1<<40); len(got) != 5 || got[4] != last {
		t.Fatalf("got %v", got)
	}

	// Nothing of the tenant's at or after the start: no message ever comes.
	if got := run(last+1, 1<<40); len(got) != 0 {
		t.Fatalf("got %v", got)
	}

	waitNoConsumers(t, l)
}

func TestReplayCancelAndHandlerError(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	var last uint64

	for range 30 {
		last = mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	}

	// A handler error ends that call with the error, once.
	boom := errors.New("boom")

	var calls int

	err := l.Replay(tctx(t, 20*time.Second), testTenant, 1, last, func(LogMessage) error {
		calls++

		return boom
	})
	if !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}

	// The context ends a call whose handler is busy; the handler is not
	// running once Replay has returned.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var (
		running atomic.Int32
		entered = make(chan struct{})
		calls2  atomic.Int32
	)

	go func() {
		<-entered
		cancel()
	}()

	err = l.Replay(ctx, testTenant, 1, last, func(LogMessage) error {
		calls2.Add(1)
		running.Add(1)
		defer running.Add(-1)

		if calls2.Load() == 1 {
			close(entered)
			time.Sleep(300 * time.Millisecond)
		}

		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	// The error says how far the consumer got.
	if !strings.Contains(err.Error(), "messages handed out") {
		t.Fatalf("err = %v, want the consumer's progress in it", err)
	}

	if running.Load() != 0 {
		t.Fatal("handler still running after Replay returned")
	}

	n := calls2.Load()

	time.Sleep(300 * time.Millisecond)

	if calls2.Load() != n {
		t.Fatal("handler called after Replay returned")
	}

	waitNoConsumers(t, l)

	l.rmu.Lock()
	defer l.rmu.Unlock()

	if len(l.replayers) != 0 {
		t.Fatalf("replayers left: %v", l.replayers)
	}
}

func TestAppendInvalidInput(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	long := strings.Repeat("a", 1025)

	for name, tc := range map[string]struct {
		msg   LogMessage
		msgID string
	}{
		"empty tenant":    {LogMessage{Topic: "t", QoS: 1}, ""},
		"topic too long":  {LogMessage{Tenant: testTenant, Topic: long, QoS: 1}, ""},
		"qos 0":           {LogMessage{Tenant: testTenant, Topic: "t"}, ""},
		"qos 3":           {LogMessage{Tenant: testTenant, Topic: "t", QoS: 3}, ""},
		"origin newline":  {LogMessage{Tenant: testTenant, Topic: "t", QoS: 1, Origin: "n1\r\nX: y"}, ""},
		"msg id newline":  {LogMessage{Tenant: testTenant, Topic: "t", QoS: 1}, "a\nb"},
		"msg id trailing": {LogMessage{Tenant: testTenant, Topic: "t", QoS: 1}, "a "},
	} {
		_, _, err := l.Append(tctx(t, 5*time.Second), tc.msg, tc.msgID)
		if !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("%s: err = %v, want ErrInvalidInput", name, err)
		}
	}

	// The same checks guard readers and replays.
	if _, err := l.Reader("", 0, func(LogMessage) error { return nil }); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("reader with empty tenant: %v", err)
	}

	if _, err := l.Reader(testTenant, 0, nil); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("reader with nil fn: %v", err)
	}

	if err := l.Replay(tctx(t, 5*time.Second), "", 1, 2, func(LogMessage) error { return nil }); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("replay with empty tenant: %v", err)
	}

	// Nothing was published: the log is empty.
	st, err := l.stream(tctx(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	info, err := st.Info(tctx(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if info.State.Msgs != 0 {
		t.Fatalf("log holds %d messages after only invalid appends", info.State.Msgs)
	}
}

func TestAppendUnavailable(t *testing.T) {
	logs := startLogs(t, 2)
	l := logs[1]

	mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	l.c.Close()

	_, _, err := l.Append(tctx(t, 5*time.Second), LogMessage{Tenant: testTenant, Topic: "t", QoS: 1}, "")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestLogMessageRoundTripThroughLog(t *testing.T) {
	logs := startLogs(t, 1)
	l := logs[0]

	format := byte(0)
	at := time.Date(2026, 10, 23, 12, 30, 15, 123456789, time.UTC)

	props := Props{
		User: []UserProperty{
			{"k", "line one\r\nline two"},
			{"k", "second"}, // duplicate key, kept in order
			{"", ""},
			{"  spaced  ", "  value  "},
			{"unicode", "h\u00e9llo \u2603 \U0001F600"},
			{"colon", "Nats-Msg-Id: x\r\n\r\nbody"},
		},
		ContentType:     "text/plain;\r\ncharset=\"utf-8\"",
		ResponseTopic:   "reply/\n/to",
		CorrelationData: []byte{0, 1, 2, '\r', '\n', 0xff, 0xfe},
		MessageExpiry:   3600,
		PayloadFormat:   &format,
	}

	col := newCollector()

	r, err := l.Reader(testTenant, 0, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	msg := LogMessage{
		Tenant: testTenant, Topic: "a b/\u00e9/$x/", Payload: []byte("body\r\n"), QoS: 2, Retain: true,
		Props: props, Origin: "node-1", ClientID: "dev \u00e9\r\n/1", PublishedAt: at,
	}

	seq, _, err := l.Append(tctx(t, 20*time.Second), msg, "id-1")
	if err != nil {
		t.Fatal(err)
	}

	// Properties that are all absent add no headers.
	mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "plain", QoS: 1})

	got := col.waitFor(t, 2)

	msg.Seq = seq
	if !reflect.DeepEqual(got[0], msg) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got[0], msg)
	}

	if !got[1].Props.IsZero() || got[1].Retain || got[1].Origin != "" || got[1].ClientID != "" || len(got[1].Payload) != 0 {
		t.Fatalf("plain message: %+v", got[1])
	}
}

func isB64URLChar(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
}

func TestPropsEncodeDecode(t *testing.T) {
	zero, one := byte(0), byte(1)

	cases := map[string]Props{
		"zero":          {},
		"user only":     {User: []UserProperty{{"a", "1"}, {"a", "2"}, {"b", ""}}},
		"newlines":      {User: []UserProperty{{"k\n", "v\r\n"}, {"\r\n\r\n", "\n"}}, ContentType: "x\ny", ResponseTopic: "\r\n"},
		"not utf8":      {User: []UserProperty{{"\xff\xfe", "\x80"}}},
		"empty and nul": {User: []UserProperty{{"", ""}, {"\x00", "\x00"}}},
		"binary":        {CorrelationData: bytes.Repeat([]byte{0, 0xff, '\r', '\n'}, 100)},
		"numbers":       {MessageExpiry: 1<<32 - 1, PayloadFormat: &one},
		"format zero":   {PayloadFormat: &zero},
		"large":         {User: slices.Repeat([]UserProperty{{strings.Repeat("k", 300), strings.Repeat("v", 70000)}}, 3)},
	}

	for name, p := range cases {
		h := nats.Header{}
		p.encode(h)

		for k, vs := range h {
			for _, v := range vs {
				if !strings.HasPrefix(k, "Tether-Prop-") {
					t.Errorf("%s: unexpected header %q", name, k)
				}

				for _, c := range v {
					if !isB64URLChar(c) {
						t.Fatalf("%s: header %s holds %q", name, k, c)
					}
				}
			}
		}

		var got Props
		if err := got.decode(h); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		if !reflect.DeepEqual(got, p) {
			t.Fatalf("%s:\n got %+v\nwant %+v", name, got, p)
		}
	}

	if h := (nats.Header{}); true {
		Props{}.encode(h)

		if len(h) != 0 {
			t.Fatalf("zero Props wrote headers %v", h)
		}
	}

	// Bad headers are errors, not silently dropped properties.
	for name, h := range map[string]nats.Header{
		"user not base64":  {hdrPropUser: {"***"}},
		"user truncated":   {hdrPropUser: {"BQE"}},
		"expiry not num":   {hdrPropExpiry: {"x"}},
		"expiry too large": {hdrPropExpiry: {"4294967296"}},
		"format too large": {hdrPropFormat: {"256"}},
		"corr not base64":  {hdrPropCorrelation: {"%"}},
	} {
		var p Props
		if err := p.decode(h); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestReaderAndReplayWhenConnClosed(t *testing.T) {
	logs := startLogs(t, 2)
	l := logs[1]

	for range 5 {
		mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	}

	col := newCollector()

	r, err := l.Reader(testTenant, 1, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	col.waitFor(t, 5)

	// A replay that is still running when the connection goes away must end.
	held := make(chan struct{})
	replayed := make(chan error, 1)

	go func() {
		replayed <- l.Replay(tctx(t, 60*time.Second), testTenant, 1, 5, func(LogMessage) error {
			select {
			case held <- struct{}{}:
			default:
			}

			time.Sleep(time.Second)

			return nil
		})
	}()

	<-held
	l.c.Close()

	stopped := make(chan struct{})

	go func() {
		r.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(20 * time.Second):
		t.Fatal("Stop did not return after the connection closed")
	}

	select {
	case <-replayed:
	case <-time.After(30 * time.Second):
		t.Fatal("Replay did not return after the connection closed")
	}
}

// startOutageLog starts a one-server cell, so a test can take NATS away from
// the node, and returns the log with the server to stop. Readers and replays
// notice a dead server after about a second.
func startOutageLog(t *testing.T) (*Log, *clustertest.Cluster) {
	t.Helper()

	cl := clustertest.StartCluster(t, 1)

	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.NodeID = "n1"
	cfg.Cell = "c1"
	cfg.Mode = ModeExternal
	cfg.URLs = cl.URLs()
	cfg.Replicas = 1

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	if err := Provision(tctx(t, 30*time.Second), c, cfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	l := NewLog(c, cfg)
	l.heartbeat = 500 * time.Millisecond

	return l, cl
}

// A node cut off from NATS has readers inside the ordered consumer's recreate
// loop, which the library does not leave for ten seconds or more. Stop must
// not wait for it: a fenced node stops all its readers at once.
func TestReaderStopWhileUnreachable(t *testing.T) {
	l, cl := startOutageLog(t)

	for range 5 {
		mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	}

	var calls atomic.Int32

	readers := make([]*Reader, 3)

	for i := range readers {
		tenant := testTenant
		if i > 0 {
			tenant = fmt.Sprintf("ttnt_other%d", i)

			mustAppend(t, l, LogMessage{Tenant: tenant, Topic: "t"})
		}

		r, err := l.Reader(tenant, 1, func(LogMessage) error {
			calls.Add(1)

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		readers[i] = r
	}

	deadline := time.Now().Add(20 * time.Second)
	for calls.Load() < 7 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if calls.Load() != 7 {
		t.Fatalf("readers saw %d messages, want 7", calls.Load())
	}

	cl.Stop(0)
	time.Sleep(4 * time.Second) // well past two missed heartbeats

	start := time.Now()

	for _, r := range readers {
		r.Stop()
	}

	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("stopping 3 readers took %s while NATS was unreachable", d)
	}

	n := calls.Load()

	time.Sleep(time.Second)

	if calls.Load() != n {
		t.Fatal("handler called after Stop returned")
	}
}

// After Stop returns the handler is not running and is never called again,
// even when Stop arrives in the middle of a message.
func TestReaderStopWaitsForRunningHandler(t *testing.T) {
	l, _ := startOutageLog(t)

	var (
		running atomic.Int32
		calls   atomic.Int32
		entered = make(chan struct{}, 1)
	)

	r, err := l.Reader(testTenant, 0, func(LogMessage) error {
		calls.Add(1)
		running.Add(1)
		defer running.Add(-1)

		select {
		case entered <- struct{}{}:
		default:
		}

		time.Sleep(300 * time.Millisecond)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for range 10 {
		mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	}

	<-entered
	r.Stop()

	if running.Load() != 0 {
		t.Fatal("handler running after Stop returned")
	}

	n := calls.Load()

	time.Sleep(time.Second)

	if calls.Load() != n {
		t.Fatalf("handler called after Stop returned: %d -> %d", n, calls.Load())
	}

	waitNoConsumers(t, l)
}

// A cancelled Replay returns when its context ends, also while the consumer
// is stuck recreating itself against a server that is gone.
func TestReplayCancelWhileUnreachable(t *testing.T) {
	l, cl := startOutageLog(t)

	const total = 3000

	var wg sync.WaitGroup

	for w := range 10 {
		wg.Go(func() {
			for range total / 10 {
				if _, _, err := l.Append(tctx(t, 60*time.Second), LogMessage{Tenant: testTenant, Topic: fmt.Sprintf("t/%d", w), QoS: 1}, ""); err != nil {
					t.Error(err)

					return
				}
			}
		})
	}

	wg.Wait()

	if t.Failed() {
		return
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var (
		gate    = make(chan struct{})
		entered = make(chan struct{})
		once    sync.Once
		calls   atomic.Int32
		running atomic.Int32
	)

	done := make(chan error, 1)

	go func() {
		done <- l.Replay(ctx, testTenant, 1, total, func(LogMessage) error {
			calls.Add(1)
			running.Add(1)
			defer running.Add(-1)

			once.Do(func() {
				close(entered)
				<-gate
			})

			return nil
		})
	}()

	<-entered
	cl.Stop(0)
	close(gate) // the buffered messages drain, then the consumer is cut off

	time.Sleep(4 * time.Second)

	select {
	case err := <-done:
		t.Fatalf("replay ended before cancel: %v (%d messages)", err, calls.Load())
	default:
	}

	start := time.Now()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Replay did not return after cancel")
	}

	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("cancelled Replay took %s to return while NATS was unreachable", d)
	}

	if running.Load() != 0 {
		t.Fatal("handler running after Replay returned")
	}

	n := calls.Load()

	time.Sleep(time.Second)

	if calls.Load() != n {
		t.Fatal("handler called after Replay returned")
	}
}

// Done closes once the reader's loop has exited: after Stop, and on its own
// when the loop ends without one (here the connection closes under it).
func TestReaderDone(t *testing.T) {
	logs := startLogs(t, 2)

	r, err := logs[0].Reader(testTenant, 0, func(LogMessage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-r.Done():
		t.Fatal("Done closed before Stop")
	case <-time.After(200 * time.Millisecond):
	}

	r.Stop()

	select {
	case <-r.Done():
	case <-time.After(20 * time.Second):
		t.Fatal("Done not closed after Stop")
	}

	l := logs[1]

	r2, err := l.Reader(testTenant, 0, func(LogMessage) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	defer r2.Stop()

	l.c.Close()

	select {
	case <-r2.Done():
	case <-time.After(30 * time.Second):
		t.Fatal("Done not closed after the connection closed")
	}
}

// Appends started one after another with AppendAsync are committed in that
// order, while their waits overlap.
func TestAppendAsyncKeepsOrder(t *testing.T) {
	logs := startLogs(t, 2)
	l := logs[1]

	col := newCollector()

	r, err := logs[0].Reader(testTenant, 0, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	const n = 200

	pending := make([]*PendingAppend, n)

	for i := range n {
		p, err := l.AppendAsync(tctx(t, 20*time.Second), LogMessage{
			Tenant: testTenant, Topic: "order", QoS: 1, Payload: []byte(fmt.Sprint(i)),
		}, fmt.Sprintf("order-%d", i))
		if err != nil {
			t.Fatal(err)
		}

		pending[i] = p
	}

	var last uint64

	for i, p := range pending {
		seq, dup, err := p.Wait(tctx(t, 20*time.Second))
		if err != nil || dup {
			t.Fatalf("append %d: seq %d dup %v err %v", i, seq, dup, err)
		}

		if seq <= last {
			t.Fatalf("append %d got seq %d after %d", i, seq, last)
		}

		last = seq
	}

	got := col.waitFor(t, n)
	for i, m := range got[:n] {
		if string(m.Payload) != fmt.Sprint(i) {
			t.Fatalf("message %d is %q", i, m.Payload)
		}
	}

	if _, err := l.AppendAsync(tctx(t, time.Second), LogMessage{Tenant: testTenant, Topic: "x", QoS: 3}, ""); !errors.Is(err, tether.ErrInvalidInput) {
		t.Fatalf("qos 3: %v", err)
	}
}

// Get reads one message by sequence, refuses another tenant's and a
// sequence that holds nothing.
func TestLogGet(t *testing.T) {
	logs := startLogs(t, 2)

	seq := mustAppend(t, logs[0], LogMessage{Tenant: "t1", Topic: "a/b", Payload: []byte("x"), QoS: 2, Props: Props{ContentType: "c"}})
	other := mustAppend(t, logs[0], LogMessage{Tenant: "t2", Topic: "a/b", Payload: []byte("y")})

	m, err := logs[1].Get(tctx(t, 10*time.Second), "t1", seq)
	if err != nil {
		t.Fatal(err)
	}

	if m.Seq != seq || m.Topic != "a/b" || string(m.Payload) != "x" || m.QoS != 2 || m.Props.ContentType != "c" {
		t.Fatalf("got %+v", m)
	}

	if _, err := logs[1].Get(tctx(t, 10*time.Second), "t1", other); !errors.Is(err, tether.ErrNotFound) {
		t.Fatalf("another tenant's: %v", err)
	}

	if _, err := logs[1].Get(tctx(t, 10*time.Second), "t1", other+100); !errors.Is(err, tether.ErrNotFound) {
		t.Fatalf("past the end: %v", err)
	}

	if _, err := logs[1].Get(tctx(t, 10*time.Second), "", seq); !errors.Is(err, tether.ErrInvalidInput) {
		t.Fatalf("empty tenant: %v", err)
	}
}
