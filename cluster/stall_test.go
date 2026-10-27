package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/subject"
)

// errCodeConsumerOffline is JetStream's "consumer is offline" (10119).
const errCodeConsumerOffline jetstream.ErrorCode = 10119

// leaderKillCell is a three-server cell set up for the chaos finding CHAOS-2:
// node is a node whose own NATS server leads the log stream (its connection
// goes to that server first and fails over to the others), and app appends
// through the other two. Killing the leader makes node reconnect at once and
// its ordered consumers recreate themselves.
type leaderKillCell struct {
	cl         *clustertest.Cluster
	cfg        Config
	leader     int    // the index of the stream leader's server
	leaderName string // its server name
	node, app  *Conn
}

func startLeaderKillCell(t *testing.T) *leaderKillCell {
	t.Helper()

	cl := clustertest.StartCluster(t, 3)
	urls := cl.URLs()

	configFor := func(id string, us []string) Config {
		cfg := DefaultConfig()
		cfg.Enabled = true
		cfg.NodeID = id
		cfg.Cell = "c1"
		cfg.Mode = ModeExternal
		cfg.URLs = us
		cfg.NoDiscovery = true
		cfg.Replicas = 3

		return cfg
	}

	connect := func(id string, us []string) *Conn {
		c, err := Connect(tctx(t, 20*time.Second), configFor(id, us), nil)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(c.Close)

		return c
	}

	cfg := configFor("setup", urls)
	setup := connect("setup", urls)

	if err := Provision(tctx(t, 30*time.Second), setup, cfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	// The kill must leave the meta leader alive: a consumer create needs one,
	// and without it the reset would time out rather than meet an offline
	// consumer. Move the stream leaders off the meta leader when they share
	// a server.
	meta := -1

	eventually(t, 20*time.Second, "a meta leader", func() bool {
		for i, s := range cl.Servers {
			if s.JetStreamIsLeader() {
				meta = i

				return true
			}
		}

		return false
	})

	leaderOf := func(stream string) string {
		var name string

		eventually(t, 20*time.Second, "a leader of "+stream, func() bool {
			st, err := setup.js.Stream(tctx(t, 5*time.Second), stream)
			if err != nil {
				return false
			}

			info, err := st.Info(tctx(t, 5*time.Second))
			if err != nil || info.Cluster == nil || info.Cluster.Leader == "" {
				return false
			}

			name = info.Cluster.Leader

			return true
		})

		return name
	}

	metaName := cl.Servers[meta].Name()

	for range 20 {
		if leaderOf(cfg.LogStream()) != metaName {
			break
		}

		if _, err := setup.nc.Request("$JS.API.STREAM.LEADER.STEPDOWN."+cfg.LogStream(), nil, 5*time.Second); err != nil {
			t.Logf("stepdown: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	}

	logLeader := leaderOf(cfg.LogStream())
	if logLeader == metaName {
		t.Fatal("could not move the log stream leader off the meta leader")
	}

	leader := -1

	for i, s := range cl.Servers {
		if s.Name() == logLeader {
			leader = i
		}
	}

	if leader < 0 {
		t.Fatalf("no server named %s", logLeader)
	}

	others := make([]string, 0, 2)

	for i, u := range urls {
		if i != leader {
			others = append(others, u)
		}
	}

	return &leaderKillCell{
		cl: cl, cfg: cfg, leader: leader, leaderName: logLeader,
		node: connect("n1", append([]string{urls[leader]}, others...)),
		app:  connect("n2", others),
	}
}

// pinNextReset places the consumer an ordered consumer will create at its
// next reset on the stream leader's server, before that server dies, the way
// the meta layer placed it in the chaos run (it created it 4 ms after the
// kill, before it knew the server was gone). cur is the ordered consumer's
// current server consumer name ("<prefix>_<serial>"), cfg the configuration
// nats.go will ask for. It returns the pinned name.
func (lk *leaderKillCell) pinNextReset(t *testing.T, stream, cur string, cfg jetstream.ConsumerConfig) string {
	t.Helper()

	i := strings.LastIndexByte(cur, '_')
	serial, err := strconv.Atoi(cur[i+1:])

	if i < 0 || err != nil {
		t.Fatalf("consumer name %q has no serial", cur)
	}

	cfg.Name = fmt.Sprintf("%s_%d", cur[:i], serial+1)

	st, err := lk.app.js.Stream(tctx(t, 5*time.Second), stream)
	if err != nil {
		t.Fatal(err)
	}

	for range 60 {
		cons, err := st.CreateOrUpdateConsumer(tctx(t, 10*time.Second), cfg)
		if err != nil {
			t.Fatal(err)
		}

		info, err := cons.Info(tctx(t, 5*time.Second))
		if err == nil && info.Cluster != nil && info.Cluster.Leader == lk.leaderName {
			return cfg.Name
		}

		if err := st.DeleteConsumer(tctx(t, 5*time.Second), cfg.Name); err != nil {
			t.Fatal(err)
		}
	}

	t.Fatalf("consumer %s never landed on %s", cfg.Name, lk.leaderName)

	return ""
}

// kill stops the leader's server and checks the pinned consumer now answers
// "consumer is offline" (10119), the shape the chaos run found.
func (lk *leaderKillCell) kill(t *testing.T, stream, pinned string) {
	t.Helper()

	lk.cl.Stop(lk.leader)

	eventually(t, 20*time.Second, "the pinned consumer to be offline", func() bool {
		_, err := lk.app.js.Consumer(tctx(t, 5*time.Second), stream, pinned)

		var apiErr *jetstream.APIError

		return errors.As(err, &apiErr) && apiErr.ErrorCode == errCodeConsumerOffline
	})
}

// readerConfig is the configuration nats.go asks for when it resets a log
// reader of tenant that has delivered up to stream sequence pos.
func readerConfig(tenant string, pos uint64) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       pos + 1,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: readerInactive,
		Replicas:          1,
		MemoryStorage:     true,
		FilterSubject:     subject.LogTenantAll(tenant),
	}
}

// appendDuring appends n messages through app, waiting through the stream's
// leader election, and returns their sequences.
func appendDuring(t *testing.T, app *Log, n int) []uint64 {
	t.Helper()

	out := make([]uint64, 0, n)

	for i := range n {
		seq, _, err := app.Append(tctx(t, 30*time.Second), LogMessage{Tenant: testTenant, Topic: fmt.Sprintf("after/%d", i), QoS: 1}, "")
		if err != nil {
			t.Fatalf("append %d after the kill: %v", i, err)
		}

		out = append(out, seq)
	}

	return out
}

// CHAOS-2. A reader whose consumer reset meets a consumer placed on the dead
// stream leader gets "consumer is offline" from every create. With nats.go's
// unlimited reset attempts it retried that forever inside Next, so the reader
// never ended, never delivered again and logged nothing. With one attempt the
// reader ends, Done closes, and a reader started from Position()+1 delivers
// every message appended through the survivors, gap-free and in order.
func TestReaderEndsOnOfflineConsumerAndResumes(t *testing.T) {
	lk := startLeaderKillCell(t)
	node, app := NewLog(lk.node, lk.cfg), NewLog(lk.app, lk.cfg)
	node.heartbeat = 500 * time.Millisecond

	before := make([]uint64, 0, 20)
	for i := range 20 {
		before = append(before, mustAppend(t, app, LogMessage{Tenant: testTenant, Topic: fmt.Sprintf("before/%d", i)}))
	}

	col := newCollector()

	r, err := node.Reader(testTenant, before[0], col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	col.waitFor(t, len(before))

	pinned := lk.pinNextReset(t, lk.cfg.LogStream(), r.cons.CachedInfo().Name, readerConfig(testTenant, before[len(before)-1]))
	killed := time.Now()

	lk.kill(t, lk.cfg.LogStream(), pinned)

	after := appendDuring(t, app, 50)

	select {
	case <-r.Done():
		t.Logf("reader ended %s after the kill", time.Since(killed).Round(time.Millisecond))
	case <-time.After(30 * time.Second):
		t.Fatalf("the reader did not end within 30 s of the kill; it delivered %d of %d", len(col.snapshot()), len(before)+len(after))
	}

	if got := r.Position(); got > after[len(after)-1] {
		t.Fatalf("position %d past the last append %d", got, after[len(after)-1])
	}

	r2, err := node.Reader(testTenant, r.Position()+1, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r2.Stop()

	want := append(slices.Clone(before), after...)

	got := seqs(col.waitFor(t, len(want)))
	if !slices.Equal(got, want) {
		t.Fatalf("delivered %v,\nwant %v", got, want)
	}

	t.Logf("all %d messages delivered %s after the kill", len(want), time.Since(killed).Round(time.Millisecond))
}

// The stall watch is the second line: with nats.go's endless reset loop put
// back (resetAttempts -1), the same offline consumer stalls the reader
// exactly as in the chaos run (nothing more delivered, Done open), until the
// watch sees the log holding messages past the reader's position with nothing
// delivered for stallAfter, and ends it.
func TestStallWatchEndsReaderStuckInResetLoop(t *testing.T) {
	const stallAfter = 3 * time.Second

	lk := startLeaderKillCell(t)
	node, app := NewLog(lk.node, lk.cfg), NewLog(lk.app, lk.cfg)
	node.heartbeat = 500 * time.Millisecond
	node.resetAttempts = -1
	node.stallAfter = stallAfter

	before := make([]uint64, 0, 10)
	for i := range 10 {
		before = append(before, mustAppend(t, app, LogMessage{Tenant: testTenant, Topic: fmt.Sprintf("before/%d", i)}))
	}

	col := newCollector()

	r, err := node.Reader(testTenant, before[0], col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Stop()

	col.waitFor(t, len(before))

	pinned := lk.pinNextReset(t, lk.cfg.LogStream(), r.cons.CachedInfo().Name, readerConfig(testTenant, before[len(before)-1]))
	killed := time.Now()

	lk.kill(t, lk.cfg.LogStream(), pinned)

	after := appendDuring(t, app, 30)
	appended := time.Now()

	select {
	case <-r.Done():
		// The watch needs stallAfter of silence after the last delivery,
		// which came before the kill.
		if quiet := time.Since(killed); quiet < stallAfter-time.Second {
			t.Fatalf("the reader ended %s after the kill, before the stall bound; not the watch", quiet)
		}
	case <-time.After(stallAfter + 20*time.Second):
		t.Fatalf("the stall watch did not end the reader; it delivered %d of %d", len(col.snapshot()), len(before)+len(after))
	}

	// The stall itself: nothing appended after the kill reached the reader.
	if n := len(col.snapshot()); n != len(before) {
		t.Fatalf("the reader delivered %d messages before it was ended, want the %d from before the kill (the reset loop should have stalled it)", n, len(before))
	}

	if r.Position() != before[len(before)-1] {
		t.Fatalf("position %d, want %d", r.Position(), before[len(before)-1])
	}

	t.Logf("stalled reader ended %s after the kill, %s after the appends", time.Since(killed).Round(time.Millisecond), time.Since(appended).Round(time.Millisecond))

	r2, err := node.Reader(testTenant, r.Position()+1, col.fn)
	if err != nil {
		t.Fatal(err)
	}

	defer r2.Stop()

	want := append(slices.Clone(before), after...)
	if got := seqs(col.waitFor(t, len(want))); !slices.Equal(got, want) {
		t.Fatalf("delivered %v,\nwant %v", got, want)
	}
}

// The control bus shares the reader's construction. Its consumer, reset
// against the dead leader the same way, ends and is replaced from the event
// after the last one handled: every event published through the survivors is
// handled once and in order.
func TestControlReplacesConsumerOnOfflineReset(t *testing.T) {
	lk := startLeaderKillCell(t)

	bus := NewControl(lk.node, lk.cfg)
	bus.heartbeat = 500 * time.Millisecond

	var (
		mu  sync.Mutex
		got []string
	)

	if err := bus.Subscribe(KindTenantUpdated, func(data []byte) {
		mu.Lock()
		got = append(got, string(data))
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}

	if err := bus.Start(tctx(t, 10*time.Second)); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(bus.Stop)

	pub := NewControl(lk.app, lk.cfg)

	var want []string

	publish := func(i int) {
		ev := TenantUpdated{TenantID: fmt.Sprintf("t%d", i)}

		eventually(t, 30*time.Second, "publish "+ev.TenantID, func() bool {
			return pub.Publish(tctx(t, 5*time.Second), KindTenantUpdated, ev) == nil
		})

		want = append(want, fmt.Sprintf(`{"tenant_id":"t%d"}`, i))
	}

	handled := func() []string {
		mu.Lock()
		defer mu.Unlock()

		return slices.Clone(got)
	}

	for i := range 5 {
		publish(i)
	}

	eventually(t, 20*time.Second, "the first events", func() bool { return len(handled()) == 5 })

	bus.mu.Lock()
	run := bus.run
	bus.mu.Unlock()

	pinned := lk.pinNextReset(t, lk.cfg.ControlStream(), run.cons.CachedInfo().Name, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       run.last.Load() + 1,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: readerInactive,
		Replicas:          1,
		MemoryStorage:     true,
		FilterSubject:     ctlPrefix + ">",
	})

	lk.kill(t, lk.cfg.ControlStream(), pinned)

	for i := 5; i < 25; i++ {
		publish(i)
	}

	eventually(t, 40*time.Second, "every event after the kill", func() bool { return len(handled()) >= len(want) })

	time.Sleep(500 * time.Millisecond)

	if h := handled(); !slices.Equal(h, want) {
		t.Fatalf("handled %v,\nwant %v", h, want)
	}

	bus.mu.Lock()
	replaced := bus.run != run
	bus.mu.Unlock()

	if !replaced {
		t.Fatal("the bus still runs its first consumer")
	}
}

// After an outage the stall watch gives the loop a whole bound from the
// moment the stream answers again. Here nothing is ever delivered and the
// stream holds more throughout; behind fails for three bounds, then answers.
// The stall verdict must come at least a bound (less one jittered look)
// after that first answer. Counting a failed read as progress alone was not
// enough: the read before the outage ended could be a bound old, and the
// first answer then ended the loop at once.
func TestStallWatchGivesABoundAfterAnOutage(t *testing.T) {
	const after = 400 * time.Millisecond

	w := newStallWatch(after)
	w.progress()

	var (
		down     atomic.Bool
		answered atomic.Int64
	)

	down.Store(true)

	stalledAt := make(chan time.Time, 1)
	stop, done := make(chan struct{}), make(chan struct{})

	defer close(stop)

	go w.run(stop, done, func(context.Context) (uint64, uint64, error) {
		if down.Load() {
			return 0, 0, errors.New("no responders")
		}

		answered.CompareAndSwap(0, time.Now().UnixNano())

		return 10, 5, nil
	}, func(uint64, uint64) { stalledAt <- time.Now() })

	time.Sleep(3 * after)
	down.Store(false)

	select {
	case at := <-stalledAt:
		got := at.Sub(time.Unix(0, answered.Load()))
		if floor := after - after*5/16; got < floor {
			t.Fatalf("stalled %s after the stream answered again, want at least %s", got, floor)
		}

		t.Logf("stalled %s after the stream answered again (bound %s)", got.Round(time.Millisecond), after)
	case <-time.After(5 * after):
		t.Fatal("the watch never ended a loop that stayed behind")
	}
}
