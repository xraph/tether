package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/subject"
)

// startCellConns starts a three-server JetStream cluster, provisions a cell on
// it and returns one connection per node, node i on server i%3, with the
// cell's config.
func startCellConns(t *testing.T, nodes int) ([]*Conn, Config) {
	t.Helper()

	urls := clustertest.Start(t, 3)
	out := make([]*Conn, 0, nodes)

	var cfg Config

	for i := range nodes {
		cfg = DefaultConfig()
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
		}

		out = append(out, c)
	}

	return out, cfg
}

func startRetained(t *testing.T, nodes int) []*Retained {
	t.Helper()

	conns, cfg := startCellConns(t, nodes)
	out := make([]*Retained, 0, nodes)

	for _, c := range conns {
		out = append(out, NewRetained(c, cfg))
	}

	return out
}

// retEvent is one call of a Watch callback.
type retEvent struct {
	topic   string
	msg     LogMessage
	deleted bool
	at      time.Time
}

// retWatcher collects what a Watch reports.
type retWatcher struct {
	mu     sync.Mutex
	events []retEvent
	ch     chan retEvent
}

func newRetWatcher() *retWatcher { return &retWatcher{ch: make(chan retEvent, 256)} }

func (w *retWatcher) fn(topic string, msg *LogMessage, deleted bool) {
	ev := retEvent{topic: topic, msg: *msg, deleted: deleted, at: time.Now()}

	w.mu.Lock()
	w.events = append(w.events, ev)
	w.mu.Unlock()

	w.ch <- ev
}

func (w *retWatcher) snapshot() []retEvent {
	w.mu.Lock()
	defer w.mu.Unlock()

	return slices.Clone(w.events)
}

func (w *retWatcher) next(t *testing.T, d time.Duration) retEvent {
	t.Helper()

	select {
	case ev := <-w.ch:
		return ev
	case <-time.After(d):
		t.Fatalf("no retained event within %v", d)

		return retEvent{}
	}
}

func (w *retWatcher) quiet(t *testing.T, d time.Duration) {
	t.Helper()

	select {
	case ev := <-w.ch:
		t.Fatalf("unexpected retained event: %+v", ev)
	case <-time.After(d):
	}
}

func mustWatch(t *testing.T, r *Retained, tenant string, w *retWatcher) {
	t.Helper()

	h, err := r.Watch(tctx(t, 60*time.Second), tenant, w.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(h.Stop)
}

func mustPut(t *testing.T, r *Retained, tenant, topic, payload string) {
	t.Helper()

	if err := r.Put(tctx(t, 20*time.Second), tenant, topic, LogMessage{Payload: []byte(payload), QoS: 1}, 0); err != nil {
		t.Fatal(err)
	}
}

func mustLoad(t *testing.T, r *Retained, tenant string) []LogMessage {
	t.Helper()

	ms, err := r.Load(tctx(t, 20*time.Second), tenant)
	if err != nil {
		t.Fatal(err)
	}

	return ms
}

func topicsOf(ms []LogMessage) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Topic+"="+string(m.Payload))
	}

	return out
}

func TestRetainedRoundTrip(t *testing.T) {
	cell := startRetained(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	zero := byte(0)
	at := time.Date(2026, 10, 26, 12, 0, 0, 123456789, time.UTC)
	msg := LogMessage{
		Payload: []byte("on\r\n\x00\xff"),
		QoS:     2,
		Props: Props{
			User:            []UserProperty{{"k", "1"}, {"a", "x"}, {"k", "2"}, {"k", "1"}},
			ContentType:     "text/plain",
			ResponseTopic:   "reply/here",
			CorrelationData: []byte{0, 1, 2},
			MessageExpiry:   60,
			PayloadFormat:   &zero,
		},
		Origin:      "n1",
		ClientID:    "dev 1/ü",
		PublishedAt: at,
		// Ignored by Put.
		Seq: 99, Tenant: "other", Topic: "other", Retain: false,
	}

	if err := a.Put(ctx, testTenant, "home/lamp", msg, 0); err != nil {
		t.Fatal(err)
	}

	got := mustLoad(t, b, testTenant)
	if len(got) != 1 {
		t.Fatalf("Load = %d messages, want 1", len(got))
	}

	if got[0].Seq == 0 {
		t.Fatal("Load gave Seq 0, want the key's revision")
	}

	want := msg
	want.Seq, want.Tenant, want.Topic, want.Retain = got[0].Seq, testTenant, "home/lamp", true

	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("Load =\n%+v\nwant\n%+v", got[0], want)
	}

	// A second put replaces the first, with a higher revision.
	mustPut(t, a, testTenant, "home/lamp", "off")

	again := mustLoad(t, b, testTenant)
	if len(again) != 1 || string(again[0].Payload) != "off" || again[0].Seq <= got[0].Seq {
		t.Fatalf("after a second put Load = %+v, want one message \"off\" with a higher Seq than %d", again, got[0].Seq)
	}

	if !again[0].Props.IsZero() || again[0].PublishedAt.IsZero() {
		t.Fatalf("second put kept props %+v or lost the publish time", again[0].Props)
	}

	// An empty payload deletes.
	if err := a.Put(ctx, testTenant, "home/lamp", LogMessage{QoS: 1}, 0); err != nil {
		t.Fatal(err)
	}

	if ms := mustLoad(t, b, testTenant); len(ms) != 0 {
		t.Fatalf("after an empty put Load = %v, want nothing", topicsOf(ms))
	}
}

func TestRetainedDelete(t *testing.T) {
	cell := startRetained(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	mustPut(t, a, testTenant, "a/1", "x")
	mustPut(t, a, testTenant, "a/2", "y")

	w := newRetWatcher()
	mustWatch(t, b, testTenant, w)

	// Watch reported the existing messages before it returned.
	initial := w.snapshot()
	if len(initial) != 2 {
		t.Fatalf("first pass reported %d events, want 2: %+v", len(initial), initial)
	}

	for range 2 {
		w.next(t, time.Second)
	}

	if err := a.Delete(ctx, testTenant, "a/1"); err != nil {
		t.Fatal(err)
	}

	ev := w.next(t, 5*time.Second)
	if !ev.deleted || ev.topic != "a/1" || ev.msg.Topic != "a/1" || ev.msg.Tenant != testTenant || ev.msg.Seq == 0 || ev.msg.Payload != nil {
		t.Fatalf("event = %+v, want a deletion of a/1 with its revision", ev)
	}

	if got := topicsOf(mustLoad(t, b, testTenant)); !slices.Equal(got, []string{"a/2=y"}) {
		t.Fatalf("Load after delete = %v", got)
	}

	// Deleting what is not there is fine, and Watch says nothing about it.
	if err := a.Delete(ctx, testTenant, "a/1"); err != nil {
		t.Fatalf("second delete: %v", err)
	}

	if err := a.Delete(ctx, testTenant, "never/set"); err != nil {
		t.Fatalf("delete of an unset topic: %v", err)
	}

	w.quiet(t, 500*time.Millisecond)

	// A topic can be set again after a delete.
	mustPut(t, a, testTenant, "a/1", "z")

	if ev := w.next(t, 5*time.Second); ev.deleted || ev.topic != "a/1" || string(ev.msg.Payload) != "z" {
		t.Fatalf("event = %+v, want a put of a/1", ev)
	}
}

func TestRetainedExpiry(t *testing.T) {
	cell := startRetained(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	w := newRetWatcher()
	mustWatch(t, b, testTenant, w)

	msg := LogMessage{Payload: []byte("soon gone"), QoS: 1, Props: Props{MessageExpiry: 2}}
	if err := a.Put(ctx, testTenant, "short/lived", msg, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	put := time.Now()
	mustPut(t, a, testTenant, "long/lived", "stays")

	if ev := w.next(t, 5*time.Second); ev.deleted || ev.topic != "short/lived" || ev.msg.Props.MessageExpiry != 2 {
		t.Fatalf("first event = %+v, want the put of short/lived", ev)
	}

	w.next(t, 5*time.Second) // long/lived

	ev := w.next(t, 10*time.Second)
	if !ev.deleted || ev.topic != "short/lived" {
		t.Fatalf("event = %+v, want the expiry of short/lived", ev)
	}

	if d := ev.at.Sub(put); d < time.Second || d > 3500*time.Millisecond {
		t.Fatalf("expiry reported %v after the put, want about 2 s", d)
	} else {
		t.Logf("expiry reported %v after the put", d.Round(10*time.Millisecond))
	}

	if got := topicsOf(mustLoad(t, a, testTenant)); !slices.Equal(got, []string{"long/lived=stays"}) {
		t.Fatalf("Load after expiry = %v", got)
	}

	w.quiet(t, 500*time.Millisecond)
}

func TestRetainedTenantIsolation(t *testing.T) {
	cell := startRetained(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	const t1, t2 = "ttnt_one", "ttnt_two"

	w1 := newRetWatcher()
	mustWatch(t, b, t1, w1)

	mustPut(t, a, t1, "same/topic", "one")
	mustPut(t, b, t2, "same/topic", "two")
	mustPut(t, b, t2, "$SYS/x", "two-sys")

	if ev := w1.next(t, 5*time.Second); ev.msg.Tenant != t1 || string(ev.msg.Payload) != "one" {
		t.Fatalf("t1 watch got %+v", ev)
	}

	if err := a.Delete(ctx, t2, "same/topic"); err != nil {
		t.Fatal(err)
	}

	w1.quiet(t, 500*time.Millisecond)

	if got := topicsOf(mustLoad(t, a, t1)); !slices.Equal(got, []string{"same/topic=one"}) {
		t.Fatalf("Load(t1) = %v", got)
	}

	if got := topicsOf(mustLoad(t, a, t2)); !slices.Equal(got, []string{"$SYS/x=two-sys"}) {
		t.Fatalf("Load(t2) = %v", got)
	}

	// A tenant whose encoded token is a prefix of another's sees only its own.
	mustPut(t, a, "ttnt_one_more", "same/topic", "more")

	if got := topicsOf(mustLoad(t, a, t1)); !slices.Equal(got, []string{"same/topic=one"}) {
		t.Fatalf("Load(t1) with a longer tenant around = %v", got)
	}

	w1.quiet(t, 300*time.Millisecond)
}

func TestRetainedDollarKeys(t *testing.T) {
	cell := startRetained(t, 1)
	r := cell[0]
	ctx := tctx(t, 60*time.Second)

	w := newRetWatcher()
	mustWatch(t, r, testTenant, w)

	mustPut(t, r, testTenant, "$SYS/stats/clients", "3")
	mustPut(t, r, testTenant, "plain/topic", "p")

	sysKey := subject.RetainedKey(testTenant, "$SYS/stats/clients")
	if !strings.HasPrefix(sysKey, "==") {
		t.Fatalf("$SYS key %q is not in the == space", sysKey)
	}

	h, err := r.handles(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// What an ordinary "#" filter of the tenant lists.
	lister, err := h.kv.ListKeysFiltered(ctx, subject.RetainedKeyFilter(testTenant, "#")...)
	if err != nil {
		t.Fatal(err)
	}

	var keys []string
	for k := range lister.Keys() {
		keys = append(keys, k)
	}

	if want := []string{subject.RetainedKey(testTenant, "plain/topic")}; !slices.Equal(keys, want) {
		t.Fatalf("keys under %v = %v, want %v", subject.RetainedKeyFilter(testTenant, "#"), keys, want)
	}

	if got := topicsOf(mustLoad(t, r, testTenant)); !slices.Equal(got, []string{"$SYS/stats/clients=3", "plain/topic=p"}) {
		t.Fatalf("Load = %v, want both spaces", got)
	}

	for _, want := range []string{"$SYS/stats/clients", "plain/topic"} {
		if ev := w.next(t, 5*time.Second); ev.topic != want || ev.deleted {
			t.Fatalf("event = %+v, want a put of %s", ev, want)
		}
	}
}

// A watch whose subscription is closed under it starts again, and its first
// pass reports what changed in the gap and nothing that did not.
func TestRetainedWatchRestarts(t *testing.T) {
	cell := startRetained(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	mustPut(t, a, testTenant, "keep", "k")
	mustPut(t, a, testTenant, "gone", "g")
	mustPut(t, a, testTenant, "moved", "m1")

	var (
		mu  sync.Mutex
		kws []jetstream.KeyWatcher
	)

	b.onWatch = func(kw jetstream.KeyWatcher) {
		mu.Lock()
		kws = append(kws, kw)
		mu.Unlock()
	}

	gap := make(chan struct{})
	resume := make(chan struct{})

	b.beforeRestart = func() {
		close(gap)
		<-resume
	}

	w := newRetWatcher()
	mustWatch(t, b, testTenant, w)

	for range 3 {
		w.next(t, time.Second)
	}

	mu.Lock()
	first := kws[0]
	mu.Unlock()

	// Stop unsubscribes first and then deletes the consumer, which may be
	// gone already (an ordered consumer that reset itself); the subscription
	// is closed either way.
	if err := first.Stop(); err != nil && !errors.Is(err, nats.ErrConsumerNotFound) && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatal(err)
	}

	select {
	case <-gap:
	case <-time.After(10 * time.Second):
		t.Fatal("the watch did not notice its subscription closing")
	}

	if err := a.Delete(ctx, testTenant, "gone"); err != nil {
		t.Fatal(err)
	}

	mustPut(t, a, testTenant, "moved", "m2")
	mustPut(t, a, testTenant, "new", "n")
	close(resume)

	got := map[string]retEvent{}
	for range 3 {
		ev := w.next(t, 10*time.Second)
		got[ev.topic] = ev
	}

	if ev := got["gone"]; !ev.deleted {
		t.Fatalf("gone: %+v, want a deletion", ev)
	}

	if ev := got["moved"]; ev.deleted || string(ev.msg.Payload) != "m2" {
		t.Fatalf("moved: %+v, want a put of m2", ev)
	}

	if ev := got["new"]; ev.deleted || string(ev.msg.Payload) != "n" {
		t.Fatalf("new: %+v, want a put of n", ev)
	}

	// keep did not change, so the resync says nothing about it.
	w.quiet(t, 500*time.Millisecond)

	mu.Lock()
	n := len(kws)
	mu.Unlock()

	if n != 2 {
		t.Fatalf("%d watchers started, want 2", n)
	}

	// The restarted watch goes on reporting.
	mustPut(t, a, testTenant, "keep", "k2")

	if ev := w.next(t, 5*time.Second); ev.topic != "keep" || string(ev.msg.Payload) != "k2" {
		t.Fatalf("event after restart = %+v", ev)
	}
}

// A first pass that misses writes the leader had already acknowledged, as a
// watcher on a lagging replica does, still returns them: Load and Watch read
// on until the watcher reaches the leader's mark. The afterOpen hook writes
// after the watcher took its snapshot and before the mark is read, so without
// the mark both would return the stale snapshot every time. The newest entry
// under the mark is a delete marker, so this also fails if markers are left
// out of the pass.
func TestRetainedFirstPassReachesLeaderMark(t *testing.T) {
	cell := startRetained(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	mustPut(t, a, testTenant, "old/kept", "k")
	mustPut(t, a, testTenant, "old/gone", "g")

	round := 0

	b.afterOpen = func() {
		round++
		mustPut(t, a, testTenant, "late/put", fmt.Sprintf("v%d", round))

		if err := a.Delete(ctx, testTenant, "old/gone"); err != nil {
			t.Error(err)
		}
	}

	got := topicsOf(mustLoad(t, b, testTenant))
	if want := []string{"late/put=v1", "old/kept=k"}; !slices.Equal(got, want) {
		t.Fatalf("Load = %v, want %v", got, want)
	}

	mustPut(t, a, testTenant, "old/gone", "g")

	w := newRetWatcher()
	mustWatch(t, b, testTenant, w)

	state := map[string]string{}
	for _, ev := range w.snapshot() {
		if ev.deleted {
			delete(state, ev.topic)
		} else {
			state[ev.topic] = string(ev.msg.Payload)
		}
	}

	if want := map[string]string{"late/put": "v2", "old/kept": "k"}; !reflect.DeepEqual(state, want) {
		t.Fatalf("state when Watch returned = %v, want %v", state, want)
	}
}

// Writes acknowledged on one node are in a Load on another right away, round
// after round. This is the leader mark at work, not a timing allowance.
func TestRetainedLoadSeesAcknowledgedWrite(t *testing.T) {
	cell := startRetained(t, 3)

	for i := range 40 {
		w, r := cell[i%3], cell[(i+1)%3]
		v := fmt.Sprint(i)

		mustPut(t, w, testTenant, "flip", v)

		if got := topicsOf(mustLoad(t, r, testTenant)); !slices.Equal(got, []string{"flip=" + v}) {
			t.Fatalf("round %d: Load on node %d = %v, want flip=%s", i, (i+1)%3+1, got, v)
		}
	}
}

// The watch's start has a bound of its own: with NATS gone, a Watch whose ctx
// never ends still gives up, with ErrUnavailable, after about 10 s.
func TestRetainedWatchStartBound(t *testing.T) {
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

	if err = Provision(tctx(t, 30*time.Second), c, cfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	r := NewRetained(c, cfg)
	mustPut(t, r, testTenant, "a", "x") // opens the bucket handle while NATS is up

	cl.Stop(0)

	start := time.Now()

	_, err = r.Watch(t.Context(), testTenant, func(string, *LogMessage, bool) {})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Watch = %v, want ErrUnavailable from the start bound", err)
	}

	if d := time.Since(start); d > opTimeout+5*time.Second {
		t.Fatalf("Watch gave up after %v, want at most about %v", d, opTimeout)
	} else {
		t.Logf("Watch gave up after %v", d.Round(10*time.Millisecond))
	}
}

// A restarted watch's first pass reports as deleted a topic the callback
// holds whose key the pass did not read at all (its marker expired in the
// gap), which a live cell takes a minute to show.
func TestRetainedResyncDropsUnseen(t *testing.T) {
	r := &Retained{c: &Conn{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	w := newRetWatcher()
	rw := &retainedWatch{r: r, tenant: testTenant, fn: w.fn, known: map[string]uint64{
		subject.RetainedKey(testTenant, "seen"):   4,
		subject.RetainedKey(testTenant, "unseen"): 5,
	}}

	rw.resync = map[string]struct{}{subject.RetainedKey(testTenant, "seen"): {}}
	rw.endFirstPass()

	evs := w.snapshot()
	if len(evs) != 1 || evs[0].topic != "unseen" || !evs[0].deleted {
		t.Fatalf("events = %+v, want one deletion of unseen", evs)
	}

	if _, ok := rw.known[subject.RetainedKey(testTenant, "unseen")]; ok || len(rw.known) != 1 || rw.resync != nil {
		t.Fatalf("known = %v, resync = %v after the first pass", rw.known, rw.resync)
	}
}

func TestRetainedWatchStop(t *testing.T) {
	cell := startRetained(t, 1)
	r := cell[0]

	w := newRetWatcher()

	h, err := r.Watch(tctx(t, 60*time.Second), testTenant, w.fn)
	if err != nil {
		t.Fatal(err)
	}

	h.Stop()
	h.Stop()

	mustPut(t, r, testTenant, "after/stop", "x")
	w.quiet(t, 500*time.Millisecond)

	// A watch whose context ends stops too.
	ctx, cancel := context.WithCancel(t.Context())

	w2 := newRetWatcher()
	if _, err := r.Watch(ctx, testTenant, w2.fn); err != nil {
		t.Fatal(err)
	}

	w2.next(t, time.Second) // the first pass

	cancel()
	time.Sleep(200 * time.Millisecond)

	mustPut(t, r, testTenant, "after/cancel", "y")
	w2.quiet(t, 500*time.Millisecond)
}

func TestRetainedInvalidInput(t *testing.T) {
	cell := startRetained(t, 1)
	r := cell[0]
	ctx := tctx(t, 30*time.Second)

	long := strings.Repeat("a", subject.MaxTopicBytes+1)
	msg := LogMessage{Payload: []byte("x")}
	nop := func(string, *LogMessage, bool) {}

	cases := map[string]error{
		"put empty tenant":    r.Put(ctx, "", "a", msg, 0),
		"put wildcard +":      r.Put(ctx, testTenant, "a/+", msg, 0),
		"put wildcard #":      r.Put(ctx, testTenant, "a/#", msg, 0),
		"put empty topic":     r.Put(ctx, testTenant, "", msg, 0),
		"put long topic":      r.Put(ctx, testTenant, long, msg, 0),
		"put QoS 3":           r.Put(ctx, testTenant, "a", LogMessage{Payload: []byte("x"), QoS: 3}, 0),
		"empty put QoS 3":     r.Put(ctx, testTenant, "a", LogMessage{QoS: 3}, 0),
		"delete empty tenant": r.Delete(ctx, "", "a"),
		"delete long topic":   r.Delete(ctx, testTenant, long),
		"empty put bad topic": r.Put(ctx, testTenant, "a/#", LogMessage{}, 0),
		"load empty tenant":   second2(r.Load(ctx, "")),
		"watch empty tenant":  second2(r.Watch(ctx, "", nop)),
		"watch nil callback":  second2(r.Watch(ctx, testTenant, nil)),
	}

	for name, err := range cases {
		if !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("%s: err = %v, want ErrInvalidInput", name, err)
		}
	}

	if ms := mustLoad(t, r, testTenant); len(ms) != 0 {
		t.Fatalf("refused writes stored %v", topicsOf(ms))
	}
}

func second2[T any](_ T, err error) error { return err }

// Sync returns only once the watch has been through every write acknowledged
// before it was called: a put on another node, even with the callback slow,
// and removals of topics the callback never held (which it is never told
// about) do not hold it up. On a stopped watch it fails.
func TestRetainedWatchSync(t *testing.T) {
	cell := startRetained(t, 2)
	a, b := cell[0], cell[1]

	var slow atomic.Bool

	w := newRetWatcher()
	h, err := b.Watch(tctx(t, 60*time.Second), testTenant, func(topic string, msg *LogMessage, deleted bool) {
		if slow.Load() {
			time.Sleep(300 * time.Millisecond)
		}

		w.fn(topic, msg, deleted)
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(h.Stop)

	for round := range 5 {
		slow.Store(round%2 == 0)

		name := fmt.Sprintf("sync/%d", round)
		mustPut(t, a, testTenant, name, "v")

		if err := h.Sync(tctx(t, 20*time.Second)); err != nil {
			t.Fatal(err)
		}

		if !slices.ContainsFunc(w.snapshot(), func(ev retEvent) bool { return ev.topic == name && !ev.deleted }) {
			t.Fatalf("round %d: Sync returned before the watch reported %s", round, name)
		}
	}

	slow.Store(false)

	// The newest entries are removals of topics the callback never held:
	// nothing is reported, and Sync still returns.
	for i := range 3 {
		if err := a.Delete(tctx(t, 20*time.Second), testTenant, fmt.Sprintf("never/%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()

	if err := h.Sync(tctx(t, 20*time.Second)); err != nil {
		t.Fatal(err)
	}

	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Sync over unreported removals took %v", d)
	}

	// A delete of a held topic is through the callback once Sync returns.
	if err := a.Delete(tctx(t, 20*time.Second), testTenant, "sync/0"); err != nil {
		t.Fatal(err)
	}

	if err := h.Sync(tctx(t, 20*time.Second)); err != nil {
		t.Fatal(err)
	}

	if !slices.ContainsFunc(w.snapshot(), func(ev retEvent) bool { return ev.topic == "sync/0" && ev.deleted }) {
		t.Fatal("Sync returned before the watch reported the delete of sync/0")
	}

	h.Stop()

	if err := h.Sync(tctx(t, 5*time.Second)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Sync on a stopped watch = %v, want ErrUnavailable", err)
	}
}
