package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster/clustertest"
)

// startLoads starts a three-server cell and returns one Load per node, node i
// on server i%3, each reporting every interval.
func startLoads(t *testing.T, nodes int, interval time.Duration) []*Load {
	t.Helper()

	conns, cfg := startCellConns(t, nodes)
	cfg.LoadInterval = interval

	out := make([]*Load, 0, nodes)

	for i, c := range conns {
		nc := cfg
		nc.NodeID = fmt.Sprintf("n%d", i+1)

		out = append(out, NewLoad(c, nc))
	}

	return out
}

// loadFeed collects what a Load watch tells its callback.
type loadFeed struct {
	mu      sync.Mutex
	reports map[string]*NodeLoad
	deletes map[string]int
	calls   int
}

func newLoadFeed() *loadFeed {
	return &loadFeed{reports: make(map[string]*NodeLoad), deletes: make(map[string]int)}
}

func (f *loadFeed) fn(id string, r *NodeLoad, deleted bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++

	if deleted {
		delete(f.reports, id)

		f.deletes[id]++

		return
	}

	f.reports[id] = r
}

func (f *loadFeed) report(id string) *NodeLoad {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.reports[id]
}

func (f *loadFeed) deleted(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.deletes[id]
}

func (f *loadFeed) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

// waitN1 waits until the feed holds a report from n1 that ok accepts.
func (f *loadFeed) waitN1(t *testing.T, d time.Duration, ok func(*NodeLoad) bool) *NodeLoad {
	t.Helper()

	var got *NodeLoad

	waitFor(t, "a report from n1", d, func() bool {
		got = f.report("n1")

		return got != nil && ok(got)
	})

	return got
}

func anyReport(*NodeLoad) bool { return true }

// sampleLoad is a report of node id with two tenants, every field set. gen
// makes two reports differ.
func sampleLoad(id string, gen uint64) *NodeLoad {
	return &NodeLoad{
		NodeID:    id,
		StartedAt: time.Date(2026, 11, 3, 8, 30, 0, 123456789, time.UTC),
		At:        time.Date(2026, 11, 3, 9, 0, 0, 5000000, time.UTC).Add(time.Duration(gen) * time.Second),
		Tenants: map[string]TenantLoad{
			"acme": {
				Connections: 12, MessagesIn: 1000 + gen, MessagesOut: 2000 + gen,
				BytesIn: 1 << 33, BytesOut: 1<<40 + gen, ConnectionMillis: 987654321,
			},
			"globex": {Connections: 3, MessagesIn: 7, MessagesOut: 9, BytesIn: 11, BytesOut: 13, ConnectionMillis: 15},
		},
	}
}

func sameLoad(t *testing.T, got, want *NodeLoad) {
	t.Helper()

	if got.NodeID != want.NodeID || !got.StartedAt.Equal(want.StartedAt) || !got.At.Equal(want.At) ||
		!reflect.DeepEqual(got.Tenants, want.Tenants) {
		t.Fatalf("report = %+v, want %+v", got, want)
	}
}

func TestLoadRoundTrip(t *testing.T) {
	loads := startLoads(t, 2, 2*time.Second)
	a, b := loads[0], loads[1]

	feed := newLoadFeed()

	stop, err := b.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	want := sampleLoad("n1", 0)
	if err := a.Report(tctx(t, 10*time.Second), want); err != nil {
		t.Fatal(err)
	}

	got := feed.waitN1(t, 10*time.Second, anyReport)
	sameLoad(t, got, want)

	// The next report replaces the last, and the watch hands it over whole.
	next := sampleLoad("n1", 1)
	if err := a.Report(tctx(t, 10*time.Second), next); err != nil {
		t.Fatal(err)
	}

	got = feed.waitN1(t, 10*time.Second, func(r *NodeLoad) bool { return r.At.Equal(next.At) })
	sameLoad(t, got, next)

	if n := feed.deleted("n1"); n != 0 {
		t.Fatalf("%d removals reported for a node that is reporting", n)
	}
}

func TestLoadReportFillsNodeAndTime(t *testing.T) {
	loads := startLoads(t, 2, 2*time.Second)
	a, b := loads[0], loads[1]

	feed := newLoadFeed()

	stop, err := b.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	before := time.Now().Add(-time.Second)

	r := &NodeLoad{StartedAt: before}
	if err := a.Report(tctx(t, 10*time.Second), r); err != nil {
		t.Fatal(err)
	}

	if r.NodeID != "" || !r.At.IsZero() {
		t.Fatalf("Report changed the caller's report: %+v", r)
	}

	got := feed.waitN1(t, 10*time.Second, anyReport)
	if got.NodeID != "n1" || got.At.Before(before) || time.Since(got.At) > time.Minute {
		t.Fatalf("report = %+v, want node n1 stamped now", got)
	}
}

func TestLoadReportRefusesAnotherNodesReport(t *testing.T) {
	a := startLoads(t, 1, time.Second)[0]

	for name, r := range map[string]*NodeLoad{"nil": nil, "another node": {NodeID: "n2"}} {
		err := a.Report(tctx(t, 5*time.Second), r)
		if !errors.Is(err, tether.ErrInvalidInput) {
			t.Fatalf("%s: err = %v, want tether.ErrInvalidInput", name, err)
		}
	}
}

func TestLoadExpiresWhenNodeStops(t *testing.T) {
	loads := startLoads(t, 2, time.Second)
	a, b := loads[0], loads[1]

	feed := newLoadFeed()

	stop, err := b.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	if err := a.Report(tctx(t, 10*time.Second), sampleLoad("n1", 0)); err != nil {
		t.Fatal(err)
	}

	start := time.Now()

	feed.waitN1(t, 10*time.Second, anyReport)

	// A stops reporting. Its key lives 3 x LoadInterval, then JetStream
	// removes it and the watch says so.
	waitFor(t, "removal of the silent node's report", 8*time.Second, func() bool { return feed.deleted("n1") > 0 })

	elapsed := time.Since(start)
	t.Logf("report removed %s after it was written", elapsed.Round(10*time.Millisecond))

	if elapsed < 2*time.Second {
		t.Fatalf("report removed after %s, before its 3 s lifetime", elapsed)
	}

	if feed.report("n1") != nil {
		t.Fatal("the callback still holds the expired report")
	}
}

func TestLoadRefreshKeepsKey(t *testing.T) {
	loads := startLoads(t, 2, time.Second)
	a, b := loads[0], loads[1]

	feed := newLoadFeed()

	stop, err := b.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	// Reports 1.5 s apart against a 3 s lifetime: each one pushes the expiry
	// out, so the key outlives several lifetimes.
	for i := range uint64(4) {
		if err := a.Report(tctx(t, 10*time.Second), sampleLoad("n1", i)); err != nil {
			t.Fatal(err)
		}

		time.Sleep(1500 * time.Millisecond)
	}

	if n := feed.deleted("n1"); n != 0 {
		t.Fatalf("%d removals while the node kept reporting", n)
	}

	if feed.report("n1") == nil {
		t.Fatal("no report held")
	}
}

func TestLoadWatchFirstPass(t *testing.T) {
	loads := startLoads(t, 3, 2*time.Second)

	reports := map[string]*NodeLoad{"n1": sampleLoad("n1", 1), "n2": sampleLoad("n2", 2)}

	for i, id := range []string{"n1", "n2"} {
		if err := loads[i].Report(tctx(t, 10*time.Second), reports[id]); err != nil {
			t.Fatal(err)
		}
	}

	feed := newLoadFeed()

	stop, err := loads[2].Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	// No waiting: both are in before Watch returned.
	for id, want := range reports {
		got := feed.report(id)
		if got == nil {
			t.Fatalf("no report from %s when Watch returned", id)
		}

		sameLoad(t, got, want)
	}
}

func TestLoadWatchFirstPassEmpty(t *testing.T) {
	l := startLoads(t, 1, 2*time.Second)[0]
	feed := newLoadFeed()

	stop, err := l.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	if n := feed.callCount(); n != 0 {
		t.Fatalf("%d callbacks from an empty bucket", n)
	}
}

func TestLoadWatchReportsDelete(t *testing.T) {
	loads := startLoads(t, 2, time.Minute)
	a, b := loads[0], loads[1]

	feed := newLoadFeed()

	stop, err := b.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	if rerr := a.Report(tctx(t, 10*time.Second), sampleLoad("n1", 0)); rerr != nil {
		t.Fatal(rerr)
	}

	feed.waitN1(t, 10*time.Second, anyReport)

	kv, err := b.c.js.KeyValue(tctx(t, 10*time.Second), b.bucket)
	if err != nil {
		t.Fatal(err)
	}

	if err := kv.Delete(tctx(t, 10*time.Second), "n1"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "removal after a delete", 10*time.Second, func() bool { return feed.deleted("n1") == 1 })

	// A removal of a node the callback never held says nothing.
	if err := kv.Delete(tctx(t, 10*time.Second), "ghost"); err != nil {
		t.Fatal(err)
	}

	if err := a.Report(tctx(t, 10*time.Second), sampleLoad("n1", 1)); err != nil {
		t.Fatal(err)
	}

	feed.waitN1(t, 10*time.Second, anyReport)

	if n := feed.deleted("ghost"); n != 0 {
		t.Fatalf("%d removals reported for a node never held", n)
	}
}

func TestLoadWatchStop(t *testing.T) {
	loads := startLoads(t, 2, 2*time.Second)
	a, b := loads[0], loads[1]

	feed := newLoadFeed()

	stop, err := b.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	stop()
	stop() // idempotent

	calls := feed.callCount()

	if err := a.Report(tctx(t, 10*time.Second), sampleLoad("n1", 0)); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	if got := feed.callCount(); got != calls {
		t.Fatalf("callback ran %d more times after stop", got-calls)
	}
}

func TestLoadWatchRestartsWhenClosed(t *testing.T) {
	loads := startLoads(t, 2, 2*time.Second)
	a, b := loads[0], loads[1]

	var (
		mu       sync.Mutex
		watchers []jetstream.KeyWatcher
	)

	b.onWatch = func(w jetstream.KeyWatcher) {
		mu.Lock()
		defer mu.Unlock()

		watchers = append(watchers, w)
	}

	opened := func() int {
		mu.Lock()
		defer mu.Unlock()

		return len(watchers)
	}

	feed := newLoadFeed()

	stop, err := b.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	if err := a.Report(tctx(t, 10*time.Second), sampleLoad("n1", 0)); err != nil {
		t.Fatal(err)
	}

	feed.waitN1(t, 10*time.Second, anyReport)

	// Close the watcher under the watch (nats.go does this to one it cannot
	// recreate): Updates closes, and the watch must open another.
	mu.Lock()
	first := watchers[0]
	mu.Unlock()

	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "second watcher", 15*time.Second, func() bool { return opened() >= 2 })

	next := sampleLoad("n1", 5)
	if err := a.Report(tctx(t, 10*time.Second), next); err != nil {
		t.Fatal(err)
	}

	got := feed.waitN1(t, 15*time.Second, func(r *NodeLoad) bool { return r.At.Equal(next.At) })
	sameLoad(t, got, next)

	if n := feed.deleted("n1"); n != 0 {
		t.Fatalf("%d removals across the restart", n)
	}
}

func TestLoadWatchBadArguments(t *testing.T) {
	l := startLoads(t, 1, 2*time.Second)[0]

	if _, err := l.Watch(tctx(t, 5*time.Second), nil); !errors.Is(err, tether.ErrInvalidInput) {
		t.Fatalf("nil callback: err = %v, want tether.ErrInvalidInput", err)
	}
}

// loadOfSize is a report of node id whose JSON is exactly size bytes.
func loadOfSize(t *testing.T, id string, size int) *NodeLoad {
	t.Helper()

	r := sampleLoad(id, 0)
	r.Tenants = map[string]TenantLoad{"x": {Connections: 1}}

	base, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}

	if size <= len(base) {
		t.Fatalf("size %d is under the %d bytes of the smallest report", size, len(base))
	}

	r.Tenants = map[string]TenantLoad{"x" + strings.Repeat("t", size-len(base)): {Connections: 1}}

	if data, _ := json.Marshal(r); len(data) != size {
		t.Fatalf("built %d bytes, want %d", len(data), size)
	}

	return r
}

func TestLoadTooLarge(t *testing.T) {
	l := startLoads(t, 1, 2*time.Second)[0]
	limit := int(l.c.MaxPayload())

	// The 1 KiB the limit keeps back is for the headers: a report that would
	// fit the server is refused before it is sent.
	for _, size := range []int{limit - loadHeaderRoom + 1, limit + 1, 2 * limit} {
		err := l.Report(tctx(t, 10*time.Second), loadOfSize(t, "n1", size))
		if !errors.Is(err, tether.ErrInvalidInput) || !errors.Is(err, ErrTooLarge) {
			t.Fatalf("%d bytes (limit %d): err = %v, want ErrTooLarge and tether.ErrInvalidInput", size, limit, err)
		}
	}

	// Exactly at the line it goes.
	if err := l.Report(tctx(t, 20*time.Second), loadOfSize(t, "n1", limit-loadHeaderRoom)); err != nil {
		t.Fatalf("a report of limit-1KiB bytes: %v", err)
	}
}

func TestLoadReportUnavailableWithoutBucket(t *testing.T) {
	// A cell that was never provisioned has no load bucket.
	urls := clustertest.Start(t, 1)
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.NodeID = "n1"
	cfg.Cell = "c9"
	cfg.Mode = ModeExternal
	cfg.URLs = urls
	cfg.Replicas = 1

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	l := NewLoad(c, cfg)
	if err := l.Report(tctx(t, 5*time.Second), sampleLoad("n1", 0)); err == nil {
		t.Fatal("Report with no bucket succeeded")
	}
}

func TestLoadIntervalConfig(t *testing.T) {
	good := func(mut func(*Config)) Config {
		c := DefaultConfig()
		c.Enabled = true
		c.NodeID = "n1"
		c.Cell = "c1"
		c.Mode = ModeExternal
		c.URLs = []string{"nats://127.0.0.1:4222"}

		mut(&c)

		return c
	}

	if got := DefaultConfig().LoadInterval; got != 2*time.Second {
		t.Fatalf("default LoadInterval = %s, want 2s", got)
	}

	for _, tc := range []struct {
		name string
		cfg  Config
		bad  bool
	}{
		{"default", good(func(*Config) {}), false},
		{"zero", good(func(c *Config) { c.LoadInterval = 0 }), true},
		{"negative", good(func(c *Config) { c.LoadInterval = -time.Second }), true},
		// A report lives three intervals, and must not outlive the lease
		// (a reclaimed dead node's report would still count).
		{"a third of the lease", good(func(c *Config) { c.LoadInterval = c.Lease / 3 }), false},
		{"over a third of the lease", good(func(c *Config) { c.LoadInterval = c.Lease/3 + time.Nanosecond }), true},
		{"half the lease", good(func(c *Config) { c.LoadInterval = c.Lease / 2 }), true},
		{"over half the lease", good(func(c *Config) { c.LoadInterval = c.Lease/2 + time.Nanosecond }), true},
	} {
		err := tc.cfg.Validate()
		if tc.bad != (err != nil) {
			t.Errorf("%s: Validate = %v", tc.name, err)
		}

		if err != nil && !strings.Contains(err.Error(), "load_interval") {
			t.Errorf("%s: error %q does not name load_interval", tc.name, err)
		}
	}

	if got := DefaultConfig().LoadBucket(); got != "tether-load-" {
		t.Fatalf("LoadBucket with no cell = %q", got)
	}
}

func TestProvisionCreatesLoadBucket(t *testing.T) {
	for _, tc := range []struct {
		name     string
		servers  int
		replicas int
	}{
		{"single", 1, 1},
		{"three", 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			urls := clustertest.Start(t, tc.servers)
			cfg := DefaultConfig()
			cfg.Enabled = true
			cfg.NodeID = "n1"
			cfg.Cell = "c1"
			cfg.Mode = ModeExternal
			cfg.URLs = urls

			c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(c.Close)

			ctx := tctx(t, 30*time.Second)

			for i := range 2 {
				if perr := Provision(ctx, c, cfg, time.Hour); perr != nil {
					t.Fatalf("Provision #%d: %v", i+1, perr)
				}
			}

			if got := cfg.LoadBucket(); got != "tether-load-c1" {
				t.Fatalf("LoadBucket = %q", got)
			}

			kv, err := c.JS().KeyValue(ctx, cfg.LoadBucket())
			if err != nil {
				t.Fatal(err)
			}

			st, err := kv.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}

			sc := st.(*jetstream.KeyValueBucketStatus).StreamInfo().Config
			if st.History() != 1 || sc.Replicas != tc.replicas || sc.Storage != jetstream.FileStorage {
				t.Errorf("history %d, replicas %d (want %d), storage %v", st.History(), sc.Replicas, tc.replicas, sc.Storage)
			}

			if !sc.AllowMsgTTL || sc.SubjectDeleteMarkerTTL != time.Minute {
				t.Errorf("limit markers off (AllowMsgTTL %v, marker TTL %v)", sc.AllowMsgTTL, sc.SubjectDeleteMarkerTTL)
			}

			// The same replicas as the sessions bucket.
			skv, err := c.JS().KeyValue(ctx, cfg.SessionsBucket())
			if err != nil {
				t.Fatal(err)
			}

			sst, err := skv.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if sr := sst.(*jetstream.KeyValueBucketStatus).StreamInfo().Config.Replicas; sr != sc.Replicas {
				t.Errorf("load bucket has %d replicas, sessions bucket %d", sc.Replicas, sr)
			}
		})
	}
}

// A node that withdraws its report leaves its peers' view at once, long
// before the report's lifetime runs out.
func TestLoadWithdraw(t *testing.T) {
	loads := startLoads(t, 2, 10*time.Second) // a 30 s lifetime
	a, b := loads[0], loads[1]

	feed := newLoadFeed()

	stop, err := b.Watch(tctx(t, 30*time.Second), feed.fn)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(stop)

	if err := a.Report(tctx(t, 10*time.Second), sampleLoad("n1", 0)); err != nil {
		t.Fatal(err)
	}

	feed.waitN1(t, 10*time.Second, anyReport)

	start := time.Now()

	if err := a.Withdraw(tctx(t, 10*time.Second)); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "removal of the withdrawn report", 5*time.Second, func() bool { return feed.deleted("n1") > 0 })
	t.Logf("withdrawn report removed after %s", time.Since(start).Round(10*time.Millisecond))

	if feed.report("n1") != nil {
		t.Fatal("the callback still holds the withdrawn report")
	}

	// Withdrawing again, with nothing left to remove, is not an error.
	if err := a.Withdraw(tctx(t, 10*time.Second)); err != nil {
		t.Fatalf("second withdraw: %v", err)
	}
}
