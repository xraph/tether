package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster/clustertest"
)

// startControls returns one started-later Control per node on a three-server
// cell.
func startControls(t *testing.T, nodes int) []*Control {
	t.Helper()

	conns, cfg := startCellConns(t, nodes)
	out := make([]*Control, 0, nodes)

	for _, c := range conns {
		ctl := NewControl(c, cfg)
		t.Cleanup(ctl.Stop)

		out = append(out, ctl)
	}

	return out
}

// ctlCounter counts the events one node applied, by an ID each carries.
type ctlCounter struct {
	mu     sync.Mutex
	counts map[string]int
	total  atomic.Int32
}

func newCtlCounter() *ctlCounter { return &ctlCounter{counts: map[string]int{}} }

func (c *ctlCounter) add(id string) {
	c.mu.Lock()
	c.counts[id]++
	c.mu.Unlock()
	c.total.Add(1)
}

// subscribeAll registers a handler for every event kind that decodes the event
// and counts it under "<kind>:<id>".
func (c *ctlCounter) subscribeAll(t *testing.T, ctl *Control) {
	t.Helper()

	sub := func(kind string, id func([]byte) (string, error)) {
		err := ctl.Subscribe(kind, func(data []byte) {
			v, err := id(data)
			if err != nil {
				t.Errorf("%s: %v", kind, err)

				return
			}

			c.add(kind + ":" + v)
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	sub(KindKeyInvalidated, func(b []byte) (string, error) {
		var e KeyInvalidated
		err := json.Unmarshal(b, &e)

		if err == nil && (!e.Disconnect || !e.GraceEnds.Equal(time.Unix(1700000000, 0))) {
			err = fmt.Errorf("fields lost: %+v", e)
		}

		return e.KeyID, err
	})
	sub(KindRoleChanged, func(b []byte) (string, error) {
		var e RoleChanged
		err := json.Unmarshal(b, &e)

		return e.RoleID, err
	})
	sub(KindSubjectChanged, func(b []byte) (string, error) {
		var e SubjectChanged
		err := json.Unmarshal(b, &e)

		if err == nil && e.Kind != "user" {
			err = fmt.Errorf("kind lost: %+v", e)
		}

		return e.ID, err
	})
	sub(KindTenantUpdated, func(b []byte) (string, error) {
		var e TenantUpdated
		err := json.Unmarshal(b, &e)

		return e.TenantID, err
	})
	sub(KindSessionKick, func(b []byte) (string, error) {
		var e SessionKick
		err := json.Unmarshal(b, &e)

		if err == nil && e.Tenant != testTenant {
			err = fmt.Errorf("tenant lost: %+v", e)
		}

		return e.ClientID, err
	})
	sub(KindSessionDelete, func(b []byte) (string, error) {
		var e SessionDelete
		err := json.Unmarshal(b, &e)

		if err == nil && e.Tenant != testTenant {
			err = fmt.Errorf("tenant lost: %+v", e)
		}

		return e.ClientID, err
	})
}

// event returns the i-th test event of a node: its kind, its value and the
// ID ctlCounter files it under.
func event(node, i int) (kind string, v any, id string) {
	tag := fmt.Sprintf("n%d-%d", node, i)

	switch i % 6 {
	case 0:
		return KindKeyInvalidated, KeyInvalidated{KeyID: tag, Disconnect: true, GraceEnds: time.Unix(1700000000, 0)}, KindKeyInvalidated + ":" + tag
	case 1:
		return KindRoleChanged, RoleChanged{RoleID: tag}, KindRoleChanged + ":" + tag
	case 2:
		return KindSubjectChanged, SubjectChanged{Kind: "user", ID: tag}, KindSubjectChanged + ":" + tag
	case 3:
		return KindTenantUpdated, TenantUpdated{TenantID: tag}, KindTenantUpdated + ":" + tag
	case 4:
		return KindSessionKick, SessionKick{Tenant: testTenant, ClientID: tag}, KindSessionKick + ":" + tag
	default:
		return KindSessionDelete, SessionDelete{Tenant: testTenant, ClientID: tag}, KindSessionDelete + ":" + tag
	}
}

// Every node, the publisher included, applies every event exactly once.
func TestControlEventOnce(t *testing.T) {
	ctls := startControls(t, 3)
	ctx := tctx(t, 60*time.Second)

	// Published before any node started: nobody gets it.
	if err := ctls[0].Publish(ctx, KindTenantUpdated, TenantUpdated{TenantID: "before-start"}); err != nil {
		t.Fatal(err)
	}

	counters := make([]*ctlCounter, len(ctls))
	for i, ctl := range ctls {
		counters[i] = newCtlCounter()
		counters[i].subscribeAll(t, ctl)

		if err := ctl.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}

	const perNode = 12

	var wg sync.WaitGroup

	want := make([]string, 0, perNode*len(ctls))

	for n, ctl := range ctls {
		for i := range perNode {
			_, _, id := event(n+1, i)
			want = append(want, id)
		}

		wg.Go(func() {
			for i := range perNode {
				kind, v, _ := event(n+1, i)
				if err := ctl.Publish(ctx, kind, v); err != nil {
					t.Error(err)
				}
			}
		})
	}

	wg.Wait()

	total := int32(len(want))

	for i, c := range counters {
		eventually(t, 15*time.Second, fmt.Sprintf("node %d to apply %d events", i+1, total), func() bool { return c.total.Load() >= total })
	}

	time.Sleep(500 * time.Millisecond) // room for a duplicate to show up

	for i, c := range counters {
		c.mu.Lock()

		if n := c.total.Load(); n != total {
			t.Errorf("node %d applied %d events, want %d", i+1, n, total)
		}

		for _, id := range want {
			if c.counts[id] != 1 {
				t.Errorf("node %d applied %s %d times, want once", i+1, id, c.counts[id])
			}
		}

		if c.counts[KindTenantUpdated+":before-start"] != 0 {
			t.Errorf("node %d applied an event published before it started", i+1)
		}

		c.mu.Unlock()
	}
}

// Events of a kind nobody subscribed to are ignored, and handlers see only
// their own kind, in registration order.
func TestControlUnknownKindsIgnored(t *testing.T) {
	ctl := startControls(t, 1)[0]
	ctx := tctx(t, 30*time.Second)

	var (
		mu    sync.Mutex
		calls []string
	)

	record := func(tag string) func([]byte) {
		return func(data []byte) {
			mu.Lock()
			calls = append(calls, tag+":"+string(data))
			mu.Unlock()
		}
	}

	if err := ctl.Subscribe(KindSessionKick, record("first")); err != nil {
		t.Fatal(err)
	}

	if err := ctl.Subscribe(KindSessionKick, record("second")); err != nil {
		t.Fatal(err)
	}

	if err := ctl.Start(ctx); err != nil {
		t.Fatal(err)
	}

	for _, kind := range []string{"custom.thing", KindSessionKick + ".extra", "session", KindSessionDelete} {
		if err := ctl.Publish(ctx, kind, map[string]string{"kind": kind}); err != nil {
			t.Fatal(err)
		}
	}

	if err := ctl.Publish(ctx, KindSessionKick, SessionKick{Tenant: "t", ClientID: "c"}); err != nil {
		t.Fatal(err)
	}

	eventually(t, 10*time.Second, "the kick to arrive", func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(calls) >= 2
	})

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	want := []string{
		`first:{"tenant":"t","client_id":"c"}`,
		`second:{"tenant":"t","client_id":"c"}`,
	}

	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("calls = %q, want %q", calls, want)
	}
}

func TestControlInvalidInputAndLifecycle(t *testing.T) {
	ctl := startControls(t, 1)[0]
	ctx := tctx(t, 30*time.Second)

	for _, kind := range []string{"", "a..b", ".a", "a.", "a.*", "a.>", "a b", "ctl>", "é", string(make([]byte, maxKindBytes+1))} {
		if err := ctl.Publish(ctx, kind, 1); !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("Publish(%q) = %v, want ErrInvalidInput", kind, err)
		}

		if err := ctl.Subscribe(kind, func([]byte) {}); !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("Subscribe(%q) = %v, want ErrInvalidInput", kind, err)
		}
	}

	if err := ctl.Subscribe(KindRoleChanged, nil); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("Subscribe(nil) = %v, want ErrInvalidInput", err)
	}

	if err := ctl.Publish(ctx, KindRoleChanged, func() {}); err == nil {
		t.Error("Publish of a value JSON cannot encode succeeded")
	}

	if err := ctl.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if err := ctl.Start(ctx); err == nil {
		t.Error("second Start succeeded")
	}

	ctl.Stop()
	ctl.Stop()

	if err := ctl.Subscribe(KindRoleChanged, func([]byte) {}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Subscribe after Stop = %v, want ErrUnavailable", err)
	}

	if err := ctl.Start(ctx); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Start after Stop = %v, want ErrUnavailable", err)
	}

	// Publish needs no running consumer.
	if err := ctl.Publish(ctx, KindRoleChanged, RoleChanged{RoleID: "r"}); err != nil {
		t.Errorf("Publish after Stop = %v", err)
	}
}

// A node cut off from NATS has its consumer inside the ordered consumer's
// recreate loop. Stop must not wait for it.
func TestControlStopWhileUnreachable(t *testing.T) {
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

	ctl := NewControl(c, cfg)
	ctl.heartbeat = 500 * time.Millisecond

	var calls atomic.Int32

	if err := ctl.Subscribe(KindTenantUpdated, func([]byte) { calls.Add(1) }); err != nil {
		t.Fatal(err)
	}

	ctx := tctx(t, 30*time.Second)

	if err := ctl.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if err := ctl.Publish(ctx, KindTenantUpdated, TenantUpdated{TenantID: "x"}); err != nil {
		t.Fatal(err)
	}

	eventually(t, 10*time.Second, "the event", func() bool { return calls.Load() == 1 })

	cl.Stop(0)
	time.Sleep(3 * time.Second) // well past two missed heartbeats

	start := time.Now()
	ctl.Stop()

	if d := time.Since(start); d > time.Second {
		t.Fatalf("Stop took %s while NATS was unreachable", d)
	}
}

// Stop waits for a handler that is running, and no handler runs after it.
func TestControlStopWaitsForRunningHandler(t *testing.T) {
	ctl := startControls(t, 1)[0]
	ctx := tctx(t, 30*time.Second)

	entered := make(chan struct{})
	release := make(chan struct{})

	var calls atomic.Int32

	err := ctl.Subscribe(KindRoleChanged, func([]byte) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := ctl.Start(ctx); err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		if err := ctl.Publish(ctx, KindRoleChanged, RoleChanged{RoleID: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never ran")
	}

	stopped := make(chan struct{})

	go func() {
		ctl.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned while a handler was running")
	case <-time.After(300 * time.Millisecond):
	}

	close(release)

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the handler finished")
	}

	time.Sleep(300 * time.Millisecond)

	if n := calls.Load(); n != 1 {
		t.Fatalf("handler ran %d times, want 1: nothing after Stop", n)
	}
}
