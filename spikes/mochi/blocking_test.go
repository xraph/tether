package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// Question 4, the short version of `go run .`: a commit inside OnPublish
// serializes one client's QoS 1 publishes whatever its receive maximum, an
// ack written later from the hook does not, and another client's round trip
// is not held up either way.
func TestBlockingOnPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("measures for several seconds")
	}

	const commit = 2 * time.Millisecond

	run := func(mode commitMode, rm uint16, clients int) loadResult {
		t.Helper()

		res, err := runLoad(t.Context(), loadConfig{
			mode: mode, receiveMaximum: rm, loadClients: clients,
			duration: 1500 * time.Millisecond, commit: commit,
		})
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%-9s rm=%-3d clients=%d: %6.0f msgs/s, probe p50 %v p99 %v, commit median %v",
			mode, rm, clients, res.rate, res.probeP50, res.probeP99, res.commitMedian)

		return res
	}

	base := run(modeBlocking, 10, 0)
	b1 := run(modeBlocking, 1, 1)
	b100 := run(modeBlocking, 100, 1)
	a10 := run(modeAsyncAck, 10, 1)

	// One message per commit at most, with or without a window.
	ceiling := float64(time.Second) / float64(b100.commitMedian)
	if b100.rate > 1.1*ceiling {
		t.Fatalf("blocking rm=100 ran %.0f msgs/s, above one per commit (%.0f)", b100.rate, ceiling)
	}

	if b100.rate > 1.5*b1.rate {
		t.Fatalf("blocking rm=100 (%.0f) much faster than rm=1 (%.0f): not serialized", b100.rate, b1.rate)
	}

	if a10.rate < 3*b100.rate {
		t.Fatalf("async ack rm=10 ran %.0f msgs/s, want over 3x blocking (%.0f)", a10.rate, b100.rate)
	}

	// Loose, for a shared machine: the probe is not stuck behind the commits.
	limit := max(4*base.probeP99, 5*time.Millisecond)
	if b100.probeP99 > limit {
		t.Fatalf("probe p99 under blocking load %v, over %v (baseline %v)", b100.probeP99, limit, base.probeP99)
	}
}

// The async ack also works for QoS 2 with mochi's own PUBREL handling, as
// long as the hook puts the PUBREC into the client's inflight before it
// writes it.
func TestAsyncAckQoS2(t *testing.T) {
	hook := &commitHook{mode: modeAsyncAck, commit: 2 * time.Millisecond}
	srv, addr := start(t, hook.wire, hook)

	c, _ := connect(t, addr, "load-q2", true, false)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	var (
		wg sync.WaitGroup
		ok atomic.Int64
	)

	for range 10 {
		wg.Go(func() {
			for range 10 {
				pr, err := c.Publish(ctx, &paho.Publish{Topic: "load/q2", QoS: 2, Payload: []byte("x")})
				if err == nil && pr.ReasonCode == 0 {
					ok.Add(1)
				}
			}
		})
	}

	wg.Wait()

	if n := ok.Load(); n != 100 {
		t.Fatalf("%d of 100 QoS 2 publishes completed", n)
	}

	eventually(t, "inflight back to 0", func() bool { return atomic.LoadInt64(&srv.Info.Inflight) == 0 })
}
