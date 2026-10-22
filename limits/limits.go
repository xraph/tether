// Package limits enforces per-tenant connection counts and message rates.
//
// The broker asks for a connection slot at CONNECT and gives it back on
// disconnect, and asks AllowMessage before it accepts a PUBLISH. Both are keyed
// by tenant ID, so one tenant's load never spends another tenant's budget.
package limits

import (
	"math"
	"sync"

	"golang.org/x/time/rate"

	"github.com/xraph/tether/tenant"
)

// Limiter tracks connection slots and message rates per tenant. The zero value
// is not usable; call New. A Limiter is safe for concurrent use.
type Limiter struct {
	mu    sync.Mutex
	conns map[string]int
	rates map[string]*bucket
}

// bucket is a tenant's rate limiter and the limits it was built from.
type bucket struct {
	lim   *rate.Limiter
	rps   float64
	burst int
}

// New returns an empty Limiter.
func New() *Limiter {
	return &Limiter{conns: make(map[string]int), rates: make(map[string]*bucket)}
}

// AllowMessage reports whether tenantID may send one more message under l. A
// MessagesPerSecond of zero or less means no limit. Burst defaults to the rate
// rounded up, and at least one.
//
// Each tenant has one bucket. When l differs from the limits the bucket last
// saw, the bucket is updated in place (rate.Limiter SetLimit and SetBurst):
// its tokens carry over and are clamped to the new burst. Callers may pass
// different snapshots of a tenant's limits (the broker passes each
// connection's, taken at CONNECT), and alternating them must not hand out a
// fresh, full bucket each time.
func (lt *Limiter) AllowMessage(tenantID string, l tenant.Limits) bool {
	if l.MessagesPerSecond <= 0 {
		return true
	}

	burst := l.Burst
	if burst <= 0 {
		burst = max(1, int(math.Ceil(l.MessagesPerSecond)))
	}

	lt.mu.Lock()

	b := lt.rates[tenantID]

	switch {
	case b == nil:
		b = &bucket{lim: rate.NewLimiter(rate.Limit(l.MessagesPerSecond), burst), rps: l.MessagesPerSecond, burst: burst}
		lt.rates[tenantID] = b
	case b.rps != l.MessagesPerSecond || b.burst != burst:
		b.lim.SetLimit(rate.Limit(l.MessagesPerSecond))
		b.lim.SetBurst(burst)
		b.rps, b.burst = l.MessagesPerSecond, burst
	}

	lt.mu.Unlock()

	return b.lim.Allow()
}

// AcquireConnection takes a connection slot for tenantID and reports whether
// it got one. A max of zero or less means unlimited; the slot is still counted.
// Every successful call needs one ReleaseConnection.
func (lt *Limiter) AcquireConnection(tenantID string, maxConns int) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if maxConns > 0 && lt.conns[tenantID] >= maxConns {
		return false
	}

	lt.conns[tenantID]++

	return true
}

// ReleaseConnection gives back a slot taken by AcquireConnection. Releasing
// more than was acquired is ignored.
func (lt *Limiter) ReleaseConnection(tenantID string) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	n := lt.conns[tenantID]
	if n <= 1 {
		delete(lt.conns, tenantID)

		return
	}

	lt.conns[tenantID] = n - 1
}
