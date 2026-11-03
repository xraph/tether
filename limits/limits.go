// Package limits enforces per-tenant connection counts and message rates.
//
// The broker asks for a connection slot at CONNECT and gives it back on
// disconnect, and asks AllowMessage before it accepts a PUBLISH. Both are keyed
// by tenant ID, so one tenant's load never spends another tenant's budget.
//
// A Limiter counts one node. In a cell the broker widens both limits to the
// cell: it tells AcquireConnectionWithRemote how many connections the tenant
// holds on the other nodes, and gives AllowMessage this node's share of the
// tenant's rate (SetShare).
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
	mu     sync.Mutex
	conns  map[string]int
	rates  map[string]*bucket
	shares map[string]float64
}

// bucket is a tenant's rate limiter and the rate and burst it was last set
// to, shares applied.
type bucket struct {
	lim   *rate.Limiter
	rps   float64
	burst int
}

// minShare is the smallest share SetShare keeps. A tenant's share is never
// zero: it would turn the tenant's rate on this node to nothing.
const minShare = 0.001

// New returns an empty Limiter.
func New() *Limiter {
	return &Limiter{conns: make(map[string]int), rates: make(map[string]*bucket), shares: make(map[string]float64)}
}

// SetShare sets the fraction of tenantID's rate and burst this node may spend,
// in a cell where the tenant's budget is split across nodes. share is clamped
// to (0, 1]: a value above 1 is 1, and one at or under zero, or not a number,
// is a tiny share (0.001). A tenant never given a share has all of its
// budget (1).
//
// The tenant's bucket picks the new share up on its next AllowMessage and is
// updated in place, so its tokens carry over.
func (lt *Limiter) SetShare(tenantID string, share float64) {
	switch {
	case math.IsNaN(share) || share < minShare:
		share = minShare
	case share > 1:
		share = 1
	}

	lt.mu.Lock()
	defer lt.mu.Unlock()

	if share == 1 {
		delete(lt.shares, tenantID)

		return
	}

	lt.shares[tenantID] = share
}

// Share returns tenantID's share of its message budget, 1 when none was set.
func (lt *Limiter) Share(tenantID string) float64 {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if s, ok := lt.shares[tenantID]; ok {
		return s
	}

	return 1
}

// AllowMessage reports whether tenantID may send one more message under l. A
// MessagesPerSecond of zero or less means no limit. Burst defaults to the rate
// rounded up, and at least one.
//
// The tenant's share (SetShare) scales both: the bucket refills at rate x share
// and holds max(1, ceil(burst x share)). A tenant without a share gets the
// whole of l.
//
// Each tenant has one bucket. When l or the share differs from what the bucket
// last saw, the bucket is updated in place (rate.Limiter SetLimit and SetBurst):
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

	rps := l.MessagesPerSecond

	if share, ok := lt.shares[tenantID]; ok {
		rps *= share
		burst = max(1, int(math.Ceil(float64(burst)*share)))
	}

	b := lt.rates[tenantID]

	switch {
	case b == nil:
		b = &bucket{lim: rate.NewLimiter(rate.Limit(rps), burst), rps: rps, burst: burst}
		lt.rates[tenantID] = b
	case b.rps != rps || b.burst != burst:
		b.lim.SetLimit(rate.Limit(rps))
		b.lim.SetBurst(burst)
		b.rps, b.burst = rps, burst
	}

	lt.mu.Unlock()

	return b.lim.Allow()
}

// AcquireConnection takes a connection slot for tenantID and reports whether
// it got one. A max of zero or less means unlimited; the slot is still counted.
// Every successful call needs one ReleaseConnection.
func (lt *Limiter) AcquireConnection(tenantID string, maxConns int) bool {
	return lt.AcquireConnectionWithRemote(tenantID, maxConns, 0)
}

// AcquireConnectionWithRemote is AcquireConnection for a tenant that also
// holds remote connections on other nodes: it refuses when maxConns is above
// zero and this node's connections plus remote are already maxConns or more.
// Only this node's slot is counted here.
func (lt *Limiter) AcquireConnectionWithRemote(tenantID string, maxConns, remote int) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if maxConns > 0 && lt.conns[tenantID]+max(remote, 0) >= maxConns {
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
