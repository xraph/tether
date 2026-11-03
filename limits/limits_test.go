package limits

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xraph/tether/tenant"
)

func TestConnectionSlots(t *testing.T) {
	l := New()

	assert.True(t, l.AcquireConnection("a", 1))
	assert.False(t, l.AcquireConnection("a", 1), "second slot over max")
	assert.True(t, l.AcquireConnection("b", 1), "tenants are independent")

	l.ReleaseConnection("a")
	assert.True(t, l.AcquireConnection("a", 1), "a released slot is reusable")
}

func TestConnectionSlotsUnlimited(t *testing.T) {
	l := New()

	for range 100 {
		assert.True(t, l.AcquireConnection("a", 0))
	}

	assert.True(t, l.AcquireConnection("a", -1), "negative is unlimited too")
}

func TestConnectionCountNeverNegative(t *testing.T) {
	l := New()

	l.ReleaseConnection("a")
	l.ReleaseConnection("a")

	assert.True(t, l.AcquireConnection("a", 1))
	assert.False(t, l.AcquireConnection("a", 1), "extra releases must not create slots")
}

func TestConnectionSlotsConcurrent(t *testing.T) {
	l := New()

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		got int
	)

	for range 50 {
		wg.Go(func() {
			if l.AcquireConnection("a", 10) {
				mu.Lock()
				got++
				mu.Unlock()
			}
		})
	}

	wg.Wait()
	assert.Equal(t, 10, got)
}

func TestAllowMessageBurst(t *testing.T) {
	l := New()
	lim := tenant.Limits{MessagesPerSecond: 1, Burst: 2}

	assert.True(t, l.AllowMessage("a", lim))
	assert.True(t, l.AllowMessage("a", lim))
	assert.False(t, l.AllowMessage("a", lim), "burst spent")
	assert.True(t, l.AllowMessage("b", lim), "tenants are independent")
}

func TestAllowMessageDefaultBurst(t *testing.T) {
	l := New()
	lim := tenant.Limits{MessagesPerSecond: 0.5}

	assert.True(t, l.AllowMessage("a", lim), "burst defaults to at least one")
	assert.False(t, l.AllowMessage("a", lim))
}

func TestAllowMessageUnlimited(t *testing.T) {
	l := New()

	for range 1000 {
		assert.True(t, l.AllowMessage("a", tenant.Limits{}))
	}
}

// A change of limits updates the tenant's bucket in place: the tokens carry
// over, so a spent budget stays spent.
func TestAllowMessageLimitsUpdatedInPlace(t *testing.T) {
	l := New()

	tight := tenant.Limits{MessagesPerSecond: 0.01, Burst: 1}
	assert.True(t, l.AllowMessage("a", tight))
	assert.False(t, l.AllowMessage("a", tight))

	loose := tenant.Limits{MessagesPerSecond: 0.01, Burst: 5}
	assert.False(t, l.AllowMessage("a", loose), "a larger burst is no fresh bucket")

	// A lower burst clamps what is left.
	l2 := New()
	assert.True(t, l2.AllowMessage("b", tenant.Limits{MessagesPerSecond: 0.01, Burst: 5}))
	assert.True(t, l2.AllowMessage("b", tenant.Limits{MessagesPerSecond: 0.01, Burst: 1}))
	assert.False(t, l2.AllowMessage("b", tenant.Limits{MessagesPerSecond: 0.01, Burst: 1}), "clamped to the new burst")
}

// Connections of one tenant can carry different limits snapshots (taken at
// their CONNECT, before and after the tenant was updated). Alternating them
// must not refill the bucket: the allowance stays bounded by the more
// permissive snapshot.
func TestAllowMessageAlternatingSnapshotsStayBounded(t *testing.T) {
	strict := tenant.Limits{MessagesPerSecond: 1, Burst: 1}
	permissive := tenant.Limits{MessagesPerSecond: 10, Burst: 10}

	for _, order := range [][2]tenant.Limits{{strict, permissive}, {permissive, strict}} {
		l := New()
		allowed := 0

		for i := range 1000 {
			if l.AllowMessage("a", order[i%2]) {
				allowed++
			}
		}

		// The permissive burst, plus one token of slack for what the
		// permissive rate refills while the loop runs.
		assert.GreaterOrEqual(t, allowed, 1)
		assert.LessOrEqual(t, allowed, permissive.Burst+1, "alternating snapshots refilled the bucket")
	}
}

func TestAcquireWithRemote(t *testing.T) {
	l := New()

	assert.True(t, l.AcquireConnectionWithRemote("a", 3, 2), "local 0 + remote 2 < 3")
	assert.False(t, l.AcquireConnectionWithRemote("a", 3, 2), "local 1 + remote 2 = 3")
	assert.True(t, l.AcquireConnectionWithRemote("a", 3, 1), "fewer remote connections leave room")

	for range 50 {
		assert.True(t, l.AcquireConnectionWithRemote("u", 0, 1000), "max 0 is unlimited whatever the remote count")
	}

	// A remote count over the max leaves no room, even with nothing local.
	assert.False(t, l.AcquireConnectionWithRemote("b", 2, 5))

	// The plain call is the same with remote 0, and the slot counts.
	assert.True(t, l.AcquireConnection("c", 1))
	assert.False(t, l.AcquireConnectionWithRemote("c", 1, 0))
}

func TestShareScalesRateAndBurst(t *testing.T) {
	l := New()
	l.SetShare("a", 0.25)

	lim := tenant.Limits{MessagesPerSecond: 100, Burst: 100}
	allowed := 0

	for range 200 {
		if l.AllowMessage("a", lim) {
			allowed++
		}
	}

	// ceil(100 x 0.25) = 25 in the burst, and a few tokens of refill (25/s)
	// while the loop runs.
	assert.GreaterOrEqual(t, allowed, 25)
	assert.LessOrEqual(t, allowed, 28)

	// The default burst scales too: rate 8, no burst, share 0.25 gives 2.
	l.SetShare("b", 0.25)

	got := 0

	for range 20 {
		if l.AllowMessage("b", tenant.Limits{MessagesPerSecond: 8}) {
			got++
		}
	}

	assert.GreaterOrEqual(t, got, 2)
	assert.LessOrEqual(t, got, 3)

	// A tiny share still allows one message.
	l.SetShare("c", 0.0001)
	assert.True(t, l.AllowMessage("c", tenant.Limits{MessagesPerSecond: 100, Burst: 100}))
}

// A changed share updates the bucket in place: a spent budget stays spent.
func TestShareChangeKeepsTokens(t *testing.T) {
	l := New()
	lim := tenant.Limits{MessagesPerSecond: 0.01, Burst: 4}

	for range 4 {
		assert.True(t, l.AllowMessage("a", lim))
	}

	assert.False(t, l.AllowMessage("a", lim))

	l.SetShare("a", 1)
	l.SetShare("a", 0.5)
	assert.False(t, l.AllowMessage("a", lim), "a new share is no fresh bucket")
}

func TestShareDefaultOne(t *testing.T) {
	l := New()

	assert.InDelta(t, 1.0, l.Share("never"), 0)

	lim := tenant.Limits{MessagesPerSecond: 100, Burst: 10}
	allowed := 0

	for range 30 {
		if l.AllowMessage("never", lim) {
			allowed++
		}
	}

	assert.GreaterOrEqual(t, allowed, 10, "a tenant without a share gets the whole burst")
}

func TestShareClamped(t *testing.T) {
	l := New()

	l.SetShare("a", 5)
	assert.InDelta(t, 1.0, l.Share("a"), 0)

	for _, bad := range []float64{0, -1, math.NaN(), math.Inf(-1)} {
		l.SetShare("b", bad)
		assert.Greater(t, l.Share("b"), 0.0, "share %v", bad)
		assert.LessOrEqual(t, l.Share("b"), 1.0)
	}

	l.SetShare("c", math.Inf(1))
	assert.InDelta(t, 1.0, l.Share("c"), 0)

	l.SetShare("d", 0.4)
	assert.InDelta(t, 0.4, l.Share("d"), 1e-9)
}
