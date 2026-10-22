package limits

import (
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
