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

func TestAllowMessageRecreatedWhenLimitsChange(t *testing.T) {
	l := New()

	tight := tenant.Limits{MessagesPerSecond: 1, Burst: 1}
	assert.True(t, l.AllowMessage("a", tight))
	assert.False(t, l.AllowMessage("a", tight))

	loose := tenant.Limits{MessagesPerSecond: 1, Burst: 5}
	assert.True(t, l.AllowMessage("a", loose), "new limits get a fresh bucket")
}
