package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
)

// fakeMetrics records what the broker reports. closed receives one value per
// ConnectionClosed, so a test can wait for a disconnect without sleeping.
type fakeMetrics struct {
	mu        sync.Mutex
	opened    int
	closedN   int
	in, out   int
	authFails map[string]int
	denials   map[string]int

	closed chan struct{}
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{authFails: map[string]int{}, denials: map[string]int{}, closed: make(chan struct{}, 16)}
}

func (f *fakeMetrics) ConnectionOpened() { f.mu.Lock(); f.opened++; f.mu.Unlock() }

func (f *fakeMetrics) ConnectionClosed() {
	f.mu.Lock()
	f.closedN++
	f.mu.Unlock()

	f.closed <- struct{}{}
}

func (f *fakeMetrics) MessageIn()  { f.mu.Lock(); f.in++; f.mu.Unlock() }
func (f *fakeMetrics) MessageOut() { f.mu.Lock(); f.out++; f.mu.Unlock() }

func (f *fakeMetrics) AuthFailure(reason string) { f.mu.Lock(); f.authFails[reason]++; f.mu.Unlock() }
func (f *fakeMetrics) ACLDenied(op string)       { f.mu.Lock(); f.denials[op]++; f.mu.Unlock() }

// snapshot is a copy of the counts, for assertions.
type metricsSnapshot struct {
	opened, closed, in, out int
	authFails, denials      map[string]int
}

func (f *fakeMetrics) snapshot() metricsSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()

	s := metricsSnapshot{opened: f.opened, closed: f.closedN, in: f.in, out: f.out,
		authFails: map[string]int{}, denials: map[string]int{}}

	for k, v := range f.authFails {
		s.authFails[k] = v
	}

	for k, v := range f.denials {
		s.denials[k] = v
	}

	return s
}

func TestMetricsReported(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	m := newFakeMetrics()

	b, err := New(testConfig(), Deps{
		Auth:    auth.NewStatic(map[string]*auth.Identity{"dev": ident(tn, "dev")}),
		Perms:   fakePerms{rules: map[string]acl.Rules{"dev": {Publish: []string{"up/#"}, Subscribe: []string{"down/#"}}}},
		Logger:  testLogger(),
		Metrics: m,
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	addr := b.Addr().String()

	c, err := connect311(t, addr, "dev-1", "dev", nil)
	require.NoError(t, err)

	got := m.snapshot()
	assert.Equal(t, 1, got.opened, "one connection opened")
	assert.Equal(t, 0, got.closed)

	// A refused subscribe and a refused publish. Mochi closes a 3.1.1
	// client over a refused QoS 1 publish, so this one is QoS 0; the
	// granted QoS 1 publish after it is acknowledged only once the server
	// has read both, which orders the check.
	tok := c.Subscribe("secret/#", 1, nil)
	require.True(t, tok.WaitTimeout(waitTimeout))
	require.True(t, c.Publish("nope/x", 0, false, "denied").WaitTimeout(waitTimeout))

	// One message in.
	publish311(t, c, "up/x", "hello")

	// One message out: a broker publish the client is subscribed to.
	msgs := subscribe311(t, c, "down/#")
	require.NoError(t, b.Publish(t.Context(), tn.ID.String(), "down/x", []byte("hi"), 1, false))
	assert.Equal(t, "down/x", recv(t, msgs).Topic)

	// A refused CONNECT.
	_, err = connect311(t, addr, "intruder", "nobody", nil)
	require.Error(t, err)

	got = m.snapshot()
	assert.Equal(t, 1, got.in, "only the granted publish counts in")
	assert.Equal(t, 1, got.out)
	assert.Equal(t, map[string]int{AuthReasonBadCredentials: 1}, got.authFails)
	assert.Equal(t, map[string]int{ACLOpPublish: 1, ACLOpSubscribe: 1}, got.denials)

	c.Disconnect(0)

	select {
	case <-m.closed:
	case <-time.After(waitTimeout):
		require.FailNow(t, "the connection was not reported closed")
	}

	got = m.snapshot()
	assert.Equal(t, 1, got.opened)
	assert.Equal(t, 1, got.closed)
}

// A takeover inherits the old connection's slot, so the connection count
// does not move: one device, one connection.
func TestMetricsTakeoverKeepsOneConnection(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	m := newFakeMetrics()

	b, err := New(testConfig(), Deps{
		Auth:    auth.NewStatic(map[string]*auth.Identity{"dev": ident(tn, "dev")}),
		Perms:   fakePerms{},
		Logger:  testLogger(),
		Metrics: m,
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	_, err = connect311(t, b.Addr().String(), "same", "dev", nil)
	require.NoError(t, err)

	_, err = connect311(t, b.Addr().String(), "same", "dev", nil)
	require.NoError(t, err)

	// The first connection is closed by the takeover.
	select {
	case <-m.closed:
	case <-time.After(waitTimeout):
		require.FailNow(t, "the replaced connection was not reported closed")
	}

	got := m.snapshot()
	assert.Equal(t, 1, got.opened-got.closed, "one live connection")
}

// A publish a device makes over HTTP counts as a message in, the way its
// MQTT PUBLISH would; one the host makes does not.
func TestDevicePublishCountsIn(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	m := newFakeMetrics()

	b, err := New(testConfig(), Deps{Auth: auth.NewStatic(nil), Perms: fakePerms{}, Metrics: m})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	tid := tn.ID.String()

	require.NoError(t, b.Publish(t.Context(), tid, "up/1", []byte("host"), 0, false))
	assert.Equal(t, uint64(0), b.Stats(tid).MessagesIn)

	require.NoError(t, b.DevicePublish(t.Context(), tid, "up/1", []byte("dev"), 0, false))
	assert.Equal(t, uint64(1), b.Stats(tid).MessagesIn)
	assert.Equal(t, 1, m.snapshot().in)

	// A refused one counts nothing.
	require.Error(t, b.DevicePublish(t.Context(), tid, "$SYS/x", nil, 0, false))
	assert.Equal(t, uint64(1), b.Stats(tid).MessagesIn)
}
