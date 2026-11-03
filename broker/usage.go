package broker

import (
	"slices"
	"strings"
	"sync"
	"time"
)

// TenantUsage is one tenant's metered usage on this node.
//
// Connections is how many network connections the tenant holds right now.
// Every other field is cumulative since the broker started. Bytes are PUBLISH
// payload bytes, so the topic and the properties are not counted.
type TenantUsage struct {
	// Tenant is the tenant ID.
	Tenant string

	// Connections is the tenant's current count of network connections on
	// this node.
	Connections int

	// MessagesIn and BytesIn count what Stats.MessagesIn counts: PUBLISH
	// packets from the tenant's clients that passed the ACL and the rate
	// limit, and the HTTP publish endpoint's (Broker.DevicePublish).
	MessagesIn, BytesIn uint64

	// MessagesOut and BytesOut count what Stats.MessagesOut counts: PUBLISH
	// packets written to the tenant's clients, $SYS stats included.
	MessagesOut, BytesOut uint64

	// ConnectionTime is the connection time the tenant's connections have
	// added up, integrated up to the Usage call.
	ConnectionTime time.Duration
}

// connClock integrates a tenant's connection time: a count of live
// connections and the time of its last change. Each change adds the count
// times the time since the last one.
type connClock struct {
	mu    sync.Mutex
	conns int
	last  time.Time
	total time.Duration
}

// settle adds the time since the last change, at the current count. The
// caller holds mu.
func (c *connClock) settle(now time.Time) {
	if c.conns > 0 {
		// A monotonic reading never runs backwards; a stale now (taken
		// before another goroutine's change) would, so it adds nothing.
		if d := now.Sub(c.last); d > 0 {
			c.total += time.Duration(c.conns) * d
		}
	}

	if now.After(c.last) {
		c.last = now
	}
}

// open counts one more connection.
func (c *connClock) open(now time.Time) {
	c.mu.Lock()
	c.settle(now)
	c.conns++
	c.mu.Unlock()
}

// close counts one connection fewer.
func (c *connClock) close(now time.Time) {
	c.mu.Lock()
	c.settle(now)

	if c.conns > 0 {
		c.conns--
	}

	c.mu.Unlock()
}

// read returns the live count and the connection time up to now.
func (c *connClock) read(now time.Time) (conns int, total time.Duration) {
	c.mu.Lock()
	c.settle(now)
	conns, total = c.conns, c.total
	c.mu.Unlock()

	return conns, total
}

// connectionOpened counts a connection of the tenant that took a slot of its
// own. A takeover that inherits the old connection's slot opens nothing.
func (c *tenantCounters) connectionOpened() {
	c.clock.open(time.Now())
}

// connectionClosed counts a connection of the tenant that gave its slot back.
func (c *tenantCounters) connectionClosed() {
	c.clock.close(time.Now())
}

// StartedAt returns when the broker started, the zero time before Start. The
// cumulative fields of Usage count from then.
func (b *Broker) StartedAt() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.startedAt
}

// Usage returns the usage of every tenant this broker has seen since Start,
// sorted by tenant ID. A tenant is seen once it connects or its traffic is
// counted. ConnectionTime is integrated up to the call. It takes no lock a
// publish needs and calls into neither mochi nor the cell.
func (b *Broker) Usage() []TenantUsage {
	return b.usageAt(time.Now())
}

// usageAt is Usage with the connection time integrated up to now.
func (b *Broker) usageAt(now time.Time) []TenantUsage {
	b.countersMu.Lock()
	tenants := make([]string, 0, len(b.counters))
	counters := make([]*tenantCounters, 0, len(b.counters))

	for id, c := range b.counters {
		tenants = append(tenants, id)
		counters = append(counters, c)
	}
	b.countersMu.Unlock()

	out := make([]TenantUsage, len(tenants))

	for i, c := range counters {
		out[i] = c.usage(tenants[i], now)
	}

	slices.SortFunc(out, func(a, b TenantUsage) int { return strings.Compare(a.Tenant, b.Tenant) })

	return out
}

// usage reads the counters as tenantID's TenantUsage, with the connection
// time integrated up to now.
func (c *tenantCounters) usage(tenantID string, now time.Time) TenantUsage {
	conns, total := c.clock.read(now)

	return TenantUsage{
		Tenant:         tenantID,
		Connections:    conns,
		MessagesIn:     c.in.Load(),
		MessagesOut:    c.out.Load(),
		BytesIn:        c.bytesIn.Load(),
		BytesOut:       c.bytesOut.Load(),
		ConnectionTime: total,
	}
}

// localUsage is tenantID's row of Usage, integrated up to now: zero but for
// Tenant when this node has not seen the tenant.
func (b *Broker) localUsage(tenantID string, now time.Time) TenantUsage {
	b.countersMu.Lock()
	c := b.counters[tenantID]
	b.countersMu.Unlock()

	if c == nil {
		return TenantUsage{Tenant: tenantID}
	}

	return c.usage(tenantID, now)
}
