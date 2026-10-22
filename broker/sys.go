package broker

import (
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"

	"github.com/xraph/tether/topic"
)

// defaultSysInterval is how often each tenant's $SYS stats are published.
const defaultSysInterval = 10 * time.Second

// withheldTopic replaces the topic of an outbound PUBLISH that failed closed
// (see hook.withhold). It names no tenant.
const withheldTopic = "$tether/withheld"

// Stats is a snapshot of one tenant's activity on this node.
type Stats struct {
	// Connected counts the tenant's open network connections.
	Connected int

	// Sessions counts the tenant's sessions: open connections plus kept
	// sessions of disconnected clients.
	Sessions int

	// Subscriptions counts the subscriptions held by the tenant's sessions.
	Subscriptions int

	// Retained counts the retained messages in the tenant's namespace.
	Retained int

	// MessagesIn counts PUBLISH packets from the tenant's clients that passed
	// the ACL and the rate limit, since the broker started.
	MessagesIn uint64

	// MessagesOut counts PUBLISH packets written to the tenant's clients,
	// $SYS stats included, since the broker started.
	MessagesOut uint64
}

// tenantCounters are a tenant's message counters.
type tenantCounters struct {
	in, out atomic.Uint64
}

// countersFor returns tenantID's counters, creating them on first use.
func (b *Broker) countersFor(tenantID string) *tenantCounters {
	b.countersMu.Lock()
	defer b.countersMu.Unlock()

	c := b.counters[tenantID]
	if c == nil {
		c = &tenantCounters{}
		b.counters[tenantID] = c
	}

	return c
}

// addCounters copies tenantID's message counters into st. A tenant that never
// sent or received anything has none.
func (b *Broker) addCounters(tenantID string, st *Stats) {
	b.countersMu.Lock()
	c := b.counters[tenantID]
	b.countersMu.Unlock()

	if c != nil {
		st.MessagesIn = c.in.Load()
		st.MessagesOut = c.out.Load()
	}
}

// Stats returns tenantID's stats on this node. Before Start and after Stop
// only the message counters are known. Each call walks mochi's clients and
// retained messages once.
func (b *Broker) Stats(tenantID string) Stats {
	b.mu.Lock()
	srv := b.server
	b.mu.Unlock()

	var st Stats

	if srv != nil {
		if s := collect(srv, tenantID, false)[tenantID]; s != nil {
			st = *s
		}
	}

	b.addCounters(tenantID, &st)

	return st
}

// collect counts sessions, connections, subscriptions and retained messages
// per tenant from mochi's own state. With only set, it counts that tenant
// alone. With liveOnly, tenants without an open connection are left out.
func collect(srv *mqtt.Server, only string, liveOnly bool) map[string]*Stats {
	out := make(map[string]*Stats)

	for _, cl := range srv.Clients.GetAll() {
		if cl.Net.Inline {
			continue
		}

		if only != "" && !strings.HasPrefix(cl.ID, only+"/") {
			continue
		}

		tenantID, ok := tenantOfID(cl.ID)
		if !ok {
			continue
		}

		st := out[tenantID]
		if st == nil {
			st = &Stats{}
			out[tenantID] = st
		}

		st.Sessions++
		st.Subscriptions += cl.State.Subscriptions.Len()

		if !cl.Closed() {
			st.Connected++
		}
	}

	if only != "" && out[only] == nil {
		out[only] = &Stats{}
	}

	if liveOnly {
		for tenantID, st := range out {
			if st.Connected == 0 {
				delete(out, tenantID)
			}
		}
	}

	if len(out) == 0 {
		return out
	}

	for name := range srv.Topics.Retained.GetAll() {
		if st := out[retainedTenant(name)]; st != nil {
			st.Retained++
		}
	}

	return out
}

// retainedTenant returns the level of a namespaced topic name that holds the
// tenant ID: the first, or the second when the first starts with "$" (see
// topic.Namespace). For mochi's own "$SYS/broker/..." that is "broker", which
// is no tenant.
func retainedTenant(name string) string {
	first, rest, _ := strings.Cut(name, "/")
	if !strings.HasPrefix(first, "$") {
		return first
	}

	second, _, _ := strings.Cut(rest, "/")

	return second
}

// sysTopics are the tenant-relative $SYS names the broker publishes for every
// tenant with an open connection, and how to read each from Stats.
var sysTopics = []struct {
	name  string
	value func(*Stats) string
}{
	{"$SYS/clients/connected", func(s *Stats) string { return strconv.Itoa(s.Connected) }},
	{"$SYS/messages/received", func(s *Stats) string { return strconv.FormatUint(s.MessagesIn, 10) }},
	{"$SYS/messages/sent", func(s *Stats) string { return strconv.FormatUint(s.MessagesOut, 10) }},
	{"$SYS/subscriptions/count", func(s *Stats) string { return strconv.Itoa(s.Subscriptions) }},
	{"$SYS/retained/count", func(s *Stats) string { return strconv.Itoa(s.Retained) }},
}

// runSys publishes the tenants' $SYS stats every interval, and every second
// ends the 3.1.1 sessions whose expiry ran out (expireSessions), until stop
// closes. Then it closes done.
func (b *Broker) runSys(srv *mqtt.Server, interval time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	tick := time.NewTicker(interval)
	defer tick.Stop()

	sweep := time.NewTicker(sessionSweepInterval * time.Second)
	defer sweep.Stop()

	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			b.publishSys(srv)
		case now := <-sweep.C:
			b.expireSessions(srv, now.Unix())
		}
	}
}

// publishSys publishes one round of $SYS stats through mochi's inline client,
// under each live tenant's own "$SYS/<tenant>/..." names. They are not
// retained. Only that tenant's clients can read them: OnACLCheck strips the
// subscriber's tenant from every delivery.
func (b *Broker) publishSys(srv *mqtt.Server) {
	for tenantID, st := range collect(srv, "", true) {
		b.addCounters(tenantID, st)

		for _, t := range sysTopics {
			name := topic.Namespace(tenantID, t.name)
			if err := srv.Publish(name, []byte(t.value(st)), false, 0); err != nil {
				b.log.Debug("tether: $SYS publish failed", slog.String("topic", name), slog.Any("error", err))
			}
		}
	}
}
