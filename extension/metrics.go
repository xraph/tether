package extension

import (
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/tether/broker"
)

// Metric names. None carries a tenant label: per-tenant numbers are in the
// admin API's stats and each tenant's $SYS topics.
const (
	// MetricConnections is a gauge of live MQTT connections on this node.
	MetricConnections = "tether_connections"

	// MetricMessagesIn counts client PUBLISH packets that passed the ACL
	// and the tenant's rate limit.
	MetricMessagesIn = "tether_messages_in_total"

	// MetricMessagesOut counts PUBLISH packets written to clients.
	MetricMessagesOut = "tether_messages_out_total"

	// MetricAuthFailures counts refused CONNECTs, labelled reason (one of
	// the broker.AuthReason constants).
	MetricAuthFailures = "tether_auth_failures_total"

	// MetricACLDenials counts failed ACL checks, labelled op ("publish" or
	// "subscribe").
	MetricACLDenials = "tether_acl_denials_total"

	// The cell metrics below exist only when the node runs in a cell.

	// MetricClusterFenced is a gauge, 1 while the node is fenced from its
	// cell (it has dropped its clients and refuses new ones), else 0.
	MetricClusterFenced = "tether_cluster_fenced"

	// MetricLogAppendSeconds is a histogram of the time a QoS 1 or 2 append
	// to the cell's log takes from send to commit, client publishes, host
	// publishes and wills alike. The time includes any wait before the send
	// while the node's publisher has 4000 acks outstanding.
	MetricLogAppendSeconds = "tether_log_append_seconds"

	// MetricCASLost counts session record writes that lost their
	// compare-and-set to another writer: contested CONNECTs, takeovers and
	// dead-node reclaims racing each other.
	MetricCASLost = "tether_cas_lost_total"

	// MetricReplayMessages counts messages replayed from the log to
	// sessions that came back, or moved here, behind the tenant's reader.
	MetricReplayMessages = "tether_replay_messages_total"
)

// logAppendBuckets are the bounds of MetricLogAppendSeconds, in seconds: a
// commit on a healthy cell takes a millisecond or a few, and the broker gives
// one attempt of a client publish up to 10 s (its append attempt bound, the
// publisher's backpressure wait included) before it sends it again.
var logAppendBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// brokerMetrics reports the broker's counters to forge's metrics.
type brokerMetrics struct {
	connections forge.Gauge
	in, out     forge.Counter
	authFails   *forge.Vec[forge.Counter]
	aclDenials  *forge.Vec[forge.Counter]
}

var _ broker.Metrics = (*brokerMetrics)(nil)

func newBrokerMetrics(m forge.Metrics) *brokerMetrics {
	return &brokerMetrics{
		connections: m.Gauge(MetricConnections),
		in:          m.Counter(MetricMessagesIn),
		out:         m.Counter(MetricMessagesOut),
		authFails:   forge.CounterVec(m, MetricAuthFailures, "reason"),
		aclDenials:  forge.CounterVec(m, MetricACLDenials, "op"),
	}
}

func (b *brokerMetrics) ConnectionOpened()         { b.connections.Inc() }
func (b *brokerMetrics) ConnectionClosed()         { b.connections.Dec() }
func (b *brokerMetrics) MessageIn()                { b.in.Inc() }
func (b *brokerMetrics) MessageOut()               { b.out.Inc() }
func (b *brokerMetrics) AuthFailure(reason string) { b.authFails.With(reason).Inc() }
func (b *brokerMetrics) ACLDenied(op string)       { b.aclDenials.With(op).Inc() }

// cellMetrics is brokerMetrics plus the cell's: what a node in a cell
// reports to.
type cellMetrics struct {
	*brokerMetrics

	fenced   forge.Gauge
	appends  forge.Histogram
	casLost  forge.Counter
	replayed forge.Counter
}

var _ broker.ClusterMetrics = (*cellMetrics)(nil)

func newCellMetrics(m forge.Metrics) *cellMetrics {
	return &cellMetrics{
		brokerMetrics: newBrokerMetrics(m),
		fenced:        m.Gauge(MetricClusterFenced),
		appends:       forge.HistogramVec(m, MetricLogAppendSeconds, forge.HistogramOpts{Buckets: logAppendBuckets}).With(),
		casLost:       m.Counter(MetricCASLost),
		replayed:      m.Counter(MetricReplayMessages),
	}
}

func (c *cellMetrics) ClusterFenced(fenced bool) {
	if fenced {
		c.fenced.Set(1)
	} else {
		c.fenced.Set(0)
	}
}

func (c *cellMetrics) LogAppended(d time.Duration) { c.appends.Observe(d.Seconds()) }
func (c *cellMetrics) CASLost()                    { c.casLost.Inc() }
func (c *cellMetrics) ReplayMessage()              { c.replayed.Inc() }
