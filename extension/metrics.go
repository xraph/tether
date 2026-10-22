package extension

import (
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
)

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
