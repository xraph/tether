package extension

import "github.com/xraph/tether/broker"

// WithClusterDeps makes the broker one node of a cell over c (its Conn and
// Config, provisioned by the test). Task 14 replaces it with WithCluster.
func WithClusterDeps(c *broker.Cluster) Option {
	return func(e *Extension) { e.clusterDeps = c }
}
