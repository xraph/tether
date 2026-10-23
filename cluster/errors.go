package cluster

import "errors"

// Sentinel errors of the cluster package. Wrap them with %w and match with
// errors.Is.
var (
	// ErrLostCAS means a compare-and-set against a KV bucket lost the race.
	// JetStream reports it as error 10071 (wrong last sequence) or 10164
	// (key exists on a create), and both map here.
	ErrLostCAS = errors.New("tether/cluster: lost compare-and-set")

	// ErrFenced means this node no longer holds the lease it was acting under
	// and must stop acting for the sessions it owned.
	ErrFenced = errors.New("tether/cluster: node is fenced")

	// ErrUnavailable means NATS or JetStream could not be reached in time.
	ErrUnavailable = errors.New("tether/cluster: nats or jetstream unavailable")
)
