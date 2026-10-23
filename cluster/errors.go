package cluster

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Sentinel errors of the cluster package. Wrap them with %w and match with
// errors.Is.
var (
	// ErrLostCAS means a compare-and-set against a KV bucket lost the race.
	// JetStream reports it as error 10071 (wrong last sequence) or 10164
	// (the same, while the winner's write is still in flight on a replicated
	// stream), and both map here. The error also wraps the JetStream error, so
	// errors.As finds the *jetstream.APIError.
	ErrLostCAS = errors.New("tether/cluster: lost compare-and-set")

	// ErrFenced means this node no longer holds the lease it was acting under
	// and must stop acting for the sessions it owned.
	ErrFenced = errors.New("tether/cluster: node is fenced")

	// ErrUnavailable means NATS or JetStream could not be reached in time.
	ErrUnavailable = errors.New("tether/cluster: nats or jetstream unavailable")
)

// codeWrongLastSeqConstant is nats-server's JSStreamWrongLastSequenceConstantErr
// (10164). nats.go has no exported constant for it.
const codeWrongLastSeqConstant jetstream.ErrorCode = 10164

// isLostCAS reports whether err says a compare-and-set lost: JetStream error
// 10071 or 10164, or one of nats.go's own names for them.
func isLostCAS(err error) bool {
	if err == nil {
		return false
	}

	var api *jetstream.APIError
	if errors.As(err, &api) &&
		(api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence || api.ErrorCode == codeWrongLastSeqConstant) {
		return true
	}

	return errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch)
}

// isUnavailable reports whether err means NATS could not be reached, or did
// not answer in time.
func isUnavailable(err error) bool {
	return errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrConnectionClosed) ||
		errors.Is(err, nats.ErrDisconnected) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, context.DeadlineExceeded)
}
