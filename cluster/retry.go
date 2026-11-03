package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// A node of a fresh cluster can have JetStream before its peers have all
	// joined the meta group, and placing a replicated stream then fails with
	// these API codes. They clear on their own, so they are retried.
	errCodeNoPeers     jetstream.ErrorCode = 10005 // JSClusterNoPeersErrF
	errCodeClusterWait jetstream.ErrorCode = 10008 // JSClusterNotAvailErr

	// retryGap is the wait between two attempts of retryUnanswered.
	retryGap = 200 * time.Millisecond
	// retryAttempt bounds the first attempt of retryUnanswered, and each
	// later attempt gets twice the last, up to retryAttemptMax. A stream
	// whose leader is being elected leaves a request unanswered (a member
	// that cannot lead drops it), and when the nodes of a fresh cell all
	// provision at once the meta leader leaves concurrent creates of the
	// same stream unanswered. Waiting such a request out for the whole ctx
	// waits past the leader that would answer the next one. Everything
	// retried this way is harmless to send again.
	retryAttempt    = time.Second
	retryAttemptMax = 8 * time.Second
)

// retryUnanswered runs fn until it stops failing with an error retryable
// reports, or ctx ends, and returns fn's last error. Each attempt gets a
// context bounded at retryAttempt, doubling to retryAttemptMax, and attempts
// are retryGap apart. A call whose context is also the lifetime of what it
// starts (a KV watch) uses ctx itself and ignores the attempt's; nats.go
// bounds its consumer create on its own.
//
// It is what a node uses for every one-shot JetStream request it makes while
// it starts: Provision's creates and reads, the first heartbeat and the
// members watch (Members.Start), the control bus consumer (Control.Start)
// and the owned-sessions watch (Sessions.WatchOwned). A node started while
// NATS is slow or a stream elects a leader then starts once it answers,
// instead of failing on the first request nobody answered. Any other error
// (a missing stream or bucket, a config JetStream refuses, any other API
// error) ends it at once. A request NATS permissions deny gets no answer at
// all, so it is retried until ctx ends.
func retryUnanswered(ctx context.Context, fn func(context.Context) error) error {
	attempt := retryAttempt

	for {
		actx, cancel := context.WithTimeout(ctx, attempt)
		err := fn(actx)

		cancel()

		if err == nil || !retryable(ctx, err) {
			return err
		}

		attempt = min(2*attempt, retryAttemptMax)

		select {
		case <-ctx.Done():
			return err
		case <-time.After(retryGap):
		}
	}
}

// retryable reports whether a failed request is worth another attempt: a
// placement error, or an attempt nobody answered while ctx still runs. A
// publish nobody answered is jetstream.ErrNoStreamResponse, which does not
// wrap nats.ErrNoResponders.
func retryable(ctx context.Context, err error) bool {
	var api *jetstream.APIError
	if errors.As(err, &api) {
		return api.ErrorCode == errCodeNoPeers || api.ErrorCode == errCodeClusterWait
	}

	return ctx.Err() == nil && (errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrNoStreamResponse))
}
