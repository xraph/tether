package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// logSubjects holds the QoS 1 and 2 log. See the subject package.
	logSubjects = "d.>"
	// ctlSubjects holds control messages between nodes.
	ctlSubjects = "ctl.>"
	// logDuplicateWindow is how long the log stream remembers Nats-Msg-Id.
	logDuplicateWindow = 2 * time.Minute
	// controlMaxAge is how long a control message stays in its stream.
	controlMaxAge = time.Hour
	// limitMarkerTTL is how long the sessions and retained buckets keep the
	// marker an expired key leaves behind. Per-key TTL needs it, and watchers
	// see the expiry through it.
	limitMarkerTTL = time.Minute

	// A node of a fresh cluster can have JetStream before its peers have all
	// joined the meta group, and placing a replicated stream then fails with
	// these API codes. They clear on their own, so Provision retries them.
	errCodeNoPeers     jetstream.ErrorCode = 10005 // JSClusterNoPeersErrF
	errCodeClusterWait jetstream.ErrorCode = 10008 // JSClusterNotAvailErr
	placementRetry                         = 200 * time.Millisecond
	// placementAttempt bounds the first create or update of a stream or
	// bucket, and each later attempt gets twice the last, up to
	// placementAttemptMax. When the nodes of a fresh cell all provision at
	// once, the meta leader leaves concurrent creates of the same stream
	// unanswered; the attempt times out and the next one finds the stream.
	// A create that was only slow is harmless to send again.
	placementAttempt    = time.Second
	placementAttemptMax = 8 * time.Second
)

// Provision creates the cell's streams and buckets, or updates them to the
// settings below when they exist. Call it as often as you like, from every
// node.
//
//   - TETHER_LOG_<CELL>: subjects "d.>", file storage, MaxAge
//     maxSessionExpiry (0 keeps messages until a limit removes them), a 2
//     minute duplicate window.
//   - TETHER_CTL_<CELL>: subjects "ctl.>", file storage, MaxAge 1 hour.
//   - tether-sessions-<cell>: history 1, limit markers, LimitMarkerTTL 1 minute.
//   - tether-retained-<cell>: limit markers, LimitMarkerTTL 1 minute.
//   - tether-members-<cell>: history 1.
//
// Every one gets c.Replicas() replicas, except that with automatic replicas
// (Config.Replicas 0) one that exists with more keeps them: a node that
// counted fewer servers (a seed-only route list, a server that advertises
// nothing) never scales the cell's streams down. The subjects are not
// namespaced by cell, so two cells cannot share a NATS account.
func Provision(ctx context.Context, c *Conn, cfg Config, maxSessionExpiry time.Duration) error {
	if c == nil {
		return errors.New("cluster: provision: nil connection")
	}

	if err := checkCell(cfg.Cell); err != nil {
		return err
	}

	if maxSessionExpiry < 0 {
		return fmt.Errorf("cluster: provision: negative session expiry %s", maxSessionExpiry)
	}

	replicas := c.replicas

	streams := []jetstream.StreamConfig{
		{
			Name:        cfg.LogStream(),
			Description: "tether QoS 1 and 2 message log",
			Subjects:    []string{logSubjects},
			Storage:     jetstream.FileStorage,
			Retention:   jetstream.LimitsPolicy,
			Replicas:    replicas,
			MaxAge:      maxSessionExpiry,
			Duplicates:  logDuplicateWindow,
		},
		{
			Name:        cfg.ControlStream(),
			Description: "tether control messages between nodes",
			Subjects:    []string{ctlSubjects},
			Storage:     jetstream.FileStorage,
			Retention:   jetstream.LimitsPolicy,
			Replicas:    replicas,
			MaxAge:      controlMaxAge,
		},
	}

	for i := range streams {
		streams[i].Replicas = keepReplicas(ctx, c, cfg, streams[i].Name, replicas)
	}

	for _, sc := range streams {
		err := retryPlacement(ctx, func(ctx context.Context) error {
			_, err := c.js.CreateOrUpdateStream(ctx, sc)

			return err
		})
		if err != nil {
			return fmt.Errorf("cluster: provision stream %s: %w", sc.Name, err)
		}
	}

	buckets := []jetstream.KeyValueConfig{
		{
			Bucket:         cfg.SessionsBucket(),
			Description:    "tether session ownership and state",
			History:        1,
			Storage:        jetstream.FileStorage,
			Replicas:       replicas,
			LimitMarkerTTL: limitMarkerTTL,
		},
		{
			Bucket:         cfg.RetainedBucket(),
			Description:    "tether retained messages",
			History:        1,
			Storage:        jetstream.FileStorage,
			Replicas:       replicas,
			LimitMarkerTTL: limitMarkerTTL,
		},
		{
			Bucket:      cfg.MembersBucket(),
			Description: "tether live nodes",
			History:     1,
			Storage:     jetstream.FileStorage,
			Replicas:    replicas,
		},
	}

	for i := range buckets {
		buckets[i].Replicas = keepReplicas(ctx, c, cfg, kvStreamPrefix+buckets[i].Bucket, replicas)
	}

	for _, bc := range buckets {
		err := retryPlacement(ctx, func(ctx context.Context) error {
			_, err := c.js.CreateOrUpdateKeyValue(ctx, bc)

			return err
		})
		if err != nil {
			return fmt.Errorf("cluster: provision bucket %s: %w", bc.Bucket, err)
		}
	}

	return nil
}

// kvStreamPrefix names the stream behind a KV bucket.
const kvStreamPrefix = "KV_"

// keepReplicas is the replica count Provision gives the stream named name:
// want, or with automatic replicas the stream's own count when it exists
// with more. A stream that cannot be read is given want.
func keepReplicas(ctx context.Context, c *Conn, cfg Config, name string, want int) int {
	if cfg.Replicas > 0 {
		return want
	}

	ctx, cancel := context.WithTimeout(ctx, 2*placementAttempt)
	defer cancel()

	st, err := c.js.Stream(ctx, name)
	if err != nil {
		return want
	}

	if have := st.CachedInfo().Config.Replicas; have > want {
		c.logger.Info("cluster: keeping the replica count a stream already has", "stream", name, "replicas", have, "counted", want)

		return have
	}

	return want
}

// retryPlacement runs fn until it stops failing with a placement error or an
// unanswered attempt, or ctx ends. Each attempt is bounded (placementAttempt,
// doubling). It returns fn's last error.
func retryPlacement(ctx context.Context, fn func(context.Context) error) error {
	attempt := placementAttempt

	for {
		actx, cancel := context.WithTimeout(ctx, attempt)
		err := fn(actx)

		cancel()

		if err == nil || !retryable(ctx, err) {
			return err
		}

		attempt = min(2*attempt, placementAttemptMax)

		select {
		case <-ctx.Done():
			return err
		case <-time.After(placementRetry):
		}
	}
}

// retryable reports whether a failed create is worth another attempt: a
// placement error, or an attempt nobody answered while ctx still runs.
func retryable(ctx context.Context, err error) bool {
	var api *jetstream.APIError
	if errors.As(err, &api) {
		return api.ErrorCode == errCodeNoPeers || api.ErrorCode == errCodeClusterWait
	}

	return ctx.Err() == nil && (errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders))
}
