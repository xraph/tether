package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	// limitMarkerTTL is how long the sessions, retained and load buckets keep the
	// marker an expired key leaves behind. Per-key TTL needs it, and watchers
	// see the expiry through it.
	limitMarkerTTL = time.Minute
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
//   - tether-load-<cell>: history 1, limit markers, LimitMarkerTTL 1 minute.
//
// Every one gets c.Replicas() replicas, except that with automatic replicas
// (Config.Replicas 0) one that exists with more keeps them: a node that
// counted fewer servers (a seed-only route list, a server that advertises
// nothing) never scales the cell's streams down. With automatic replicas
// Provision reads each one first, retrying the way it retries a create, and
// fails rather than guess when it cannot. The subjects are not namespaced by
// cell, so two cells cannot share a NATS account.
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
		n, err := keepReplicas(ctx, c, cfg, streams[i].Name, replicas)
		if err != nil {
			return fmt.Errorf("cluster: provision stream %s: %w", streams[i].Name, err)
		}

		streams[i].Replicas = n
	}

	for _, sc := range streams {
		err := retryUnanswered(ctx, func(ctx context.Context) error {
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
		{
			Bucket:         cfg.LoadBucket(),
			Description:    "tether per-node load reports",
			History:        1,
			Storage:        jetstream.FileStorage,
			Replicas:       replicas,
			LimitMarkerTTL: limitMarkerTTL,
		},
	}

	for i := range buckets {
		n, err := keepReplicas(ctx, c, cfg, kvStreamPrefix+buckets[i].Bucket, replicas)
		if err != nil {
			return fmt.Errorf("cluster: provision bucket %s: %w", buckets[i].Bucket, err)
		}

		buckets[i].Replicas = n
	}

	for _, bc := range buckets {
		err := retryUnanswered(ctx, func(ctx context.Context) error {
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
// with more. The read is retried like a create (retryUnanswered): a stream
// whose leader is being elected leaves it unanswered, and taking that for
// "absent" scaled the cell's streams down to a late node's count. Only a
// stream that does not exist gets want without a read; one that cannot be
// read is an error.
func keepReplicas(ctx context.Context, c *Conn, cfg Config, name string, want int) (int, error) {
	if cfg.Replicas > 0 {
		return want, nil
	}

	have := 0

	err := retryUnanswered(ctx, func(ctx context.Context) error {
		st, err := c.js.Stream(ctx, name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			have = 0

			return nil
		}

		if err != nil {
			return err
		}

		have = st.CachedInfo().Config.Replicas

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("read the replica count: %w", err)
	}

	if have > want {
		c.logger.Info("cluster: keeping the replica count a stream already has", "stream", name, "replicas", have, "counted", want)

		return have, nil
	}

	return want, nil
}
