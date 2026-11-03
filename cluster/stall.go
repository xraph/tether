package cluster

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

const (
	// consumerResetAttempts is how many times an ordered consumer of the log
	// or the control bus tries to recreate itself in one recovery before its
	// iterator gives up with an error (nats.go's MaxResetAttempts, which is
	// unlimited by default). nats.go retries under the same consumer name,
	// and a create whose name the meta layer already placed on a server that
	// died answers "consumer is offline" (10119) for as long as that server
	// stays down, so an unlimited loop never ends. One attempt hands the
	// failure to the owner, which starts a new consumer under a new name, and
	// the meta layer places that one on a live server.
	consumerResetAttempts = 1
	// pullExpiry is how long a pull request waits on the server (nats.go's
	// default, which the iterators keep); a pull heartbeat may be at most half
	// of it (maxPullHeartbeat), or nats.go refuses it.
	pullExpiry       = 30 * time.Second
	maxPullHeartbeat = pullExpiry / 2
	// replayPullHeartbeat is the idle heartbeat of the pull requests of
	// replays; readers and the control bus use Config.PullHeartbeat
	// (DefaultPullHeartbeat, 2 s), since they are many and long-lived. A pull
	// that never reaches
	// its consumer (it got to the client's server before the new consumer's
	// interest did, and the server answered it 503, which nats.go's iterator
	// ignores, or it was lost on a route) gets no heartbeats either, and
	// nats.go pulls again only after two are missed: then the ordered
	// consumer recreates itself and pulls anew. Left to nats.go the heartbeat
	// is half the 30 s pull expiry (its ordered default of 5 s is parsed and
	// then dropped, jetstream/ordered.go:259 and :275), so such a consumer
	// delivered nothing for 30 s. At 1 s it is about 2 s: a replay is short
	// and its stall watch (defaultReplayStallAfter) sits under the broker's
	// 10 s replay call, so it needs the quick recovery.
	replayPullHeartbeat = time.Second
	// defaultStallAfter is how long a consumer loop may go without a message
	// while its stream holds more for it before the stall watch ends it.
	defaultStallAfter = 30 * time.Second
	// defaultReplayStallAfter is that for a replay consumer. The broker
	// bounds a replay call at 10 s and tries again on a fresh consumer, so a
	// watch longer than that never fired for it. A replay's watch asks no
	// one (the calls waiting say what they still need), so a short one costs
	// nothing; 5 s is more than two missed pull heartbeats and a recreate.
	defaultReplayStallAfter = 5 * time.Second
	// minStallCheck bounds how often a stall watch looks.
	minStallCheck = 10 * time.Millisecond
)

// stallWatch notices a consumer loop that has stopped making progress while
// its stream holds messages it has not delivered, which the ordered
// consumer's own recovery should never allow but which would lose messages
// silently if it did. The loop calls progress when it receives a message and
// when it is done handling one, and sets busy while a handler runs, since a
// slow handler is not a stall.
type stallWatch struct {
	after time.Duration
	base  time.Time
	last  atomic.Int64 // nanoseconds since base of the last progress
	busy  atomic.Bool
}

func newStallWatch(after time.Duration) *stallWatch {
	if after <= 0 {
		after = defaultStallAfter
	}

	return &stallWatch{after: after, base: time.Now()}
}

// progress records that the loop moved.
func (w *stallWatch) progress() { w.last.Store(int64(time.Since(w.base))) }

// quiet is how long the loop has not moved.
func (w *stallWatch) quiet() time.Duration { return time.Since(w.base) - time.Duration(w.last.Load()) }

// run looks every after/4, jittered by up to a quarter so watches started
// together do not ask together, until stop or done closes. When the loop has
// been quiet for after with no handler running, it asks behind for the
// newest sequence the stream holds for the loop (want) and how far the loop
// got (at). want above at is a stall: stalled is called once and run
// returns. Otherwise the loop is idle and caught up, which counts as
// progress, so an idle loop costs one behind call per after.
//
// An error from behind (NATS unreachable, say) decides nothing. The watch
// then asks again at every look, whatever the quiet time, until the stream
// answers, and counts that first answer as progress. So after an outage the
// loop gets a whole after, from the moment the stream answers again (give or
// take one look), to deliver before it can be ended: the consumer's own
// recovery runs in that time, and a consumer catching up is not ended for
// the outage it just sat through.
func (w *stallWatch) run(stop, done <-chan struct{}, behind func(context.Context) (want, at uint64, err error), stalled func(want, at uint64)) {
	every := max(w.after/4, minStallCheck)
	timer := time.NewTimer(jitter(every))
	outage := false

	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-done:
			return
		case <-timer.C:
			timer.Reset(jitter(every))
		}

		if !outage && (w.busy.Load() || w.quiet() < w.after) {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		want, at, err := behind(ctx)

		cancel()

		switch {
		case err != nil:
			outage = true

			continue
		case outage:
			// The first answer after an outage starts the clock again.
			outage = false

			w.progress()

			continue
		case want <= at:
			w.progress()

			continue
		}

		// Look again: a message may have arrived while behind ran.
		if w.busy.Load() || w.quiet() < w.after {
			continue
		}

		select {
		case <-stop:
			return
		default:
		}

		stalled(want, at)

		return
	}
}

// jitter is d plus or minus up to a quarter of it.
func jitter(d time.Duration) time.Duration {
	q := int64(d / 4)
	if q <= 0 {
		return d
	}

	return d - time.Duration(q) + time.Duration(rand.Int64N(2*q)) // #nosec G404 -- spreads checks, nothing secret
}

// pullHeartbeatOr is d, or def when d is not positive.
func pullHeartbeatOr(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}

	return def
}
