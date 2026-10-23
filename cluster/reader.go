package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/subject"
)

const (
	// readerInactive is how long the server keeps a reader's consumer after
	// the node stops asking it for messages.
	readerInactive = 5 * time.Minute
	// replayInactive is the same for a replay's consumer, which is short
	// lived and deleted explicitly when it finishes.
	replayInactive = 30 * time.Second
	// consumerRetry is how long a read loop waits after an error that the
	// ordered consumer did not recover from by itself.
	consumerRetry = 200 * time.Millisecond
	// cleanupWait bounds the delete of a finished consumer.
	cleanupWait = 5 * time.Second
)

// Reader follows one tenant's messages in the log, in stream order, and hands
// each to a function. It is an ordered JetStream consumer with a single
// filter, "d.<tenant>.>", so it never sees another tenant's messages and a
// "$" topic of the tenant comes through like any other. When the connection
// drops or a message is missed, the consumer recreates itself from the last
// sequence it saw, so fn still sees every message once and in order.
type Reader struct {
	l      *Log
	tenant string
	cons   jetstream.Consumer
	msgs   jetstream.MessagesContext
	fn     func(LogMessage) error
	pos    atomic.Uint64
	stop   chan struct{}
	once   sync.Once

	// fnMu is held while fn runs. Stop takes it to set stopped, which is how
	// it knows fn is not running and will not run again.
	fnMu    sync.Mutex
	stopped bool
}

// Reader starts reading tenant's messages. fromSeq 0 means only messages
// appended from now on; otherwise the first message handed to fn is the first
// of the tenant's with a sequence of fromSeq or more. The consumer exists on
// the server when Reader returns, so a message appended after that is not
// missed.
//
// fn runs on the reader's one goroutine, one message at a time, so a slow fn
// slows the reader and nothing else. When fn returns an error the reader logs
// it at Warn and goes on to the next message: one message that cannot be
// delivered must not stall the tenant's log. If fn needs to retry, it retries
// itself. A message whose subject or headers do not decode is logged at Warn
// and skipped.
//
// Call Stop when done. Do not call it from fn.
func (l *Log) Reader(tenant string, fromSeq uint64, fn func(LogMessage) error) (*Reader, error) {
	if subject.LogTenantAll(tenant) == "" {
		return nil, fmt.Errorf("cluster: reader: empty tenant: %w", tether.ErrInvalidInput)
	}

	if fn == nil {
		return nil, fmt.Errorf("cluster: reader: nil handler: %w", tether.ErrInvalidInput)
	}

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	start := fromSeq

	if fromSeq == 0 {
		// Start at an explicit sequence, not at "new": the consumer may be
		// recreated before its first message, and "new" would then mean new
		// as of the recreation.
		st, err := l.stream(ctx)
		if err != nil {
			return nil, err
		}

		info, err := st.Info(ctx)
		if err != nil {
			return nil, l.wrapErr("reader: log stream info", err)
		}

		start = info.State.LastSeq + 1
	}

	cons, err := l.consumer(ctx, tenant, start, readerInactive)
	if err != nil {
		return nil, err
	}

	msgs, err := l.messages(cons)
	if err != nil {
		l.dropConsumer(cons)

		return nil, l.wrapErr("reader: start", err)
	}

	r := &Reader{
		l: l, tenant: tenant, cons: cons, msgs: msgs, fn: fn,
		stop: make(chan struct{}),
	}
	r.pos.Store(start - 1)

	go r.run()

	return r, nil
}

// Position is the sequence of the last message the reader is done with: the
// last one fn returned from, or one that was skipped because it did not
// decode. Before the first message it is one below the first sequence the
// reader will read: fromSeq-1, or the last sequence of the log when the reader
// started at "now". It never goes backwards, and it is safe to call from any
// goroutine.
func (r *Reader) Position() uint64 { return r.pos.Load() }

// Stop ends the reader. When it returns, fn is not running and is never called
// again: Stop waits for a message that fn is in the middle of, and no more
// reach it after that. It does not wait for the consumer. The iterator and the
// server consumer are shut down in the background, which can take 20 s or more
// while NATS is unreachable (the ordered consumer is then in its recreate
// loop and leaves it only between attempts), and a fenced node must be able to
// stop all its readers at once. Stop is safe to call more than once. Calling
// it from fn waits for itself forever.
func (r *Reader) Stop() {
	r.once.Do(func() {
		r.fnMu.Lock()
		r.stopped = true
		r.fnMu.Unlock()

		close(r.stop)

		go r.msgs.Stop()
	})
}

func (r *Reader) run() {
	defer r.l.dropConsumer(r.cons)

	r.l.pump(r.msgs, r.stop, r.handle)
}

func (r *Reader) handle(msg jetstream.Msg) {
	md, err := msg.Metadata()
	if err != nil {
		r.l.logger.Warn("log reader: message without metadata", "tenant", r.tenant, "error", err)

		return
	}

	seq := md.Sequence.Stream

	lm, err := decodeLogMessage(msg.Subject(), msg.Headers(), msg.Data())
	if err != nil {
		r.l.logger.Warn("log reader: skipping an entry that does not decode", "tenant", r.tenant, "seq", seq, "error", err)
		r.advance(seq)

		return
	}

	lm.Seq = seq

	if !r.call(lm) {
		return
	}

	r.advance(seq)
}

// call runs fn for lm unless the reader has been stopped, which it reports.
func (r *Reader) call(lm LogMessage) bool {
	r.fnMu.Lock()
	defer r.fnMu.Unlock()

	if r.stopped {
		return false
	}

	if err := r.fn(lm); err != nil {
		r.l.logger.Warn("log reader: handler failed, going on to the next entry", "tenant", r.tenant, "seq", lm.Seq, "error", err)
	}

	return true
}

func (r *Reader) advance(seq uint64) {
	for {
		cur := r.pos.Load()
		if seq <= cur || r.pos.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// consumer creates an ordered consumer on the log stream with the single
// filter of tenant, reading from start (a stream sequence).
func (l *Log) consumer(ctx context.Context, tenant string, start uint64, inactive time.Duration) (jetstream.Consumer, error) {
	st, err := l.stream(ctx)
	if err != nil {
		return nil, err
	}

	cons, err := st.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{subject.LogTenantAll(tenant)},
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       start,
		InactiveThreshold: inactive,
	})
	if err != nil {
		return nil, l.wrapErr("create log consumer", err)
	}

	l.consumers.Add(1)

	return cons, nil
}

// messages starts the iterator of cons.
func (l *Log) messages(cons jetstream.Consumer) (jetstream.MessagesContext, error) {
	if l.heartbeat > 0 {
		return cons.Messages(jetstream.PullHeartbeat(l.heartbeat))
	}

	return cons.Messages()
}

// dropConsumer deletes cons on the server, best effort: the server would
// remove it after its inactive threshold anyway.
func (l *Log) dropConsumer(cons jetstream.Consumer) {
	info := cons.CachedInfo()
	if info == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cleanupWait)
	defer cancel()

	st, err := l.stream(ctx)
	if err != nil {
		return
	}

	if err := st.DeleteConsumer(ctx, info.Name); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		l.logger.Debug("log consumer cleanup failed", "consumer", info.Name, "error", err)
	}
}

// pump hands every message of msgs to handle until msgs is stopped or stop is
// closed. An error from the iterator that the ordered consumer did not mend
// itself is logged and retried.
func (l *Log) pump(msgs jetstream.MessagesContext, stop <-chan struct{}, handle func(jetstream.Msg)) {
	for {
		msg, err := msgs.Next()
		if err == nil {
			handle(msg)

			continue
		}

		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			return
		}

		select {
		case <-stop:
			return
		default:
		}

		l.logger.Warn("log consumer error", "error", err)

		select {
		case <-stop:
			return
		case <-time.After(consumerRetry):
		}
	}
}
