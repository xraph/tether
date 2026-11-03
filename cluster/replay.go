package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/subject"
)

// Replay hands fn every message of tenant with a sequence in [fromSeq, toSeq],
// in order, and returns when the last one is done. It is how a session catches
// up on what it missed while it was away.
//
// The range is cut to the last message the tenant has in the log when the
// call starts, so a toSeq above it, or above the log's last sequence, does not
// make the call wait for new messages. fromSeq 0 is 1. A range with nothing of
// the tenant's in it (fromSeq above toSeq, or above the tenant's last message)
// returns nil without creating a consumer.
//
// Calls for one tenant on one node share consumers. A call joins a running
// consumer when that consumer started at or below its fromSeq and has not yet
// handed out a message with a sequence of fromSeq or more; otherwise it starts
// its own. A burst of sessions reconnecting after a restart therefore reads
// the log once, not once per session. Each call gets only the messages in its
// own range.
//
// fn runs on the consumer's goroutine, one message at a time, and calls that
// share a consumer run on the same goroutine, so a slow fn slows the others.
// An error from fn ends this call with that error (wrapped) and does not
// affect the calls it shares a consumer with. When Replay returns, fn is not
// running and will not run again. ctx ends the call with an error that wraps
// ctx's and says how far the consumer got (being made, being asked what it
// holds, or how many messages it handed out and how long it has been quiet),
// so a call that ran out of time can be told from one that was only slow; the
// consumer stops when no call is left using it.
func (l *Log) Replay(ctx context.Context, tenant string, fromSeq, toSeq uint64, fn func(LogMessage) error) error {
	if subject.LogTenantAll(tenant) == "" {
		return fmt.Errorf("cluster: replay: empty tenant: %w", tether.ErrInvalidInput)
	}

	if fn == nil {
		return fmt.Errorf("cluster: replay: nil handler: %w", tether.ErrInvalidInput)
	}

	fromSeq = max(fromSeq, 1)
	if fromSeq > toSeq {
		return nil
	}

	last, err := l.lastTenantSeq(ctx, tenant)
	if err != nil {
		return err
	}

	if fromSeq > last {
		return nil
	}

	w := &waiter{ctx: ctx, from: fromSeq, to: min(toSeq, last), fn: fn, done: make(chan struct{})}

	r, created := l.join(tenant, w)
	if created {
		go r.run()
	}

	select {
	case <-w.done:
		return w.result()
	case <-ctx.Done():
		// A fn that is running holds the waiter, so this waits for it.
		w.end(ctx.Err())

		state := r.state()

		r.shutdownIfIdle()

		err := w.result()
		if err != nil && errors.Is(err, ctx.Err()) {
			return fmt.Errorf("cluster: replay of seq %d to %d: %w (%s)", w.from, w.to, err, state)
		}

		return err
	}
}

// lastTenantSeq is the sequence of the last message tenant has in the log, 0
// when it has none. It reads that message, payload included, from the stream
// leader.
func (l *Log) lastTenantSeq(ctx context.Context, tenant string) (uint64, error) {
	ctx, cancel := opContext(ctx)
	defer cancel()

	st, err := l.stream(ctx)
	if err != nil {
		return 0, err
	}

	m, err := st.GetLastMsgForSubject(ctx, subject.LogTenantAll(tenant))
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return 0, nil
		}

		return 0, l.wrapErr("replay: last message of the tenant", err)
	}

	return m.Sequence, nil
}

// join adds w to a running replay consumer of tenant that can serve it, or
// registers a new one. created says the caller must start the new one.
func (l *Log) join(tenant string, w *waiter) (r *replayer, created bool) {
	l.rmu.Lock()
	defer l.rmu.Unlock()

	for _, r := range l.replayers[tenant] {
		if r.tryJoin(w) {
			return r, false
		}
	}

	r = &replayer{
		l: l, tenant: tenant, start: w.from, pos: w.from - 1, waiters: []*waiter{w}, stop: make(chan struct{}),
		stall: newStallWatch(l.replayStallAfter()), began: time.Now(),
	}
	l.replayers[tenant] = append(l.replayers[tenant], r)

	return r, true
}

// replayStallAfter is the stall watch bound of a replay consumer.
func (l *Log) replayStallAfter() time.Duration {
	if l.stallAfter > 0 {
		return l.stallAfter
	}

	return defaultReplayStallAfter
}

// waiter is one Replay call.
type waiter struct {
	ctx      context.Context //nolint:containedctx // the call's own context, read while it is waiting
	from, to uint64
	fn       func(LogMessage) error

	mu   sync.Mutex // held while fn runs
	fin  atomic.Bool
	err  error
	done chan struct{}
}

// deliver gives the message with sequence seq to the call if it is in the
// call's range (msg is nil when the entry did not decode), and ends the call
// when the range is done or fn fails.
func (w *waiter) deliver(seq uint64, msg *LogMessage) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fin.Load() {
		return
	}

	// A cancelled call gets no more messages, whether or not Replay has
	// noticed yet.
	if err := w.ctx.Err(); err != nil {
		w.finish(err)

		return
	}

	if seq >= w.from && seq <= w.to && msg != nil {
		if err := w.fn(*msg); err != nil {
			w.finish(fmt.Errorf("cluster: replay: handler failed at seq %d: %w", seq, err))

			return
		}
	}

	if seq >= w.to {
		w.finish(nil)
	}
}

// end finishes the call with err unless it is already finished.
func (w *waiter) end(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.fin.Load() {
		w.finish(err)
	}
}

// finish must run with w.mu held.
func (w *waiter) finish(err error) {
	w.err = err
	w.fin.Store(true)
	close(w.done)
}

func (w *waiter) result() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.err
}

// How far a replayer's goroutine got, for the error of a call whose context
// ended first.
const (
	phaseOpening  int32 = iota // making the consumer
	phaseChecking              // asking it what it holds (checkEmpty)
	phaseReading               // reading its messages
)

// replayer is one consumer shared by the Replay calls of a tenant.
type replayer struct {
	l      *Log
	tenant string
	start  uint64 // the first sequence the consumer reads
	stop   chan struct{}

	phase   atomic.Int32
	handled atomic.Int64 // messages the consumer handed out
	began   time.Time

	mu      sync.Mutex
	pos     uint64 // the last sequence handed out, or start-1
	waiters []*waiter
	closed  bool
	msgs    jetstream.MessagesContext
	own     iterOwner

	stall *stallWatch
}

// tryJoin adds w if the consumer will still read every message of w's range.
// The caller holds the Log's rmu, which is what keeps a close from slipping
// in between this check and the append.
func (r *replayer) tryJoin(w *waiter) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed || r.start > w.from || r.pos >= w.from {
		return false
	}

	r.waiters = append(r.waiters, w)

	return true
}

func (r *replayer) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.closed
}

func (r *replayer) run() {
	if r.isClosed() {
		return
	}

	cons, msgs, err := r.open()
	if err != nil {
		r.shutdown(err)

		return
	}

	defer r.l.dropConsumer(cons)

	r.phase.Store(phaseChecking)

	// A range with nothing of the tenant's left would never see a message to
	// tell it so.
	r.checkEmpty(cons)

	r.phase.Store(phaseReading)

	ended := make(chan struct{})
	defer close(ended)

	r.stall.progress()

	go r.stall.run(r.stop, ended, r.behind, r.stalled)

	if err := pumpMsgs(msgs, &r.own, r.stop, r.handle); err != nil {
		// The consumer failed and will not mend itself (a closed connection,
		// a recreate that did not work): the calls still waiting get the
		// error and try again on a new consumer.
		r.l.logger.Warn("log replay ended: its consumer failed", "tenant", r.tenant, "error", err)
		r.shutdown(fmt.Errorf("cluster: replay: %w: log consumer failed: %w", ErrUnavailable, err))
	}

	// Otherwise stop closed, which only closeLocked does: the calls are
	// settled already.
}

// behind is the stall watch's question: the furthest sequence a waiting call
// still needs, and the last one handed out. Every call's range ends at or
// below the tenant's last message when the call started, so a sequence still
// needed is in the log (unless it aged out, and then the calls are better
// ended too).
func (r *replayer) behind(context.Context) (want, at uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, w := range r.waiters {
		if !w.fin.Load() {
			want = max(want, w.to)
		}
	}

	return want, r.pos, nil
}

// stalled ends the calls of a consumer that has handed out nothing for
// stallAfter while they still need messages up to want.
func (r *replayer) stalled(want, at uint64) {
	r.l.logger.Error("log replay stalled: its consumer delivered nothing while calls wait for more; ending them",
		"tenant", r.tenant, "position", at, "wanted", want, "quiet", r.stall.quiet().Round(time.Millisecond))
	r.shutdown(fmt.Errorf("cluster: replay: %w: log consumer stalled at seq %d of %d", ErrUnavailable, at, want))
}

func (r *replayer) open() (jetstream.Consumer, jetstream.MessagesContext, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	cons, err := r.l.consumer(ctx, r.tenant, r.start, replayInactive)
	if err != nil {
		return nil, nil, err
	}

	msgs, err := r.l.messages(cons, replayPullHeartbeat)
	if err != nil {
		r.l.dropConsumer(cons)

		return nil, nil, r.l.wrapErr("replay: start", err)
	}

	r.mu.Lock()
	r.msgs = msgs
	closed := r.closed
	r.mu.Unlock()

	if closed {
		// Every call gave up while the consumer was being made.
		if r.own.takeForStop() {
			msgs.Stop()
		}
		r.l.dropConsumer(cons)

		return nil, nil, errors.New("replay stopped")
	}

	return cons, msgs, nil
}

// checkEmpty settles the calls when the consumer has nothing to deliver at
// all.
func (r *replayer) checkEmpty(cons jetstream.Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	ci, err := cons.Info(ctx)
	if err != nil {
		// The messages decide, or the calls' contexts do.
		r.l.logger.Debug("replay consumer info failed", "tenant", r.tenant, "error", err)

		return
	}

	if ci.NumPending == 0 && ci.Delivered.Consumer == 0 {
		r.settle()
	}
}

// settle ends the calls that have had everything the log holds for them, when
// the consumer says it has caught up. It does not take the consumer's word for
// it: a message can be appended after the server counted the consumer's
// pending messages and before a call read the tenant's last sequence, and that
// call's range would hold it. So settle reads the last sequence again and ends
// only the calls that were waiting before it asked, which cannot have a range
// past the answer. It runs on the consumer's goroutine, so nothing moves pos
// meanwhile.
func (r *replayer) settle() {
	r.mu.Lock()
	ws := slices.Clone(r.waiters)
	pos := r.pos
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	last, err := r.l.lastTenantSeq(ctx, r.tenant)
	if err != nil {
		r.l.logger.Debug("replay settle failed", "tenant", r.tenant, "error", err)

		return
	}

	if last > pos {
		return
	}

	for _, w := range ws {
		w.end(nil)
	}

	r.shutdownIfIdle()
}

func (r *replayer) handle(m jetstream.Msg) {
	r.stall.progress()
	defer r.stall.progress()

	r.handled.Add(1)

	r.stall.busy.Store(true)
	defer r.stall.busy.Store(false)

	md, err := m.Metadata()
	if err != nil {
		r.l.logger.Warn("log replay: message without metadata", "tenant", r.tenant, "error", err)

		return
	}

	seq := md.Sequence.Stream

	r.mu.Lock()
	r.pos = seq
	ws := slices.Clone(r.waiters)
	r.mu.Unlock()

	var (
		lm    *LogMessage
		tried bool
	)

	for _, w := range ws {
		if !tried && seq >= w.from && seq <= w.to && !w.fin.Load() {
			tried = true

			d, err := decodeLogMessage(m.Subject(), m.Headers(), m.Data())
			if err != nil {
				r.l.logger.Warn("log replay: skipping an entry that does not decode", "tenant", r.tenant, "seq", seq, "error", err)
			} else {
				d.Seq = seq
				lm = &d
			}
		}

		w.deliver(seq, lm)
	}

	if md.NumPending == 0 && slices.ContainsFunc(ws, func(w *waiter) bool { return !w.fin.Load() }) {
		r.settle()

		return
	}

	r.shutdownIfIdle()
}

// state says how far the consumer got, for a call that ran out of time.
func (r *replayer) state() string {
	r.mu.Lock()
	pos, closed := r.pos, r.closed
	waiting := 0

	for _, w := range r.waiters {
		if !w.fin.Load() {
			waiting++
		}
	}
	r.mu.Unlock()

	age := time.Since(r.began).Round(time.Millisecond)

	switch r.phase.Load() {
	case phaseOpening:
		return fmt.Sprintf("consumer from seq %d still being made after %s", r.start, age)
	case phaseChecking:
		return fmt.Sprintf("consumer from seq %d made, still being asked what it holds after %s", r.start, age)
	default:
		return fmt.Sprintf("consumer from seq %d reading for %s: %d messages handed out, last seq %d, quiet %s, %d other calls waiting, closed %v",
			r.start, age, r.handled.Load(), pos, r.stall.quiet().Round(time.Millisecond), waiting, closed)
	}
}

// shutdownIfIdle closes the consumer when no call is waiting on it.
func (r *replayer) shutdownIfIdle() {
	var msgs jetstream.MessagesContext

	r.l.rmu.Lock()

	r.mu.Lock()
	r.waiters = slices.DeleteFunc(r.waiters, func(w *waiter) bool { return w.fin.Load() })
	idle := len(r.waiters) == 0
	r.mu.Unlock()

	if idle {
		_, msgs = r.closeLocked()
	}

	r.l.rmu.Unlock()

	r.stopMsgs(msgs)
}

// shutdown closes the consumer and ends every call still waiting on it with
// err (nil for done).
func (r *replayer) shutdown(err error) {
	r.l.rmu.Lock()
	ws, msgs := r.closeLocked()
	r.l.rmu.Unlock()

	r.stopMsgs(msgs)

	for _, w := range ws {
		w.end(err)
	}
}

// closeLocked marks the consumer closed, takes it out of the Log's table and
// signals its goroutine to stop. The caller holds the Log's rmu. It returns the
// calls that were still waiting and the iterator to stop with stopMsgs once the
// lock is released.
func (r *replayer) closeLocked() ([]*waiter, jetstream.MessagesContext) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, nil
	}

	r.closed = true

	ws := r.waiters
	r.waiters = nil

	list := r.l.replayers[r.tenant]
	if i := slices.Index(list, r); i >= 0 {
		list = slices.Delete(list, i, i+1)
	}

	if len(list) == 0 {
		delete(r.l.replayers, r.tenant)
	} else {
		r.l.replayers[r.tenant] = list
	}

	close(r.stop)

	return ws, r.msgs
}

// stopMsgs stops the iterator in the background, unless the consumer's loop
// has it (pumpMsgs stops it then, once stop is closed): while NATS is
// unreachable the stop waits for the ordered consumer to finish an attempt to
// recreate itself, and the caller may be a Replay whose context has ended.
func (r *replayer) stopMsgs(msgs jetstream.MessagesContext) {
	if msgs != nil && r.own.takeForStop() {
		go msgs.Stop()
	}
}
