package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
)

// The kinds of control event, each the subject suffix of its messages
// ("ctl.<kind>") and the kind a handler subscribes to.
const (
	KindKeyInvalidated = "key.invalidated"
	KindRoleChanged    = "role.changed"
	KindSubjectChanged = "subject.changed"
	KindTenantUpdated  = "tenant.updated"
	KindSessionKick    = "session.kick"
	KindSessionDelete  = "session.delete"
)

// KeyInvalidated says a keysmith key changed. Disconnect is true when the key
// may no longer be used at all, and the clients that connected with it must
// go. A rotation with a grace window has Disconnect false and GraceEnds set:
// the previous secret works until then (RotatedAt is when the rotation ran).
type KeyInvalidated struct {
	KeyID      string    `json:"key_id"`
	Disconnect bool      `json:"disconnect,omitempty"`
	RotatedAt  time.Time `json:"rotated_at,omitzero"`
	GraceEnds  time.Time `json:"grace_ends,omitzero"`
}

// RoleChanged says a warden role or its topic grants changed.
type RoleChanged struct {
	RoleID string `json:"role_id"`
}

// SubjectChanged says the role assignments of a warden subject changed. Kind
// is the subject kind (user, service account, device key) and ID its ID.
type SubjectChanged struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// TenantUpdated says a tenant changed, so every node drops what it cached
// about it.
type TenantUpdated struct {
	TenantID string `json:"tenant_id"`
}

// SessionKick asks the node that runs a session to disconnect its client.
type SessionKick struct {
	Tenant   string `json:"tenant"`
	ClientID string `json:"client_id"`
}

// SessionDelete asks the node that runs a session to end it and delete it.
type SessionDelete struct {
	Tenant   string `json:"tenant"`
	ClientID string `json:"client_id"`
}

const ctlPrefix = "ctl."

// kindRe is what a kind may look like: dot-separated tokens of letters, digits,
// "-" and "_", so "ctl.<kind>" is a literal NATS subject.
var kindRe = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)

const maxKindBytes = 128

func checkKind(op, kind string) error {
	if len(kind) > maxKindBytes || !kindRe.MatchString(kind) {
		return fmt.Errorf("cluster: %s: bad kind %q: %w", op, kind, tether.ErrInvalidInput)
	}

	return nil
}

// errControlStopped is returned by Start and Subscribe after Stop.
var errControlStopped = fmt.Errorf("cluster: control bus stopped: %w", ErrUnavailable)

// Control is the cell's control bus: events every node must apply, such as a
// revoked key or a session another node runs that has to be kicked. Events go
// to the control stream (Config.ControlStream, subjects "ctl.>", kept an
// hour) as JSON on "ctl.<kind>", and each node reads the stream with one
// ordered consumer that starts at the end of the stream when the node starts.
//
// Every node, the publisher included, gets each event published while its
// Control runs exactly once and in stream order. An event published while a
// node was down is not replayed to it when it comes back: a restarted node has
// no clients, and they reauthorize when they reconnect.
//
// It is safe for concurrent use.
type Control struct {
	c      *Conn
	stream string
	logger *slog.Logger

	mu       sync.Mutex
	handlers map[string][]func([]byte)
	started  bool
	closed   bool
	run      *controlRun   // the running consumer, nil before Start
	quit     chan struct{} // closed by Stop
	quitOnce sync.Once

	// heartbeat is the idle heartbeat of the consumer's pull requests, 0 for
	// pullHeartbeat. Tests shorten it.
	heartbeat time.Duration
	// resetAttempts is the consumer's MaxResetAttempts, 0 for
	// consumerResetAttempts; stallAfter is how long it may deliver nothing
	// while the stream holds more, 0 for defaultStallAfter. Tests set them.
	resetAttempts int
	stallAfter    time.Duration
}

// NewControl returns the control bus of the cell cfg describes, over c. The
// stream must exist: call Provision first. Register handlers with Subscribe,
// then call Start.
func NewControl(c *Conn, cfg Config) *Control {
	return &Control{
		c:        c,
		stream:   cfg.ControlStream(),
		logger:   c.logger.With("control", cfg.ControlStream()),
		handlers: make(map[string][]func([]byte)),
		quit:     make(chan struct{}),
	}
}

// Subscribe registers fn for events of kind. Several handlers per kind are
// called in the order they were registered. It makes no NATS call and may run
// before or after Start; an event that arrives before any handler of its kind
// is registered is dropped, so register them before Start. Events of a kind
// nobody subscribed to are ignored.
//
// fn gets the event's JSON, which it shares with the other handlers of the
// kind: do not modify it. Handlers run on the bus's one goroutine, one at a
// time, so a handler must not block: hand real work to a goroutine of your
// own. A handler must not call Stop.
//
// A kind that is not dot-separated tokens of [-_A-Za-z0-9] or a nil fn is an
// error wrapping tether.ErrInvalidInput; after Stop the error wraps
// ErrUnavailable.
func (c *Control) Subscribe(kind string, fn func([]byte)) error {
	if err := checkKind("control subscribe", kind); err != nil {
		return err
	}

	if fn == nil {
		return fmt.Errorf("cluster: control subscribe: nil handler: %w", tether.ErrInvalidInput)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errControlStopped
	}

	c.handlers[kind] = append(c.handlers[kind], fn)

	return nil
}

// Publish sends v as JSON to every node, the caller's included, and returns
// once JetStream has stored it. It does not need Start. A bad kind is an
// error wrapping tether.ErrInvalidInput. A context without a deadline gets
// 10 s. A JetStream that cannot be reached, or does not answer in time, gives
// an error wrapping ErrUnavailable; the event may still have been stored.
func (c *Control) Publish(ctx context.Context, kind string, v any) error {
	if err := checkKind("control publish", kind); err != nil {
		return err
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cluster: control publish %s: encode: %w", kind, err)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	if _, err := c.c.js.PublishMsg(ctx, &nats.Msg{Subject: ctlPrefix + kind, Data: data}); err != nil {
		return c.wrapErr("control publish "+kind, err)
	}

	return nil
}

func (c *Control) wrapErr(op string, err error) error {
	if isUnavailable(err) {
		return fmt.Errorf("cluster: %s: %w: %w", op, ErrUnavailable, err)
	}

	return fmt.Errorf("cluster: %s: %w", op, err)
}

// Start creates the node's consumer, at the end of the control stream, and
// starts handing events to the handlers. The consumer exists on the server
// when Start returns, so an event published after that reaches this node.
// When the consumer fails for good (it cannot be recreated, or it delivers
// nothing for a while although the stream holds more) the bus logs it at
// Error and starts a new consumer from the event after the last one handled,
// retrying with backoff until Stop, so no event is skipped.
// Making the first consumer is retried while nobody answers (retryUnanswered).
// ctx bounds the start only (10 s when it has no deadline). Once only: a
// second call is an error, unless the first one failed. After Stop it fails
// with an error wrapping ErrUnavailable.
func (c *Control) Start(ctx context.Context) error {
	c.mu.Lock()

	switch {
	case c.closed:
		c.mu.Unlock()

		return errControlStopped
	case c.started:
		c.mu.Unlock()

		return errors.New("cluster: control bus: already started")
	}

	c.started = true
	c.mu.Unlock()

	ctx, cancel := opContext(ctx)
	defer cancel()

	// The stream lookup and the consumer's create are sent again while
	// nobody answers them, as while the control stream elects a leader.
	var r *controlRun

	err := retryUnanswered(ctx, func(actx context.Context) error {
		var oerr error

		r, oerr = c.open(actx, 0)

		return oerr
	})
	if err != nil {
		c.mu.Lock()
		c.started = false
		c.mu.Unlock()

		return err
	}

	c.mu.Lock()

	if c.closed {
		// Stop ran while the consumer was being made.
		c.mu.Unlock()
		r.halt()

		go r.cleanup(context.WithoutCancel(ctx))

		return errControlStopped
	}

	c.run = r
	c.mu.Unlock()

	go c.serve(r)

	return nil
}

// serve runs r and, each time its consumer ends without a Stop, starts a new
// one from the event after the last handled.
func (c *Control) serve(r *controlRun) {
	for {
		err := r.loop()

		if r.stopped.Load() && !r.stalled.Load() {
			return
		}

		c.logger.Error("control bus: consumer ended by itself, restarting it", "after", r.last.Load(), "error", err)

		if r = c.reopen(r.last.Load() + 1); r == nil {
			return
		}
	}
}

// reopen makes the consumer that goes on from start, retrying with backoff,
// and installs it. nil after Stop.
func (c *Control) reopen(start uint64) *controlRun {
	delay := consumerRetry

	for {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		r, err := c.open(ctx, start)

		cancel()

		if err == nil {
			c.mu.Lock()

			if c.closed {
				c.mu.Unlock()
				r.halt()
				r.cleanup(context.Background())

				return nil
			}

			c.run = r
			c.mu.Unlock()

			return r
		}

		c.logger.Error("control bus: restarting the consumer failed", "from", start, "error", err)

		select {
		case <-c.quit:
			return nil
		case <-time.After(delay):
		}

		delay = min(2*delay, watchRetryMax)
	}
}

// open makes a consumer that reads from start, or from the end of the stream
// when start is 0.
func (c *Control) open(ctx context.Context, start uint64) (*controlRun, error) {
	st, err := c.c.js.Stream(ctx, c.stream)
	if err != nil {
		return nil, c.wrapErr("control bus: open stream", err)
	}

	if start == 0 {
		info, ierr := st.Info(ctx)
		if ierr != nil {
			return nil, c.wrapErr("control bus: stream info", ierr)
		}

		// An explicit start sequence and not "new": the ordered consumer may
		// be recreated before its first message, and "new" would then mean
		// new as of the recreation.
		start = info.State.LastSeq + 1
	}

	resets := c.resetAttempts
	if resets == 0 {
		resets = consumerResetAttempts
	}

	cons, err := st.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{ctlPrefix + ">"},
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       start,
		InactiveThreshold: readerInactive,
		MaxResetAttempts:  resets,
	})
	if err != nil {
		return nil, c.wrapErr("control bus: create consumer", err)
	}

	msgs, err := cons.Messages(jetstream.PullHeartbeat(pullHeartbeatOr(c.heartbeat)))

	r := &controlRun{c: c, st: st, cons: cons, msgs: msgs, stop: make(chan struct{}), stall: newStallWatch(c.stallAfter)}
	r.last.Store(start - 1)

	if err != nil {
		go r.cleanup(context.WithoutCancel(ctx))

		return nil, c.wrapErr("control bus: start consumer", err)
	}

	return r, nil
}

// Stop ends the bus. When it returns no handler is running and none runs
// again: it waits for a handler that is running, and nothing more reaches the
// handlers after that. It does not wait for the consumer, whose shutdown and
// delete run in the background (the ordered consumer can take 20 s or more to
// leave its recreate loop while NATS is unreachable), so it returns promptly
// even then. Stop is idempotent, and Subscribe, Start and an unstarted bus all
// accept it. Do not call it from a handler.
func (c *Control) Stop() {
	c.mu.Lock()
	c.closed = true
	r := c.run
	c.run = nil
	c.mu.Unlock()

	c.quitOnce.Do(func() { close(c.quit) })

	if r != nil {
		r.halt()
	}
}

// Done is closed once Stop has been called.
func (c *Control) Done() <-chan struct{} { return c.quit }

const (
	// republishFirst is the wait before Republish's first attempt, doubling
	// to republishMax; republishAttempt bounds each attempt.
	republishFirst   = 250 * time.Millisecond
	republishMax     = 5 * time.Second
	republishAttempt = 5 * time.Second
)

// Republish calls publish, a control bus publish that has failed once, until
// it succeeds, done closes (nil: never), or the control stream's MaxAge (an
// hour) has passed since the call, waiting 250 ms before the first attempt
// and doubling the wait up to 5 s. Each attempt gets a context bounded at
// 5 s. An error wrapping tether.ErrInvalidInput ends it at once, since
// sending it again cannot help. It returns nil once publish succeeded, else
// the last error.
//
// Control events are idempotent, so a caller that applied a change on its
// own node when the first publish failed (ruling R41) uses it to reach the
// rest of the cell once the bus is back (ruling R51), and applies the event
// a second time when it comes back.
func Republish(done <-chan struct{}, publish func(ctx context.Context) error) error {
	deadline := time.Now().Add(controlMaxAge)
	delay := republishFirst

	var last error

	for {
		select {
		case <-done:
			return last
		case <-time.After(delay):
		}

		ctx, cancel := context.WithTimeout(context.Background(), republishAttempt)
		last = publish(ctx)

		cancel()

		if last == nil || errors.Is(last, tether.ErrInvalidInput) || time.Now().After(deadline) {
			return last
		}

		delay = min(2*delay, republishMax)
	}
}

// controlRun is the running consumer of a Control.
type controlRun struct {
	c     *Control
	st    jetstream.Stream
	cons  jetstream.Consumer
	msgs  jetstream.MessagesContext
	last  atomic.Uint64 // the stream sequence of the last event handled; moves under fnMu
	stop  chan struct{}
	once  sync.Once
	stall *stallWatch
	own   iterOwner

	// fnMu is held while handlers run. halt sets stopped first and then
	// takes fnMu, so it waits for handlers that are running and none start
	// after it, and last is final. stalled says the stall watch halted it,
	// not Stop.
	fnMu    sync.Mutex
	stopped atomic.Bool
	stalled atomic.Bool
}

// halt stops handing events to handlers and has the iterator shut down in
// the background: by the run's loop when it has it (pumpMsgs), else here.
func (r *controlRun) halt() {
	r.once.Do(func() {
		r.stopped.Store(true)
		r.fnMu.Lock()
		r.fnMu.Unlock() //nolint:gocritic,staticcheck // empty on purpose: waits out a running handler

		close(r.stop)

		if r.msgs != nil && r.own.takeForStop() {
			go r.msgs.Stop()
		}
	})
}

// loop reads events until the run is halted, which returns nil, or the
// iterator fails, which returns the error (see Log.pump), then deletes the
// consumer.
func (r *controlRun) loop() error {
	defer r.cleanup(context.Background())

	ended := make(chan struct{})
	defer close(ended)

	r.stall.progress()

	go r.stall.run(r.stop, ended, r.behind, r.onStall)

	// pumpMsgs stops the iterator when it ends. No handler runs after it,
	// so last is final.
	return pumpMsgs(r.msgs, &r.own, r.stop, r.handle)
}

// behind is the stall watch's question: the stream's last sequence (every
// message of the control stream is an event) and the last event handled.
func (r *controlRun) behind(ctx context.Context) (want, at uint64, err error) {
	info, err := r.st.Info(ctx)
	if err != nil {
		return 0, 0, err
	}

	return info.State.LastSeq, r.last.Load(), nil
}

// onStall halts a run whose consumer has delivered nothing for stallAfter
// while the stream holds events up to want, past the last one handled; serve
// then starts a new consumer from there.
func (r *controlRun) onStall(want, at uint64) {
	r.c.logger.Error("control bus stalled: its consumer delivered nothing while the stream holds more; replacing it",
		"last_handled", at, "stream_last", want, "quiet", r.stall.quiet().Round(time.Millisecond))
	r.stalled.Store(true)
	r.halt()
}

// cleanup deletes the consumer on the server, best effort: the server would
// remove it after its inactive threshold anyway.
func (r *controlRun) cleanup(ctx context.Context) {
	info := r.cons.CachedInfo()
	if info == nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, cleanupWait)
	defer cancel()

	if err := r.st.DeleteConsumer(ctx, info.Name); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		r.c.logger.Debug("control bus consumer cleanup failed", "consumer", info.Name, "error", err)
	}
}

func (r *controlRun) handle(msg jetstream.Msg) {
	r.stall.progress()
	defer r.stall.progress()

	md, err := msg.Metadata()
	if err != nil {
		r.c.logger.Warn("control bus: event without metadata", "error", err)

		return
	}

	seq := md.Sequence.Stream
	kind := strings.TrimPrefix(msg.Subject(), ctlPrefix)

	r.c.mu.Lock()
	fns := slices.Clone(r.c.handlers[kind])
	r.c.mu.Unlock()

	data := msg.Data()

	r.fnMu.Lock()
	defer r.fnMu.Unlock()

	// After a halt nothing runs and last stays where it is, so a new run
	// starts at the first event this one did not handle.
	if r.stopped.Load() {
		return
	}

	// The ordered consumer never hands an event out twice, but a sequence at
	// or below the last one would be a repeat, and each event runs once.
	if seq <= r.last.Load() {
		return
	}

	r.last.Store(seq)

	if len(fns) == 0 {
		r.c.logger.Debug("control bus: no handler for event", "kind", kind, "seq", seq)

		return
	}

	r.stall.busy.Store(true)
	defer r.stall.busy.Store(false)

	for _, fn := range fns {
		fn(data)
	}
}
