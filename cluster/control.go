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
	run      *controlRun // the running consumer, nil before Start

	// heartbeat is the idle heartbeat of the consumer's pull requests, 0 for
	// the nats.go default (5 s). Tests shorten it.
	heartbeat time.Duration
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

	r, err := c.open(ctx)
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

	go r.loop(context.WithoutCancel(ctx))

	return nil
}

func (c *Control) open(ctx context.Context) (*controlRun, error) {
	st, err := c.c.js.Stream(ctx, c.stream)
	if err != nil {
		return nil, c.wrapErr("control bus: open stream", err)
	}

	info, err := st.Info(ctx)
	if err != nil {
		return nil, c.wrapErr("control bus: stream info", err)
	}

	// An explicit start sequence and not "new": the ordered consumer may be
	// recreated before its first message, and "new" would then mean new as of
	// the recreation.
	start := info.State.LastSeq + 1

	cons, err := st.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{ctlPrefix + ">"},
		DeliverPolicy:     jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:       start,
		InactiveThreshold: readerInactive,
	})
	if err != nil {
		return nil, c.wrapErr("control bus: create consumer", err)
	}

	var msgs jetstream.MessagesContext
	if c.heartbeat > 0 {
		msgs, err = cons.Messages(jetstream.PullHeartbeat(c.heartbeat))
	} else {
		msgs, err = cons.Messages()
	}

	r := &controlRun{c: c, st: st, cons: cons, msgs: msgs, last: start - 1, stop: make(chan struct{})}

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

	if r != nil {
		r.halt()
	}
}

// controlRun is the running consumer of a Control.
type controlRun struct {
	c    *Control
	st   jetstream.Stream
	cons jetstream.Consumer
	msgs jetstream.MessagesContext
	last uint64 // the stream sequence of the last event handled, run goroutine only
	stop chan struct{}
	once sync.Once

	// fnMu is held while handlers run. halt sets stopped first and then
	// takes fnMu, so it waits for handlers that are running and none start
	// after it.
	fnMu    sync.Mutex
	stopped atomic.Bool
}

// halt stops handing events to handlers and shuts the iterator down in the
// background.
func (r *controlRun) halt() {
	r.once.Do(func() {
		r.stopped.Store(true)
		r.fnMu.Lock()
		r.fnMu.Unlock() //nolint:gocritic,staticcheck // empty on purpose: waits out a running handler

		close(r.stop)

		if r.msgs != nil {
			go r.msgs.Stop()
		}
	})
}

// loop reads events until the iterator stops, then deletes the consumer with
// ctx (which must not end with Start).
func (r *controlRun) loop(ctx context.Context) {
	defer r.cleanup(ctx)

	for {
		msg, err := r.msgs.Next()
		if err == nil {
			r.handle(msg)

			continue
		}

		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			return
		}

		select {
		case <-r.stop:
			return
		default:
		}

		r.c.logger.Warn("control bus consumer error", "error", err)

		select {
		case <-r.stop:
			return
		case <-time.After(consumerRetry):
		}
	}
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
	md, err := msg.Metadata()
	if err != nil {
		r.c.logger.Warn("control bus: event without metadata", "error", err)

		return
	}

	// The ordered consumer never hands an event out twice, but a sequence at
	// or below the last one would be a repeat, and each event runs once.
	seq := md.Sequence.Stream
	if seq <= r.last {
		return
	}

	r.last = seq

	kind := strings.TrimPrefix(msg.Subject(), ctlPrefix)

	r.c.mu.Lock()
	fns := slices.Clone(r.c.handlers[kind])
	r.c.mu.Unlock()

	if len(fns) == 0 {
		r.c.logger.Debug("control bus: no handler for event", "kind", kind, "seq", seq)

		return
	}

	data := msg.Data()

	r.fnMu.Lock()
	defer r.fnMu.Unlock()

	if r.stopped.Load() {
		return
	}

	for _, fn := range fns {
		fn(data)
	}
}
