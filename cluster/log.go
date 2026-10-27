package cluster

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/subject"
)

// The headers of a log entry. The tenant and topic are not among them: they
// are in the subject, which subject.DecodeLog reads back losslessly. Props
// travel in the Tether-Prop-* headers (props.go).
const (
	hdrQoS         = "Tether-QoS"
	hdrRetain      = "Tether-Retain"
	hdrOrigin      = "Tether-Origin"
	hdrClient      = "Tether-Client"
	hdrPublishedAt = "Tether-Published-At"

	// ackTimeout is how long the log's publisher keeps an unanswered publish
	// before it gives up on the ack. Append's own context ends first in
	// practice, this only frees the bookkeeping of abandoned publishes.
	ackTimeout = 30 * time.Second
	// stallRetry is how long Append waits before it tries again after the
	// publisher refused a publish because too many acks are outstanding.
	stallRetry = 5 * time.Millisecond
)

// LogMessage is one QoS 1 or 2 message in the cell's log.
//
// A reader or Replay hands the same Payload and Props backing arrays to every
// caller that receives the message on one node. Do not modify them.
type LogMessage struct {
	// Seq is the message's sequence in the log stream. Append ignores it and
	// returns the sequence instead; readers set it.
	Seq uint64
	// Tenant and Topic say where the message was published. The log subject
	// is built from them, so every topic stays under its tenant.
	Tenant string
	Topic  string
	// Payload is the message body.
	Payload []byte
	// QoS is 1 or 2.
	QoS byte
	// Retain is the retain flag of the publish.
	Retain bool
	// Props holds the MQTT 5 publish properties.
	Props Props
	// Origin is the ID of the node that appended the message, "" when unset.
	Origin string
	// ClientID is the publisher's client ID within its tenant, "" for a
	// message the host published. A node delivering the message uses it for
	// the MQTT 5 No Local option. It travels base64url encoded, so any
	// client ID survives.
	ClientID string
	// PublishedAt is when the message was published. Append fills it with the
	// current time when it is zero. It is kept to the nanosecond, in UTC.
	PublishedAt time.Time
}

// Log appends to and reads the cell's QoS 1 and 2 log: one JetStream stream
// for the whole cell, with a tenant's messages under "d.<tenant>.>". Provision
// creates the stream, Log never does.
//
// Everything that delivers a QoS 1 or 2 message to a session reads it through
// a Reader or Replay, so the order of one tenant's messages in the stream is
// the order every session sees.
//
// It is safe for concurrent use.
type Log struct {
	c      *Conn
	js     jetstream.JetStream // publishes, with its own ack timeout
	name   string
	logger *slog.Logger
	st     atomic.Pointer[streamBox]

	rmu       sync.Mutex
	replayers map[string][]*replayer // by tenant

	// consumers counts the server consumers this Log created, for tests.
	consumers atomic.Int64
	// heartbeat is the idle heartbeat of the pull requests of readers and
	// replays, 0 for the nats.go default (5 s, so a dead server is noticed
	// after about 10 s). Tests shorten it.
	heartbeat time.Duration
	// resetAttempts is the MaxResetAttempts of the ordered consumers of
	// readers and replays, 0 for consumerResetAttempts. Tests set -1, nats.go's
	// endless loop, to reproduce a stall.
	resetAttempts int
	// stallAfter is how long a reader or replay consumer may deliver nothing
	// while the log holds more for it before it is ended, 0 for
	// defaultStallAfter. Tests shorten it.
	stallAfter time.Duration

	// onAppend is the OnAppend observer, nil for none.
	onAppend atomic.Pointer[func(time.Duration)]
}

// OnAppend makes fn observe every append that commits: Wait calls it with the
// time from the send to the commit it reports, before it returns. An append
// that fails is not reported. fn must be quick and safe for concurrent use;
// it is for metrics. nil removes it.
func (l *Log) OnAppend(fn func(time.Duration)) {
	if fn == nil {
		l.onAppend.Store(nil)

		return
	}

	l.onAppend.Store(&fn)
}

type streamBox struct{ s jetstream.Stream }

// NewLog returns the log of the cell cfg describes, over c. The stream must
// exist: call Provision first.
func NewLog(c *Conn, cfg Config) *Log {
	// A JetStream handle of its own, so the log can bound how long it keeps
	// an unanswered publish without touching the settings of the shared one.
	js, err := jetstream.New(c.nc, jetstream.WithPublishAsyncTimeout(ackTimeout))
	if err != nil {
		js = c.js
	}

	return &Log{
		c:         c,
		js:        js,
		name:      cfg.LogStream(),
		logger:    c.logger.With("log", cfg.LogStream()),
		replayers: make(map[string][]*replayer),
	}
}

func (l *Log) stream(ctx context.Context) (jetstream.Stream, error) {
	if b := l.st.Load(); b != nil {
		return b.s, nil
	}

	s, err := l.c.js.Stream(ctx, l.name)
	if err != nil {
		return nil, l.wrapErr("open log stream", err)
	}

	l.st.CompareAndSwap(nil, &streamBox{s: s})

	return l.st.Load().s, nil
}

func (l *Log) wrapErr(op string, err error) error {
	if isUnavailable(err) {
		return fmt.Errorf("cluster: %s: %w: %w", op, ErrUnavailable, err)
	}

	return fmt.Errorf("cluster: %s: %w", op, err)
}

// headerSafe reports whether s can sit in a NATS header value as it is: no
// control character, and no space at either end (the parser trims those).
func headerSafe(s string) bool {
	for i := range len(s) {
		if s[i] < 0x20 || s[i] == 0x7f {
			return false
		}
	}

	return s == "" || (s[0] != ' ' && s[len(s)-1] != ' ')
}

// Append stores msg in the log and returns its sequence once JetStream has
// committed it.
//
// The subject comes from msg.Tenant and msg.Topic. An empty tenant, a topic
// over 1024 bytes, a QoS other than 1 or 2, or an Origin or msgID that cannot
// be a header value fails with an error wrapping tether.ErrInvalidInput, and
// nothing is published. Seq is ignored. A zero PublishedAt becomes the current
// time.
//
// A non-empty msgID is sent as Nats-Msg-Id: a second Append with the same ID
// inside the stream's 2 minute duplicate window stores nothing, returns the
// first one's sequence and sets dup. Use it for QoS 2, where the publisher may
// resend a packet.
//
// The publish is asynchronous and Append waits for its ack, so many callers
// share the connection. A context without a deadline gets 10 s. A JetStream
// that cannot be reached, or does not answer in time, gives an error wrapping
// ErrUnavailable.
func (l *Log) Append(ctx context.Context, msg LogMessage, msgID string) (seq uint64, dup bool, err error) {
	ctx, cancel := opContext(ctx)
	defer cancel()

	p, err := l.AppendAsync(ctx, msg, msgID)
	if err != nil {
		return 0, false, err
	}

	return p.Wait(ctx)
}

// Ping reports whether the log stream has a leader that answers, which is
// what an append needs to commit: it asks for the stream's info, which only
// the leader answers (or, while the stream has none, a member that says so).
// nil means yes. No answer, or no leader, is an error wrapping
// ErrUnavailable. A context without a deadline gets 10 s. The broker uses it
// to learn that the log is back after an outage without sending appends into
// it.
func (l *Log) Ping(ctx context.Context) error {
	ctx, cancel := opContext(ctx)
	defer cancel()

	st, err := l.stream(ctx)
	if err != nil {
		return err
	}

	info, err := st.Info(ctx)
	if err != nil {
		return l.wrapErr("ping", err)
	}

	if info.Cluster != nil && info.Cluster.Leader == "" {
		return fmt.Errorf("cluster: ping: %w: the log stream has no leader", ErrUnavailable)
	}

	return nil
}

// PendingAppend is an append that has been sent and not yet acknowledged.
type PendingAppend struct {
	l    *Log
	fut  jetstream.PubAckFuture
	sent time.Time
}

// AppendAsync is Append in two steps: it sends msg and returns at once, and
// Wait on the result waits for the commit. Calls on one Log reach the stream
// in the order they were made, so a caller that starts its appends in order
// gets them committed in that order (unless one fails and is sent again).
// It checks msg and msgID as Append does. ctx bounds only the wait for the
// publisher, which holds a send back while 4000 acks are outstanding.
func (l *Log) AppendAsync(ctx context.Context, msg LogMessage, msgID string) (*PendingAppend, error) {
	m, err := encodeLogMessage(msg)
	if err != nil {
		return nil, err
	}

	if !headerSafe(msgID) {
		return nil, fmt.Errorf("cluster: append: message id is not a valid header value: %w", tether.ErrInvalidInput)
	}

	var opts []jetstream.PublishOpt
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}

	sent := time.Now()

	fut, err := l.publishAsync(ctx, m, opts)
	if err != nil {
		return nil, err
	}

	return &PendingAppend{l: l, fut: fut, sent: sent}, nil
}

// Wait waits for the commit of the append and returns its sequence, and dup
// when the message ID was a repeat inside the duplicate window. A context
// without a deadline gets 10 s. No ack in time, or a JetStream that cannot be
// reached, gives an error wrapping ErrUnavailable; the message may still have
// been committed, so send it again with the same message ID.
func (p *PendingAppend) Wait(ctx context.Context) (seq uint64, dup bool, err error) {
	ctx, cancel := opContext(ctx)
	defer cancel()

	select {
	case ack := <-p.fut.Ok():
		if fn := p.l.onAppend.Load(); fn != nil {
			(*fn)(time.Since(p.sent))
		}

		return ack.Sequence, ack.Duplicate, nil
	case err := <-p.fut.Err():
		return 0, false, p.l.wrapErr("append", err)
	case <-ctx.Done():
		return 0, false, p.l.wrapErr("append", ctx.Err())
	}
}

// publishAsync starts the publish, waiting while the publisher has too many
// acks outstanding.
func (l *Log) publishAsync(ctx context.Context, m *nats.Msg, opts []jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	for {
		fut, err := l.js.PublishMsgAsync(m, opts...)
		if err == nil {
			return fut, nil
		}

		if !errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			return nil, l.wrapErr("append", err)
		}

		select {
		case <-ctx.Done():
			return nil, l.wrapErr("append", ctx.Err())
		case <-time.After(stallRetry):
		}
	}
}

// encodeLogMessage builds the NATS message for msg, or says why it cannot.
func encodeLogMessage(msg LogMessage) (*nats.Msg, error) {
	if err := subject.CheckTopic(msg.Topic); err != nil {
		return nil, fmt.Errorf("cluster: append: %w", err)
	}

	subj := subject.Log(msg.Tenant, msg.Topic)
	if subj == "" {
		return nil, fmt.Errorf("cluster: append: empty tenant: %w", tether.ErrInvalidInput)
	}

	if msg.QoS != 1 && msg.QoS != 2 {
		return nil, fmt.Errorf("cluster: append: QoS %d is not 1 or 2: %w", msg.QoS, tether.ErrInvalidInput)
	}

	if !headerSafe(msg.Origin) {
		return nil, fmt.Errorf("cluster: append: origin is not a valid header value: %w", tether.ErrInvalidInput)
	}

	at := msg.PublishedAt
	if at.IsZero() {
		at = time.Now()
	}

	h := nats.Header{}
	h.Set(hdrQoS, strconv.Itoa(int(msg.QoS)))
	h.Set(hdrPublishedAt, strconv.FormatInt(at.UnixNano(), 10))

	if msg.Retain {
		h.Set(hdrRetain, "1")
	}

	if msg.Origin != "" {
		h.Set(hdrOrigin, msg.Origin)
	}

	if msg.ClientID != "" {
		h.Set(hdrClient, base64.RawURLEncoding.EncodeToString([]byte(msg.ClientID)))
	}

	msg.Props.encode(h)

	return &nats.Msg{Subject: subj, Header: h, Data: msg.Payload}, nil
}

// decodeLogMessage is the inverse of encodeLogMessage, for a message read back
// from the stream. The sequence is the caller's to set.
func decodeLogMessage(subj string, h nats.Header, data []byte) (LogMessage, error) {
	tenant, topic, err := subject.DecodeLog(subj)
	if err != nil {
		return LogMessage{}, fmt.Errorf("cluster: log entry: %w", err)
	}

	msg := LogMessage{Tenant: tenant, Topic: topic, Payload: data, Origin: h.Get(hdrOrigin), Retain: h.Get(hdrRetain) == "1"}

	q, err := strconv.ParseUint(h.Get(hdrQoS), 10, 8)
	if err != nil || (q != 1 && q != 2) {
		return LogMessage{}, fmt.Errorf("cluster: log entry %q: bad %s header %q", subj, hdrQoS, h.Get(hdrQoS))
	}

	msg.QoS = byte(q)

	if v := h.Get(hdrClient); v != "" {
		id, err := base64.RawURLEncoding.DecodeString(v)
		if err != nil {
			return LogMessage{}, fmt.Errorf("cluster: log entry %q: bad %s header: %w", subj, hdrClient, err)
		}

		msg.ClientID = string(id)
	}

	if v := h.Get(hdrPublishedAt); v != "" {
		ns, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return LogMessage{}, fmt.Errorf("cluster: log entry %q: bad %s header: %w", subj, hdrPublishedAt, err)
		}

		msg.PublishedAt = time.Unix(0, ns).UTC()
	}

	if err := msg.Props.decode(h); err != nil {
		return LogMessage{}, fmt.Errorf("cluster: log entry %q: %w", subj, err)
	}

	return msg, nil
}

// Get reads one message of tenant's from the log by its sequence, from the
// stream leader. A sequence that holds no message (aged out, or never used)
// or a message of another tenant gives an error wrapping tether.ErrNotFound;
// one that does not decode is an error. A context without a deadline gets
// 10 s; an unreachable JetStream wraps ErrUnavailable. Use it for a few
// scattered sequences; Replay reads a range.
func (l *Log) Get(ctx context.Context, tenant string, seq uint64) (LogMessage, error) {
	if subject.LogTenantAll(tenant) == "" || seq == 0 {
		return LogMessage{}, fmt.Errorf("cluster: log get: empty tenant or sequence 0: %w", tether.ErrInvalidInput)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	st, err := l.stream(ctx)
	if err != nil {
		return LogMessage{}, err
	}

	raw, err := st.GetMsg(ctx, seq)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return LogMessage{}, fmt.Errorf("cluster: log get %d: %w", seq, tether.ErrNotFound)
		}

		return LogMessage{}, l.wrapErr(fmt.Sprintf("log get %d", seq), err)
	}

	msg, err := decodeLogMessage(raw.Subject, raw.Header, raw.Data)
	if err != nil {
		return LogMessage{}, err
	}

	if msg.Tenant != tenant {
		return LogMessage{}, fmt.Errorf("cluster: log get %d: not the tenant's: %w", seq, tether.ErrNotFound)
	}

	msg.Seq = raw.Sequence

	return msg, nil
}
