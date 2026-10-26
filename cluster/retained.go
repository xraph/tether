package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/subject"
	mqtttopic "github.com/xraph/tether/topic"
)

// Retained stores the cell's retained messages in the retained bucket, one key
// per tenant and topic: subject.RetainedKey(tenant, topic), which puts a topic
// whose first level starts with "$" under "==<tenant>" so no ordinary filter
// reaches it. The last write of a topic wins; there is no compare-and-set.
//
// It is safe for concurrent use.
type Retained struct {
	c      *Conn
	bucket string
	h      atomic.Pointer[retainedHandles]

	// onWatch, when a test sets it, gets every KV watcher a Watch starts, so
	// the test can end one under the watch. beforeRestart, when set, runs
	// between a watch ending by itself and its replacement starting.
	onWatch       func(jetstream.KeyWatcher)
	beforeRestart func()
}

type retainedHandles struct {
	kv     jetstream.KeyValue
	prefix string // "$KV.<bucket>.", the subject of a key's messages
}

// retainedValue is the JSON value of a retained key. Tenant and topic are not
// in it: they come back from the key.
type retainedValue struct {
	Payload     []byte    `json:"payload"`
	QoS         byte      `json:"qos,omitempty"`
	Props       Props     `json:"props,omitzero"`
	Origin      string    `json:"origin,omitempty"`
	ClientID    string    `json:"client_id,omitempty"`
	PublishedAt time.Time `json:"published_at,omitzero"`
}

// NewRetained returns the retained store of the cell cfg describes, over c.
// The bucket must exist: call Provision first.
func NewRetained(c *Conn, cfg Config) *Retained {
	return &Retained{c: c, bucket: cfg.RetainedBucket()}
}

func (r *Retained) handles(ctx context.Context) (*retainedHandles, error) {
	if h := r.h.Load(); h != nil {
		return h, nil
	}

	kv, err := r.c.js.KeyValue(ctx, r.bucket)
	if err != nil {
		return nil, r.wrapErr("open retained bucket", err)
	}

	r.h.CompareAndSwap(nil, &retainedHandles{kv: kv, prefix: "$KV." + r.bucket + "."})

	return r.h.Load(), nil
}

func (r *Retained) wrapErr(op string, err error) error {
	if isUnavailable(err) {
		return fmt.Errorf("cluster: %s: %w: %w", op, ErrUnavailable, err)
	}

	return fmt.Errorf("cluster: %s: %w", op, err)
}

// retainedKey checks tenant and topic and returns the bucket key. It never
// returns "".
func retainedKey(op, tenant, topic string) (string, error) {
	if err := subject.CheckTopic(topic); err != nil {
		return "", fmt.Errorf("cluster: %s: %w", op, err)
	}

	if err := mqtttopic.Valid(topic, false, subject.MaxTopicBytes); err != nil {
		return "", fmt.Errorf("cluster: %s: %w", op, err)
	}

	key := subject.RetainedKey(tenant, topic)
	if key == "" {
		return "", fmt.Errorf("cluster: %s: empty tenant: %w", op, tether.ErrInvalidInput)
	}

	return key, nil
}

// retainedTenantFilters returns the two key filters that hold every retained
// message of tenant: "<tenant>.>" and "==<tenant>.>" for its "$" topics. It
// returns nil for an empty tenant.
func retainedTenantFilters(tenant string) []string {
	plain, _, _ := strings.Cut(subject.RetainedKey(tenant, "x"), ".")
	dollar, _, _ := strings.Cut(subject.RetainedKey(tenant, "$x"), ".")

	if plain == "" || dollar == "" {
		return nil
	}

	return []string{plain + ".>", dollar + ".>"}
}

// Put stores msg as the retained message of topic in tenant, replacing the one
// before. It returns once JetStream has committed the write.
//
// msg.Payload, QoS, Props, Origin, ClientID and PublishedAt are stored (a zero
// PublishedAt becomes the current time); Tenant, Topic, Seq and Retain are
// ignored. An empty payload deletes the topic's retained message instead, as
// MQTT says. A positive expiry makes the key expire after it, rounded up to
// whole seconds and at least one; pass the time left of the message's MQTT 5
// expiry interval. When it runs out JetStream replaces the message with a
// limit marker and watchers see the topic deleted.
//
// The topic must be a valid MQTT topic name of at most 1024 bytes, the tenant
// non-empty and QoS at most 2; otherwise Put fails with an error wrapping
// tether.ErrInvalidInput and writes nothing. A context without a deadline gets
// 10 s. A JetStream that cannot be reached, or does not answer in time, gives
// an error wrapping ErrUnavailable.
func (r *Retained) Put(ctx context.Context, tenant, topic string, msg LogMessage, expiry time.Duration) error {
	key, err := retainedKey("put retained", tenant, topic)
	if err != nil {
		return err
	}

	if len(msg.Payload) == 0 {
		return r.Delete(ctx, tenant, topic)
	}

	if msg.QoS > 2 {
		return fmt.Errorf("cluster: put retained: QoS %d: %w", msg.QoS, tether.ErrInvalidInput)
	}

	at := msg.PublishedAt
	if at.IsZero() {
		at = time.Now()
	}

	data, err := json.Marshal(retainedValue{
		Payload: msg.Payload, QoS: msg.QoS, Props: msg.Props,
		Origin: msg.Origin, ClientID: msg.ClientID, PublishedAt: at.UTC(),
	})
	if err != nil {
		return fmt.Errorf("cluster: put retained: encode: %w", err)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := r.handles(ctx)
	if err != nil {
		return err
	}

	m := nats.NewMsg(h.prefix + key)
	m.Data = data

	if expiry > 0 {
		m.Header.Set(jetstream.MsgTTLHeader, ttlHeader(expiry))
	}

	if _, err := r.c.js.PublishMsg(ctx, m); err != nil {
		return r.wrapErr("put retained", err)
	}

	return nil
}

// Delete removes the retained message of topic in tenant. It leaves a purge
// marker that expires after the bucket's limit marker TTL, so watchers see the
// delete. Deleting a topic that has no retained message is not an error. The
// checks, the default deadline and the errors are those of Put.
func (r *Retained) Delete(ctx context.Context, tenant, topic string) error {
	key, err := retainedKey("delete retained", tenant, topic)
	if err != nil {
		return err
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := r.handles(ctx)
	if err != nil {
		return err
	}

	m := nats.NewMsg(h.prefix + key)
	m.Header.Set(kvOpHeader, kvOpPurge)
	m.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	m.Header.Set(jetstream.MsgTTLHeader, ttlHeader(limitMarkerTTL))

	if _, err := r.c.js.PublishMsg(ctx, m); err != nil {
		return r.wrapErr("delete retained", err)
	}

	return nil
}

// decodeRetained builds the message of a retained entry. Seq is the entry's
// revision and Retain is set.
func decodeRetained(key string, rev uint64, data []byte) (LogMessage, error) {
	tenant, topic, err := subject.DecodeRetainedKey(key)
	if err != nil {
		return LogMessage{}, fmt.Errorf("cluster: retained key %q: %w", key, err)
	}

	var v retainedValue
	if err := json.Unmarshal(data, &v); err != nil {
		return LogMessage{}, fmt.Errorf("cluster: retained value of %q: %w", key, err)
	}

	return LogMessage{
		Seq: rev, Tenant: tenant, Topic: topic, Payload: v.Payload, QoS: v.QoS, Retain: true,
		Props: v.Props, Origin: v.Origin, ClientID: v.ClientID, PublishedAt: v.PublishedAt,
	}, nil
}

// Load returns every retained message of tenant, "$" topics included, sorted by
// topic. Each message has Tenant, Topic and Retain set, and Seq is its key's
// revision in the bucket, which grows with every write of the cell, so of two
// readings of one topic the higher Seq is the newer.
//
// It reads the tenant's keys through a watcher, so it costs a pass over them.
// The watcher's consumer may sit on a replica a few milliseconds behind the
// stream leader, so a write acknowledged just before Load may be missing; a
// cache kept by Watch gets it moments later. An entry that does not decode is
// logged at Warn and skipped. A context without a deadline gets 10 s.
func (r *Retained) Load(ctx context.Context, tenant string) ([]LogMessage, error) {
	filters := retainedTenantFilters(tenant)
	if filters == nil {
		return nil, fmt.Errorf("cluster: load retained: empty tenant: %w", tether.ErrInvalidInput)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := r.handles(ctx)
	if err != nil {
		return nil, err
	}

	w, err := h.kv.WatchFiltered(ctx, filters, jetstream.IgnoreDeletes())
	if err != nil {
		return nil, r.wrapErr("load retained", err)
	}
	defer r.stopWatcher(w)

	var out []LogMessage

	for {
		select {
		case <-ctx.Done():
			return nil, r.wrapErr("load retained", ctx.Err())
		case e, ok := <-w.Updates():
			if !ok {
				return nil, fmt.Errorf("cluster: load retained: watcher closed: %w", ErrUnavailable)
			}

			if e == nil {
				slices.SortFunc(out, func(a, b LogMessage) int { return strings.Compare(a.Topic, b.Topic) })

				return out, nil
			}

			m, derr := decodeRetained(e.Key(), e.Revision(), e.Value())
			if derr != nil {
				r.c.logger.Warn("skipping undecodable retained entry", "key", e.Key(), "error", derr)

				continue
			}

			out = append(out, m)
		}
	}
}

// stopWatcher stops w without blocking the caller, and drains it so the
// subscription can finish closing. nats.go's KV watcher delivers on a channel
// from the subscription's goroutine, which blocks on a full channel nobody
// reads and then never runs the close that ends Updates. The stop also deletes
// the watcher's consumer, a request that waits while NATS is unreachable.
func (r *Retained) stopWatcher(w jetstream.KeyWatcher) {
	go func() {
		for range w.Updates() { //nolint:revive // draining
		}
	}()

	go func() {
		if err := w.Stop(); err != nil {
			r.c.logger.Debug("stopping retained watcher", "error", err)
		}
	}()
}

// Watch keeps fn up to date with the retained messages of tenant, "$" topics
// included and no other tenant's.
//
// It starts with the messages that exist now: fn gets each as a put, and Watch
// returns once it has, so a cache filled by fn is loaded when Watch returns.
// After that fn gets every change: a put (msg is the new message, deleted is
// false) or a removal by Delete, by an empty Put or by expiry (JetStream
// reports an expiry as a purge with the reason MaxAge; msg then carries only
// Tenant, Topic and Seq, and deleted is true). msg.Seq is the key's revision,
// 0 for a removal found by a restarted watch (below). A removal is reported
// only for a topic fn holds, and a put only when its revision is newer than the
// last one fn saw for the topic, so fn sees each change of a topic once and in
// order.
//
// fn runs on the watch's one goroutine, one call at a time, and before Watch
// returns too, so do not call Watch while holding a lock fn takes. Keep fn
// short. An entry that does not decode is logged at Warn and skipped.
//
// A watch that ends by itself (its subscription closed under it) is logged at
// Error and started again, with backoff while NATS refuses. The new watch's
// first pass catches fn up: it reports the puts fn missed and, as removed, the
// topics fn holds that are gone.
//
// The watch runs until ctx ends or stop is called. Starting it waits for NATS
// at most until ctx's deadline, or 10 s without one, and the first pass at
// most until ctx ends. stop is idempotent and returns promptly even while NATS
// is unreachable: it waits only for an fn that is running, and fn is never
// called after it returns. Do not call stop from fn.
func (r *Retained) Watch(ctx context.Context, tenant string, fn func(topic string, msg *LogMessage, deleted bool)) (stop func(), err error) {
	filters := retainedTenantFilters(tenant)
	if filters == nil || fn == nil {
		return nil, fmt.Errorf("cluster: watch retained: needs a tenant and a callback: %w", tether.ErrInvalidInput)
	}

	wctx, cancel := context.WithCancel(ctx)

	w := &retainedWatch{
		r: r, tenant: tenant, filters: filters, fn: fn,
		known: make(map[string]uint64), ready: make(chan struct{}),
	}

	stop = func() {
		cancel()

		w.stopped.Store(true)
		w.fnMu.Lock()
		w.fnMu.Unlock() //nolint:gocritic,staticcheck // empty on purpose: waits out a running fn
	}

	kw, err := w.open(wctx)
	if err != nil {
		cancel()

		return nil, err
	}

	// run returns only once wctx has ended, so waiting for ready or ctx is
	// enough: a watch that ends during its first pass starts again.
	go w.run(wctx, kw)

	select {
	case <-w.ready:
		return stop, nil
	case <-ctx.Done():
		stop()

		return nil, r.wrapErr("watch retained", ctx.Err())
	}
}

// retainedWatch is one Watch. Only its run goroutine touches known and resync.
type retainedWatch struct {
	r       *Retained
	tenant  string
	filters []string
	fn      func(topic string, msg *LogMessage, deleted bool)

	known  map[string]uint64   // keys fn holds as live, and their revision
	resync map[string]struct{} // non-nil during a restarted watch's first pass: the keys it read

	ready     chan struct{}
	readyOnce sync.Once

	// fnMu is held while fn runs. stop sets stopped first and then takes
	// fnMu, so it waits for an fn that is running and none starts after it.
	fnMu    sync.Mutex
	stopped atomic.Bool
}

// open starts a KV watcher on the tenant's keys, waiting for NATS at most until
// ctx's deadline or 10 s.
func (w *retainedWatch) open(ctx context.Context) (jetstream.KeyWatcher, error) {
	octx, cancel := opContext(ctx)
	defer cancel()

	h, err := w.r.handles(octx)
	if err != nil {
		return nil, err
	}

	// WatchFiltered writes the bucket prefix into the slice it is given.
	kw, err := h.kv.WatchFiltered(octx, slices.Clone(w.filters))
	if err != nil {
		return nil, w.r.wrapErr("watch retained", err)
	}

	if hook := w.r.onWatch; hook != nil {
		hook(kw)
	}

	return kw, nil
}

func (w *retainedWatch) run(ctx context.Context, kw jetstream.KeyWatcher) {
	for {
		w.read(ctx, kw)
		w.r.stopWatcher(kw)

		if ctx.Err() != nil {
			return
		}

		w.r.c.logger.Error("watch: the retained watch ended by itself, restarting it", "tenant", w.tenant)

		if hook := w.r.beforeRestart; hook != nil {
			hook()
		}

		var ok bool
		if kw, ok = w.restart(ctx); !ok {
			return
		}
	}
}

// read consumes kw until it closes or ctx ends.
func (w *retainedWatch) read(ctx context.Context, kw jetstream.KeyWatcher) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-kw.Updates():
			if !ok {
				return
			}

			if e == nil {
				w.endFirstPass()
				w.readyOnce.Do(func() { close(w.ready) })

				continue
			}

			w.consume(e)
		}
	}
}

// restart opens a new watch with backoff until it works or ctx ends. Its first
// pass is a resync.
func (w *retainedWatch) restart(ctx context.Context) (jetstream.KeyWatcher, bool) {
	delay := consumerRetry

	for {
		kw, err := w.open(ctx)
		if err == nil {
			w.resync = make(map[string]struct{})

			return kw, true
		}

		w.r.c.logger.Error("watch: restarting the retained watch failed", "tenant", w.tenant, "error", err)

		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(delay):
		}

		delay = min(2*delay, watchRetryMax)
	}
}

func (w *retainedWatch) consume(e jetstream.KeyValueEntry) {
	key, rev := e.Key(), e.Revision()

	if w.resync != nil {
		w.resync[key] = struct{}{}
	}

	if e.Operation() != jetstream.KeyValuePut {
		if _, live := w.known[key]; !live {
			return
		}

		delete(w.known, key)
		w.fire(key, rev, nil)

		return
	}

	if last, live := w.known[key]; live && rev <= last {
		return
	}

	m, err := decodeRetained(key, rev, e.Value())
	if err != nil {
		w.r.c.logger.Warn("watch: skipping undecodable retained entry", "key", key, "error", err)

		return
	}

	w.known[key] = rev
	w.fire(key, rev, &m)
}

// endFirstPass finishes a restarted watch's first pass: a key fn holds that the
// pass did not read is gone (its marker expired while no watch ran).
func (w *retainedWatch) endFirstPass() {
	if w.resync == nil {
		return
	}

	for key := range w.known {
		if _, seen := w.resync[key]; !seen {
			delete(w.known, key)
			w.fire(key, 0, nil)
		}
	}

	w.resync = nil
}

// fire calls fn for key, unless the watch was stopped. msg nil means removed.
func (w *retainedWatch) fire(key string, rev uint64, msg *LogMessage) {
	tenant, topic, err := subject.DecodeRetainedKey(key)
	if err != nil {
		w.r.c.logger.Warn("watch: skipping undecodable retained key", "key", key, "error", err)

		return
	}

	deleted := msg == nil
	if deleted {
		msg = &LogMessage{Seq: rev, Tenant: tenant, Topic: topic}
	}

	w.fnMu.Lock()
	defer w.fnMu.Unlock()

	if w.stopped.Load() {
		return
	}

	w.fn(topic, msg, deleted)
}
