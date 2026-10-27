package cluster

import (
	"context"
	"encoding/json"
	"errors"
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
	// afterOpen, when set, runs in Load and in every watch start after the
	// KV watcher exists and before the leader's mark is read: a write it makes
	// is missing from the watcher's first pass but covered by the mark, which
	// is what a watcher on a lagging replica looks like.
	afterOpen func()
}

type retainedHandles struct {
	kv     jetstream.KeyValue
	prefix string // "$KV.<bucket>.", the subject of a key's messages
	getMsg string // the stream's MSG.GET API subject, answered by the leader
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

	r.h.CompareAndSwap(nil, &retainedHandles{
		kv:     kv,
		prefix: "$KV." + r.bucket + ".",
		getMsg: "$JS.API.STREAM.MSG.GET.KV_" + r.bucket,
	})

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

	if msg.QoS > 2 {
		return fmt.Errorf("cluster: put retained: QoS %d: %w", msg.QoS, tether.ErrInvalidInput)
	}

	if len(msg.Payload) == 0 {
		return r.Delete(ctx, tenant, topic)
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
// Every write acknowledged before Load was called is in the result. Load reads
// the tenant's keys through a watcher, whose consumer the server may place on
// a replica a few milliseconds behind the stream leader, so it first asks the
// leader for the newest revision under the tenant's keys and reads on until
// the watcher has delivered it (see catchUp). It costs a pass over the
// tenant's keys. An entry that does not decode is logged at Warn and skipped.
// A context without a deadline gets 10 s.
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

	// Markers stay in: the newest entry under the mark can be one.
	w, err := h.kv.WatchFiltered(ctx, slices.Clone(filters))
	if err != nil {
		return nil, r.wrapErr("load retained", err)
	}
	defer r.stopWatcher(w)

	if r.afterOpen != nil {
		r.afterOpen()
	}

	p, err := r.newCatchUp(ctx, h, filters)
	if err != nil {
		return nil, err
	}

	live := make(map[string]LogMessage)

	for !p.caughtUp() {
		select {
		case <-ctx.Done():
			return nil, r.wrapErr("load retained", ctx.Err())
		case <-p.tick():
			p.recheck(ctx)
		case e, ok := <-w.Updates():
			if !ok {
				return nil, fmt.Errorf("cluster: load retained: watcher closed: %w", ErrUnavailable)
			}

			if e == nil {
				p.firstPassEnded()

				continue
			}

			p.saw(e.Revision())

			if e.Operation() != jetstream.KeyValuePut {
				delete(live, e.Key())

				continue
			}

			m, derr := decodeRetained(e.Key(), e.Revision(), e.Value())
			if derr != nil {
				delete(live, e.Key())
				r.c.logger.Warn("skipping undecodable retained entry", "key", e.Key(), "error", derr)

				continue
			}

			live[e.Key()] = m
		}
	}

	out := make([]LogMessage, 0, len(live))
	for _, m := range live {
		out = append(out, m)
	}

	slices.SortFunc(out, func(a, b LogMessage) int { return strings.Compare(a.Topic, b.Topic) })

	return out, nil
}

// catchUpRecheck is how often a pass that has not caught up yet asks the
// leader for its mark again.
const catchUpRecheck = 250 * time.Millisecond

// catchUp tells when a KV watcher's pass over a tenant has caught up with the
// stream leader. The watcher's consumer is R1, and the server places it on any
// replica of the bucket, possibly a follower that has not applied the newest
// writes yet; it delivers them later, as updates after its first pass. So a
// pass reads, after its watcher exists, the leader's newest revision under the
// tenant's key filters (the mark: every write acknowledged before then is at
// or below it), and counts as done once the watcher has ended its first pass
// and delivered a revision at or above the mark.
//
// The entry at the mark can disappear before the watcher delivers it: a purge
// marker that runs out its TTL leaves nothing behind. So while it waits the
// pass asks the leader again every catchUpRecheck and lowers the mark to the
// answer; it never raises it, so steady writes cannot hold the pass back.
//
// A catchUp belongs to one goroutine.
type catchUp struct {
	r       *Retained
	h       *retainedHandles
	filters []string
	mark    uint64
	seen    uint64
	ended   bool // the watcher sent the end of its first pass
	done    bool
}

func (r *Retained) newCatchUp(ctx context.Context, h *retainedHandles, filters []string) (*catchUp, error) {
	mark, err := r.leaderMark(ctx, h, filters)
	if err != nil {
		return nil, err
	}

	return &catchUp{r: r, h: h, filters: filters, mark: mark}, nil
}

func (p *catchUp) saw(rev uint64)  { p.seen = max(p.seen, rev) }
func (p *catchUp) firstPassEnded() { p.ended = true }

// caughtUp reports whether the pass is done. Once true it stays true.
func (p *catchUp) caughtUp() bool {
	if !p.done && p.ended && p.seen >= p.mark {
		p.done = true
	}

	return p.done
}

// tick returns a channel that fires when the mark is due a recheck, nil once
// the pass is done.
func (p *catchUp) tick() <-chan time.Time {
	if p.done {
		return nil
	}

	return time.After(catchUpRecheck)
}

// recheck asks the leader for the mark again and keeps the lower one. A
// failed read keeps the mark as it is.
func (p *catchUp) recheck(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()

	mark, err := p.r.leaderMark(rctx, p.h, p.filters)
	if err != nil {
		p.r.c.logger.Debug("retained: rereading the leader's mark failed", "error", err)

		return
	}

	p.mark = min(p.mark, mark)
}

// leaderMark returns the newest revision under any of filters, 0 when none
// has a message, from the stream leader: the JetStream non-direct message get
// with last_by_subj, which takes a wildcard subject.
func (r *Retained) leaderMark(ctx context.Context, h *retainedHandles, filters []string) (uint64, error) {
	var mark uint64

	for _, f := range filters {
		body, err := json.Marshal(msgGetRequest{LastBySubject: h.prefix + f})
		if err != nil {
			return 0, fmt.Errorf("cluster: retained leader mark: %w", err)
		}

		reply, err := r.c.nc.RequestWithContext(ctx, h.getMsg, body)
		if err != nil {
			return 0, r.wrapErr("retained leader mark", err)
		}

		var resp msgGetResponse
		if err := json.Unmarshal(reply.Data, &resp); err != nil {
			return 0, fmt.Errorf("cluster: retained leader mark: bad reply: %w", err)
		}

		switch {
		case resp.Error != nil && resp.Error.ErrorCode == jetstream.JSErrCodeMessageNotFound:
		case resp.Error != nil:
			return 0, r.wrapErr("retained leader mark", resp.Error)
		case resp.Message == nil:
			return 0, errors.New("cluster: retained leader mark: empty reply")
		default:
			mark = max(mark, resp.Message.Sequence)
		}
	}

	return mark, nil
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
// Every write acknowledged before Watch was called is in that first pass, as
// with Load (the pass waits for the leader's mark, see catchUp). After that fn
// gets every change: a put (msg is the new message, deleted is false) or a
// removal by Delete, by an empty Put or by expiry (JetStream reports an expiry
// as a purge with the reason MaxAge; msg then carries only Tenant, Topic and
// Seq, and deleted is true). msg.Seq is the key's revision, 0 for a removal
// found by a restarted watch (below). A removal is reported only for a topic
// fn holds, and a put only when its revision is newer than the last one fn saw
// for the topic, so fn sees each change of a topic once and in order.
//
// fn runs on the watch's one goroutine, one call at a time, and before Watch
// returns too, so do not call Watch while holding a lock fn takes. Keep fn
// short. An entry that does not decode is logged at Warn and skipped.
//
// A watch that ends by itself (its subscription closed under it) is logged at
// Error and started again, with backoff while NATS refuses. The new watch's
// first pass, caught up with the leader the same way, catches fn up: it
// reports the puts fn missed and, as removed, the topics fn holds that are
// gone.
//
// ctx is the watch's lifetime: it runs until ctx ends or Stop is called. The
// start (the watcher, the leader's mark and the first pass) has a bound of its
// own, 10 s, or ctx's deadline when that comes first; when it runs out Watch
// fails with an error wrapping ErrUnavailable. Stop is idempotent and returns
// promptly even while NATS is unreachable: it waits only for an fn that is
// running, and fn is never called after it returns. Do not call Stop from fn.
//
// The returned handle's Sync catches the watch up with the store: see
// RetainedWatch.Sync.
func (r *Retained) Watch(ctx context.Context, tenant string, fn func(topic string, msg *LogMessage, deleted bool)) (*RetainedWatch, error) {
	filters := retainedTenantFilters(tenant)
	if filters == nil || fn == nil {
		return nil, fmt.Errorf("cluster: watch retained: needs a tenant and a callback: %w", tether.ErrInvalidInput)
	}

	wctx, cancel := context.WithCancel(ctx)

	startCtx, cancelStart := context.WithTimeout(wctx, opTimeout)
	defer cancelStart()

	w := &retainedWatch{
		r: r, tenant: tenant, filters: filters, fn: fn,
		known: make(map[string]uint64), ready: make(chan struct{}), seenCh: make(chan struct{}),
	}

	stop := func() {
		cancel()

		w.stopped.Store(true)
		w.fnMu.Lock()
		w.fnMu.Unlock() //nolint:gocritic,staticcheck // empty on purpose: waits out a running fn
	}

	kw, end, pass, err := w.open(wctx, startCtx)
	if err != nil {
		cancel()

		return nil, err
	}

	// run returns only once wctx has ended, so waiting for ready or the start
	// bound is enough: a watch that ends during its first pass starts again.
	go w.run(wctx, kw, end, pass)

	select {
	case <-w.ready:
		return &RetainedWatch{w: w, stop: stop, done: wctx.Done()}, nil
	case <-startCtx.Done():
		stop()

		return nil, r.wrapErr("watch retained: first pass", startCtx.Err())
	}
}

// RetainedWatch is a running Watch.
type RetainedWatch struct {
	w    *retainedWatch
	stop func()
	done <-chan struct{} // closed when the watch has ended
}

// Stop ends the watch. It is idempotent and returns promptly even while NATS
// is unreachable: it waits only for a callback that is running, and the
// callback is never called after it returns. Do not call it from the
// callback.
func (h *RetainedWatch) Stop() { h.stop() }

// Sync returns once the watch has caught up with the store as of the call:
// every write to the tenant's keys acknowledged before Sync was called has
// been through the callback (or, for a removal of a topic the callback never
// held, skipped). It reads the stream leader's newest revision under the
// tenant's two key filters (the mark, as the first pass does) and waits until
// the watch has seen an entry at or above it. It counts every entry the
// watch read, whatever the callback was told, so a removal of a topic the
// callback does not hold still moves it on. While it waits the mark is read
// again every 250 ms and only lowered, for an entry at the mark that left
// the stream before the watch read it (a TTL that ran out).
//
// It costs one or two requests to the stream leader. A context without a
// deadline gets 10 s; when it runs out, or the leader cannot be read, the
// error wraps ErrUnavailable. On a watch that has ended it fails with an
// error wrapping ErrUnavailable.
func (h *RetainedWatch) Sync(ctx context.Context) error {
	w := h.w

	ctx, cancel := opContext(ctx)
	defer cancel()

	hd, err := w.r.handles(ctx)
	if err != nil {
		return err
	}

	mark, err := w.r.leaderMark(ctx, hd, w.filters)
	if err != nil {
		return err
	}

	tick := time.NewTicker(catchUpRecheck)
	defer tick.Stop()

	for {
		select {
		case <-h.done:
			return fmt.Errorf("cluster: sync retained: the watch has ended: %w", ErrUnavailable)
		default:
		}

		seen, advanced := w.seenNow()
		if seen >= mark {
			return nil
		}

		select {
		case <-advanced:
		case <-h.done:
			return fmt.Errorf("cluster: sync retained: the watch has ended: %w", ErrUnavailable)
		case <-ctx.Done():
			return w.r.wrapErr("sync retained", ctx.Err())
		case <-tick.C:
			if m, err := w.r.leaderMark(ctx, hd, w.filters); err == nil {
				mark = min(mark, m)
			}
		}
	}
}

// retainedWatch is one Watch. Only its run goroutine touches known, resync and
// the pass.
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

	// seen is the highest revision of an entry the watch has read and handed
	// to consume (whatever fn was told), for Sync; seenCh is closed and
	// replaced each time it grows. A restarted watch's first pass counts only
	// once it has ended, when its removals have been reported.
	seenMu sync.Mutex
	seen   uint64
	seenCh chan struct{}
}

// advance records that the watch has read every entry up to rev.
func (w *retainedWatch) advance(rev uint64) {
	w.seenMu.Lock()
	defer w.seenMu.Unlock()

	if rev > w.seen {
		w.seen = rev
		close(w.seenCh)
		w.seenCh = make(chan struct{})
	}
}

// seenNow returns seen and the channel that closes when it next grows.
func (w *retainedWatch) seenNow() (seen uint64, advanced <-chan struct{}) {
	w.seenMu.Lock()
	defer w.seenMu.Unlock()

	return w.seen, w.seenCh
}

// open starts a KV watcher on the tenant's keys and reads the leader's mark
// for its first pass.
//
// nats.go ends a KV watcher's subscription when the context it was created
// with ends, and uses that same context, deadline or not, for the request that
// creates its consumer. So the watcher gets a context of its own under life,
// cancelled by the returned func, and bound limits only the start: when bound
// ends during the create call the watcher's context is cancelled with it.
func (w *retainedWatch) open(life, bound context.Context) (jetstream.KeyWatcher, context.CancelFunc, *catchUp, error) {
	h, err := w.r.handles(bound)
	if err != nil {
		return nil, nil, nil, err
	}

	lctx, lcancel := context.WithCancel(life)
	unbind := context.AfterFunc(bound, lcancel)

	// WatchFiltered writes the bucket prefix into the slice it is given.
	kw, err := h.kv.WatchFiltered(lctx, slices.Clone(w.filters))

	if !unbind() { // bound ended during the call, and lctx with it
		if err == nil {
			w.r.stopWatcher(kw)
		}

		return nil, nil, nil, w.r.wrapErr("watch retained", bound.Err())
	}

	if err != nil {
		lcancel()

		return nil, nil, nil, w.r.wrapErr("watch retained", err)
	}

	if hook := w.r.onWatch; hook != nil {
		hook(kw)
	}

	if hook := w.r.afterOpen; hook != nil {
		hook()
	}

	pass, err := w.r.newCatchUp(bound, h, w.filters)
	if err != nil {
		lcancel()
		w.r.stopWatcher(kw)

		return nil, nil, nil, err
	}

	return kw, lcancel, pass, nil
}

func (w *retainedWatch) run(ctx context.Context, kw jetstream.KeyWatcher, end context.CancelFunc, pass *catchUp) {
	for {
		w.read(ctx, kw, pass)
		end()
		w.r.stopWatcher(kw)

		if ctx.Err() != nil {
			return
		}

		w.r.c.logger.Error("watch: the retained watch ended by itself, restarting it", "tenant", w.tenant)

		if hook := w.r.beforeRestart; hook != nil {
			hook()
		}

		var ok bool
		if kw, end, pass, ok = w.restart(ctx); !ok {
			return
		}
	}
}

// read consumes kw until it closes or ctx ends. When pass catches up it ends
// the first pass (endFirstPass) and marks the watch ready.
func (w *retainedWatch) read(ctx context.Context, kw jetstream.KeyWatcher, pass *catchUp) {
	for {
		if !pass.done && pass.caughtUp() {
			w.endFirstPass()
			w.advance(pass.seen)
			w.readyOnce.Do(func() { close(w.ready) })
		}

		select {
		case <-ctx.Done():
			return
		case <-pass.tick():
			pass.recheck(ctx)
		case e, ok := <-kw.Updates():
			if !ok {
				return
			}

			if e == nil {
				pass.firstPassEnded()

				continue
			}

			pass.saw(e.Revision())
			w.consume(e)

			if w.resync == nil {
				w.advance(e.Revision())
			}
		}
	}
}

// restart opens a new watch with backoff until it works or ctx ends. Its first
// pass is a resync.
func (w *retainedWatch) restart(ctx context.Context) (jetstream.KeyWatcher, context.CancelFunc, *catchUp, bool) {
	delay := consumerRetry

	for {
		bound, cancel := context.WithTimeout(ctx, opTimeout)
		kw, end, pass, err := w.open(ctx, bound)

		cancel()

		if err == nil {
			w.resync = make(map[string]struct{})

			return kw, end, pass, true
		}

		w.r.c.logger.Error("watch: restarting the retained watch failed", "tenant", w.tenant, "error", err)

		select {
		case <-ctx.Done():
			return nil, nil, nil, false
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
