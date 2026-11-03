package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
)

const (
	// loadReportLifetimes is how many LoadIntervals a report lives: two
	// missed reports in a row still leave the last one up, and the third
	// takes it down.
	loadReportLifetimes = 3
	// loadHeaderRoom is what a report leaves unused under Conn.MaxPayload,
	// for the subject, the Nats-TTL header and their framing.
	loadHeaderRoom = 1 << 10
)

// TenantLoad is one tenant's share of a node's load. It mirrors the broker's
// per-tenant usage, with the connection time in milliseconds. The JSON tags
// are short because a node reports every tenant it has every LoadInterval.
type TenantLoad struct {
	// Connections is how many clients of the tenant are connected to the node.
	Connections int `json:"c"`
	// MessagesIn and MessagesOut count the tenant's publishes received from
	// and delivered to its clients, BytesIn and BytesOut their payload bytes.
	// ConnectionMillis is the time its connections have been up.
	MessagesIn       uint64 `json:"mi"`
	MessagesOut      uint64 `json:"mo"`
	BytesIn          uint64 `json:"bi"`
	BytesOut         uint64 `json:"bo"`
	ConnectionMillis uint64 `json:"cm"`
}

// NodeLoad is one node's load report: the value of its key in the load
// bucket.
type NodeLoad struct {
	// NodeID is the reporting node. Report fills it in; a watch sets it from
	// the key.
	NodeID string `json:"node_id"`
	// StartedAt is when the node started, so a reader can tell a restarted
	// node (whose counters began again) from one that kept running.
	StartedAt time.Time `json:"started_at"`
	// At is when the report was made. Report stamps it with the current time
	// when it is zero. It is the reporting node's clock: never compare it with
	// another node's.
	At time.Time `json:"at"`
	// Tenants is the load per tenant.
	Tenants map[string]TenantLoad `json:"tenants,omitempty"`
}

// Load publishes this node's load report to the cell and watches the other
// nodes' reports, which gives every node the cell-wide connection counts and
// usage of every tenant.
//
// Each node writes its whole report under its own key (the node ID) in the
// load bucket, replacing the last, with a lifetime of three LoadIntervals. A
// node that stops, crashes or loses NATS stops refreshing it, and JetStream
// removes it, so a dead node's numbers leave its peers' view on their own and
// no ghost key is left to clean up.
//
// It is safe for concurrent use.
type Load struct {
	c        *Conn
	nodeID   string
	bucket   string
	interval time.Duration
	h        atomic.Pointer[loadHandles]

	// onWatch, when a test sets it, gets every KV watcher a Watch starts, so
	// the test can end one under the watch.
	onWatch func(jetstream.KeyWatcher)
}

type loadHandles struct {
	kv     jetstream.KeyValue
	prefix string // "$KV.<bucket>.", the subject of a key's messages
	getMsg string // the stream's MSG.GET API subject, answered by the leader
}

// NewLoad returns the load reporter of the node cfg describes, over c. The
// load bucket must exist: call Provision first. A LoadInterval that is not
// positive means DefaultLoadInterval.
func NewLoad(c *Conn, cfg Config) *Load {
	interval := cfg.LoadInterval
	if interval <= 0 {
		interval = DefaultLoadInterval
	}

	return &Load{c: c, nodeID: cfg.NodeID, bucket: cfg.LoadBucket(), interval: interval}
}

func (l *Load) handles(ctx context.Context) (*loadHandles, error) {
	if h := l.h.Load(); h != nil {
		return h, nil
	}

	kv, err := l.c.js.KeyValue(ctx, l.bucket)
	if err != nil {
		return nil, l.wrapErr("open load bucket", err)
	}

	l.h.CompareAndSwap(nil, &loadHandles{
		kv:     kv,
		prefix: "$KV." + l.bucket + ".",
		getMsg: "$JS.API.STREAM.MSG.GET.KV_" + l.bucket,
	})

	return l.h.Load(), nil
}

func (l *Load) wrapErr(op string, err error) error {
	if isTooLarge(err) {
		return wrapTooLarge(op, err)
	}

	if isUnavailable(err) {
		return fmt.Errorf("cluster: %s: %w: %w", op, ErrUnavailable, err)
	}

	return fmt.Errorf("cluster: %s: %w", op, err)
}

// Report publishes r as this node's load report, replacing the last one. r is
// not changed: Report sends a copy with NodeID set to this node (r.NodeID must
// be empty or this node's) and At set to the current time when it is zero. The
// report lives three LoadIntervals (rounded up to whole seconds, one second at
// least); report again within that to keep it.
//
// It is a plain publish to the key's subject with a Nats-TTL header, the way
// Sessions.Expire writes, without an expected sequence: each key has one
// writer, this node, and every report replaces the one before whole, so there
// is nothing to compare against, and a write whose acknowledgment was lost
// can simply be sent again.
//
// A report whose JSON is over Conn.MaxPayload minus 1 KiB is refused before it
// is sent, with an error wrapping ErrTooLarge and tether.ErrInvalidInput (so
// is a nil report or another node's). A NATS error that says it could not be
// reached or did not answer in time wraps ErrUnavailable. A call without a
// context deadline gets 10 s.
func (l *Load) Report(ctx context.Context, r *NodeLoad) error {
	if r == nil {
		return fmt.Errorf("cluster: report load: nil report: %w", tether.ErrInvalidInput)
	}

	if err := checkNodeID(l.nodeID); err != nil {
		return fmt.Errorf("cluster: report load: %w: %w", err, tether.ErrInvalidInput)
	}

	if r.NodeID != "" && r.NodeID != l.nodeID {
		return fmt.Errorf("cluster: report load: node %q reporting as %q: %w", l.nodeID, r.NodeID, tether.ErrInvalidInput)
	}

	v := *r
	v.NodeID = l.nodeID
	v.StartedAt = v.StartedAt.UTC()

	if v.At.IsZero() {
		v.At = time.Now()
	}

	v.At = v.At.UTC()

	data, err := json.Marshal(&v)
	if err != nil {
		return fmt.Errorf("cluster: report load: encode: %w", err)
	}

	if limit := l.c.MaxPayload(); limit > 0 && int64(len(data)) > limit-loadHeaderRoom {
		return fmt.Errorf("cluster: report load: %w: %w: %d bytes, the limit is %d (max_payload %d less %d for headers)",
			ErrTooLarge, tether.ErrInvalidInput, len(data), limit-loadHeaderRoom, limit, loadHeaderRoom)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := l.handles(ctx)
	if err != nil {
		return err
	}

	msg := nats.NewMsg(h.prefix + l.nodeID)
	msg.Data = data
	msg.Header.Set(jetstream.MsgTTLHeader, ttlHeader(loadReportLifetimes*l.interval))

	if _, err := l.c.js.PublishMsg(ctx, msg); err != nil {
		return l.wrapErr("report load", err)
	}

	return nil
}

// Withdraw removes this node's load report from the bucket, so its peers
// stop counting it at once instead of after its lifetime. A node calls it
// when it stops gracefully, after its last report and once its clients are
// gone; reporting again after it puts the report back.
//
// It is a purge of the node's key, whose marker lives as long as a report
// (three LoadIntervals) and then leaves nothing behind. A Watch reports it as
// a removal. Withdrawing a key that is already gone is not an error. A NATS
// error that says it could not be reached or did not answer in time wraps
// ErrUnavailable; the report's own lifetime still takes it down then. A call
// without a context deadline gets 10 s.
func (l *Load) Withdraw(ctx context.Context) error {
	if err := checkNodeID(l.nodeID); err != nil {
		return fmt.Errorf("cluster: withdraw load: %w: %w", err, tether.ErrInvalidInput)
	}

	ctx, cancel := opContext(ctx)
	defer cancel()

	h, err := l.handles(ctx)
	if err != nil {
		return err
	}

	// The marker's TTL in whole seconds, one at least, as ttlHeader rounds
	// a report's.
	ttl := max(time.Second, (loadReportLifetimes*l.interval + time.Second - 1).Truncate(time.Second))

	if err := h.kv.Purge(ctx, l.nodeID, jetstream.PurgeTTL(ttl)); err != nil {
		return l.wrapErr("withdraw load", err)
	}

	return nil
}

// Watch keeps fn up to date with every node's load report, this node's
// included.
//
// It starts with the reports that exist now: fn gets each as a put, and Watch
// returns once it has, so a table filled by fn is loaded when Watch returns.
// Every report acknowledged before Watch was called is in that first pass: the
// pass waits for the stream leader's newest revision, because the watch's
// consumer can sit on a replica that is a few milliseconds behind. After that
// fn gets every change. A put has deleted false and the report, with NodeID
// taken from the key. A removal has deleted true and r nil: a report that
// ran out its lifetime (JetStream reports that as a purge with the reason
// MaxAge) or a key that was deleted or purged. A removal is reported only for
// a node fn holds, and a put only when its revision is newer than the last
// one fn saw for the node, so fn sees each change of a node once and in
// order.
//
// fn runs on the watch's one goroutine, one call at a time, and before Watch
// returns too, so do not call Watch while holding a lock fn takes. Keep fn
// short. An entry that does not decode is logged at Warn and skipped.
//
// A watch that ends by itself (its subscription closed under it, which nats.go
// does to a watcher it cannot recreate) is logged at Error and started again,
// with backoff (200 ms doubling to 5 s) while NATS refuses, and never goes
// silent. The new watch's first pass, caught up with the leader the same way,
// catches fn up: it reports the puts fn missed and, as removed, the nodes fn
// holds that are gone.
//
// ctx is the watch's lifetime: it runs until ctx ends or stop is called. The
// start (opening the bucket, the watcher and the first pass) has a bound of
// its own, 10 s or ctx's deadline when that comes first, and each unanswered
// request in it is sent again (retryUnanswered); when the bound runs out
// Watch fails with an error wrapping ErrUnavailable. stop is idempotent and
// returns promptly even while NATS is unreachable: it waits only for an fn
// that is running, and fn is not called after it returns. Do not call stop
// from fn.
func (l *Load) Watch(ctx context.Context, fn func(nodeID string, r *NodeLoad, deleted bool)) (stop func(), err error) {
	if fn == nil {
		return nil, fmt.Errorf("cluster: watch load: needs a callback: %w", tether.ErrInvalidInput)
	}

	wctx, cancel := context.WithCancel(ctx)

	startCtx, cancelStart := opContext(wctx)
	defer cancelStart()

	var h *loadHandles

	err = retryUnanswered(startCtx, func(actx context.Context) error {
		var herr error

		h, herr = l.handles(actx)

		return herr
	})
	if err != nil {
		cancel()

		return nil, err
	}

	w := &loadWatch{l: l, h: h, fn: fn, known: make(map[string]uint64), ready: make(chan struct{})}

	stop = func() {
		cancel()

		w.stopped.Store(true)
		w.fnMu.Lock()
		w.fnMu.Unlock() //nolint:gocritic,staticcheck // empty on purpose: waits out a running fn
	}

	var (
		kw   jetstream.KeyWatcher
		pass *loadPass
	)

	err = retryUnanswered(startCtx, func(actx context.Context) error {
		var oerr error

		kw, pass, oerr = w.open(wctx, actx)

		return oerr
	})
	if err != nil {
		cancel()

		return nil, err
	}

	// run returns only once wctx has ended, so waiting for ready or the start
	// bound is enough: a watch that ends during its first pass starts again.
	go w.run(wctx, kw, pass)

	select {
	case <-w.ready:
		return stop, nil
	case <-startCtx.Done():
		stop()

		return nil, l.wrapErr("watch load: first pass", startCtx.Err())
	}
}

// loadWatch is one Watch. Only its run goroutine touches known, resync and
// the pass.
type loadWatch struct {
	l  *Load
	h  *loadHandles
	fn func(nodeID string, r *NodeLoad, deleted bool)

	known  map[string]uint64   // nodes fn holds, and the revision of their report
	resync map[string]struct{} // non-nil during a restarted watch's first pass: the keys it read

	ready     chan struct{}
	readyOnce sync.Once

	// fnMu is held while fn runs. stop sets stopped first and then takes
	// fnMu, so it waits for an fn that is running and none starts after it.
	fnMu    sync.Mutex
	stopped atomic.Bool
}

// open starts a KV watcher on every key and reads the leader's mark for its
// first pass. life is the watcher's lifetime; bound limits the mark request.
func (w *loadWatch) open(life, bound context.Context) (jetstream.KeyWatcher, *loadPass, error) {
	kw, err := w.h.kv.WatchAll(life)
	if err != nil {
		return nil, nil, w.l.wrapErr("watch load", err)
	}

	if hook := w.l.onWatch; hook != nil {
		hook(kw)
	}

	mark, err := w.l.leaderMark(bound, w.h)
	if err != nil {
		stopKVWatcher(kw, w.l.c.logger)

		return nil, nil, err
	}

	return kw, &loadPass{mark: mark}, nil
}

func (w *loadWatch) run(ctx context.Context, kw jetstream.KeyWatcher, pass *loadPass) {
	for {
		w.read(ctx, kw, pass)
		stopKVWatcher(kw, w.l.c.logger)

		if ctx.Err() != nil {
			return
		}

		w.l.c.logger.Error("watch: the load watch ended by itself, restarting it")

		var ok bool
		if kw, pass, ok = w.restart(ctx); !ok {
			return
		}
	}
}

// read consumes kw until it closes or ctx ends. When pass catches up it ends
// the first pass (endFirstPass) and marks the watch ready.
func (w *loadWatch) read(ctx context.Context, kw jetstream.KeyWatcher, pass *loadPass) {
	for {
		if pass.caughtUp() {
			w.endFirstPass()
			w.readyOnce.Do(func() { close(w.ready) })
		}

		select {
		case <-ctx.Done():
			return
		case <-pass.tick():
			pass.recheck(ctx, w)
		case e, ok := <-kw.Updates():
			if !ok {
				return
			}

			if e == nil {
				pass.ended = true

				continue
			}

			pass.seen = max(pass.seen, e.Revision())
			w.consume(e)
		}
	}
}

// restart opens a new watch with backoff until it works or ctx ends. Its first
// pass is a resync.
func (w *loadWatch) restart(ctx context.Context) (jetstream.KeyWatcher, *loadPass, bool) {
	delay := consumerRetry

	for {
		bound, cancel := context.WithTimeout(ctx, opTimeout)
		kw, pass, err := w.open(ctx, bound)

		cancel()

		if err == nil {
			w.resync = make(map[string]struct{})

			return kw, pass, true
		}

		w.l.c.logger.Error("watch: restarting the load watch failed", "error", err)

		select {
		case <-ctx.Done():
			return nil, nil, false
		case <-time.After(delay):
		}

		delay = min(2*delay, watchRetryMax)
	}
}

func (w *loadWatch) consume(e jetstream.KeyValueEntry) {
	key, rev := e.Key(), e.Revision()

	if w.resync != nil {
		w.resync[key] = struct{}{}
	}

	if e.Operation() != jetstream.KeyValuePut {
		if _, live := w.known[key]; !live {
			return
		}

		delete(w.known, key)
		w.fire(key, nil)

		return
	}

	if last, live := w.known[key]; live && rev <= last {
		return
	}

	var r NodeLoad
	if err := json.Unmarshal(e.Value(), &r); err != nil {
		w.l.c.logger.Warn("watch: skipping undecodable load report", "key", key, "error", err)

		return
	}

	r.NodeID = key

	w.known[key] = rev
	w.fire(key, &r)
}

// endFirstPass finishes a restarted watch's first pass: a node fn holds that
// the pass did not read is gone (its marker expired while no watch ran).
func (w *loadWatch) endFirstPass() {
	if w.resync == nil {
		return
	}

	for key := range w.known {
		if _, seen := w.resync[key]; !seen {
			delete(w.known, key)
			w.fire(key, nil)
		}
	}

	w.resync = nil
}

// fire calls fn for key, unless the watch was stopped. r nil means removed.
func (w *loadWatch) fire(key string, r *NodeLoad) {
	w.fnMu.Lock()
	defer w.fnMu.Unlock()

	if w.stopped.Load() {
		return
	}

	w.fn(key, r, r == nil)
}

// loadPass tells when a KV watcher's pass over the load bucket has caught up
// with the stream leader. The watcher's consumer can be placed on a replica
// that has not applied the newest writes yet; it delivers them later, as
// updates after its first pass. So a pass reads, after its watcher exists, the
// leader's newest revision (the mark: every write acknowledged before then is
// at or below it), and counts as done once the watcher has ended its first
// pass and delivered a revision at or above the mark.
//
// The entry at the mark can disappear before the watcher delivers it: a
// report that runs out its lifetime leaves nothing behind but a marker, at a
// higher revision. So while it waits the pass asks the leader again every
// catchUpRecheck and lowers the mark to the answer; it never raises it, so
// steady writes cannot hold the pass back.
//
// A loadPass belongs to one goroutine.
type loadPass struct {
	mark  uint64
	seen  uint64
	ended bool // the watcher sent the end of its first pass
	done  bool
}

// caughtUp reports whether the pass is done. Once true it stays true.
func (p *loadPass) caughtUp() bool {
	if !p.done && p.ended && p.seen >= p.mark {
		p.done = true
	}

	return p.done
}

// tick returns a channel that fires when the mark is due a recheck, nil once
// the pass is done.
func (p *loadPass) tick() <-chan time.Time {
	if p.done {
		return nil
	}

	return time.After(catchUpRecheck)
}

// recheck asks the leader for the mark again and keeps the lower one. A failed
// read keeps the mark as it is.
func (p *loadPass) recheck(ctx context.Context, w *loadWatch) {
	rctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()

	mark, err := w.l.leaderMark(rctx, w.h)
	if err != nil {
		w.l.c.logger.Debug("load: rereading the leader's mark failed", "error", err)

		return
	}

	p.mark = min(p.mark, mark)
}

// leaderMark returns the newest revision in the bucket, 0 when it holds no
// message, from the stream leader: the JetStream non-direct message get with
// last_by_subj, which takes a wildcard subject.
func (l *Load) leaderMark(ctx context.Context, h *loadHandles) (uint64, error) {
	body, err := json.Marshal(msgGetRequest{LastBySubject: h.prefix + ">"})
	if err != nil {
		return 0, fmt.Errorf("cluster: load leader mark: %w", err)
	}

	reply, err := l.c.nc.RequestWithContext(ctx, h.getMsg, body)
	if err != nil {
		return 0, l.wrapErr("load leader mark", err)
	}

	var resp msgGetResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return 0, fmt.Errorf("cluster: load leader mark: bad reply: %w", err)
	}

	switch {
	case resp.Error != nil && resp.Error.ErrorCode == jetstream.JSErrCodeMessageNotFound:
		return 0, nil
	case resp.Error != nil:
		return 0, l.wrapErr("load leader mark", resp.Error)
	case resp.Message == nil:
		return 0, errors.New("cluster: load leader mark: empty reply")
	default:
		return resp.Message.Sequence, nil
	}
}

// stopKVWatcher stops w without blocking the caller, and drains it so the
// subscription can finish closing. nats.go's KV watcher delivers on a channel
// from the subscription's goroutine, which blocks on a full channel nobody
// reads and then never runs the close that ends Updates. The stop also deletes
// the watcher's consumer, a request that waits while NATS is unreachable.
func stopKVWatcher(w jetstream.KeyWatcher, logger *slog.Logger) {
	go func() {
		for range w.Updates() { //nolint:revive // draining
		}
	}()

	go func() {
		if err := w.Stop(); err != nil {
			logger.Debug("stopping kv watcher", "error", err)
		}
	}()
}
