package broker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/topic"
)

// Retained messages in cluster mode.
//
// The cell keeps its retained messages in the retained bucket
// (cluster.Retained), and every retained publish is written there: a
// client's at any QoS, the host's (Publish, DevicePublish, DeleteRetained)
// and a retained will. mochi's retained store is each node's cache of the
// bucket. A tenant's messages are loaded into it when the node gets the
// tenant's first connection, and a watch keeps them current, applying every
// put, delete and expiry the bucket reports, in revision order, through
// Topics.RetainMessage (so mochi's OnRetainMessage never runs for them). A
// SUBSCRIBE of a tenant whose load has not finished waits for it, up to
// retainedLoadWait, in OnSubscribe, so mochi's own retained delivery after
// the SUBACK reads a loaded cache. The cache stops with the tenant's log
// reader, when the tenant has had no connection here for readerIdle.
//
// mochi never stores a retained publish itself in cluster mode: the hook
// clears the retain flag of every publish it lets through and tags the
// packet instead (tagRetained), and OnPacketEncode sets the flag again for a
// 5.0 subscriber whose matching subscription has Retain As Published.
//
// A node applies its own retained publishes to its cache at once, before it
// writes them to the bucket, so a client that publishes a retained message
// and then subscribes on the same node gets it. That cannot leave the cache
// stale: the write is issued after the local apply, so the watch reports it,
// and anything written after it, later than the apply, and the last report
// wins. Only a write that fails for good would leave the local apply behind;
// then the cache gets back what the watch last reported for the topic
// (repairRetained).

const (
	// retainedLoadWait bounds how long a SUBSCRIBE waits for its tenant's
	// retained messages to be loaded.
	retainedLoadWait = 2 * time.Second
	// retainedShards is how many queues carry the writes nobody waits for;
	// a topic's writes always share one, in order.
	retainedShards = 16
	// retainedQueueMax caps each queue; a write over it is dropped.
	retainedQueueMax = 4096
	// retainedWriteTimeout bounds one write to the bucket.
	retainedWriteTimeout = 5 * time.Second
	// retainedRetry and retainedRetryMax pace the retries of a failed write,
	// doubling; retainedRetryWindow is how long a write is retried for.
	retainedRetry       = 100 * time.Millisecond
	retainedRetryMax    = 5 * time.Second
	retainedRetryWindow = 30 * time.Second
	// retainedDrain bounds the writes still queued when the broker stops.
	retainedDrain = 2 * time.Second
	// retainedAdminWait bounds the store reads of Retained and
	// DeleteRetained.
	retainedAdminWait = 5 * time.Second
)

// retainedNode is the retained side of a cluster node, embedded in
// clusterNode.
type retainedNode struct {
	// retMu guards retCaches, retClosed and retWritersStopped. It is taken
	// before a cache's mu, never after.
	retMu     sync.Mutex
	retCaches map[string]*retainedCache
	retClosed bool

	// retCtx lives until the caches are closed; every watch runs under it.
	retCtx    context.Context //nolint:containedctx // the lifetime of the retained watches
	retCancel context.CancelFunc
	retLoads  sync.WaitGroup

	// The writes to the cell's store: one fifo and one writer goroutine per
	// shard. Every write of this node goes through its topic's shard, so one
	// topic's writes reach the store in the order they were made here.
	retQueues         [retainedShards]fifo[*retainWrite]
	retQueued         [retainedShards]atomic.Int64
	retQuit           chan struct{}
	retDrainBy        time.Time // set before retQuit closes
	retWritersStopped bool
	retWriters        sync.WaitGroup

	// retainedWait and retainedRetryWindow default to the constants; tests
	// may shorten them before Start. retainedLoadHook, when a test sets it
	// before Start, runs first in every tenant's load (to hold it), and
	// retainedAfterRetire between a cache leaving the map (already cleared)
	// and its watch being stopped.
	retainedWait, retainedRetryWindow time.Duration
	retainedLoadHook                  func(tenantID string)
	retainedAfterRetire               func(tenantID string)
	// retainedApplyDelay, when a test sets it (nanoseconds), slows every
	// watch report by that much before it is applied: a watch that lags.
	retainedApplyDelay atomic.Int64
	// retainedBeforeWrite, when a test sets it, runs as a writer takes a
	// write from its queue (to hold the shard).
	retainedBeforeWrite atomic.Pointer[func(w *retainWrite)]

	// Counters for tests: loads finished, watch reports applied, writes
	// stored, repairs, writes given up on, SUBSCRIBEs that went on without a
	// finished load or catch-up, catch-ups run.
	retainedLoads, retainedApplied, retainedWrites, retainedRepairs, retainedDropped, retainedWaitTimeouts atomic.Uint64
	retainedSyncs                                                                                          atomic.Uint64
}

func newRetainedNode() retainedNode {
	ctx, cancel := context.WithCancel(context.Background())

	return retainedNode{
		retCaches:           make(map[string]*retainedCache),
		retCtx:              ctx,
		retCancel:           cancel,
		retQuit:             make(chan struct{}),
		retainedWait:        retainedLoadWait,
		retainedRetryWindow: retainedRetryWindow,
	}
}

// startRetainedWriters starts the queue writers. Called once, from Start.
func (n *clusterNode) startRetainedWriters() {
	for i := range n.retQueues {
		n.retQueues[i].wake = make(chan struct{}, 1)
		n.retWriters.Add(1)

		go n.runRetainedWriter(i)
	}
}

// retainedCache is one tenant's retained messages in this node's mochi.
type retainedCache struct {
	tenant string

	// ready is closed when the first load has finished, or when the cache
	// stops before it did.
	ready     chan struct{}
	readyOnce sync.Once

	cancel context.CancelFunc // ends the watch and a load in progress

	// mu serializes every change of the tenant's entries in mochi's store:
	// the watch's reports, local applies, repairs and the clear at stop.
	mu      sync.Mutex
	view    map[string]packets.Packet // namespaced topic: what the watch last applied
	stopped bool
	watch   *cluster.RetainedWatch // nil until the load finished

	// syncMu guards the catch-ups of SUBSCRIBEs (syncCache): pending is the
	// batch that has not read its leader mark yet, running whether a
	// goroutine is working through batches.
	syncMu      sync.Mutex
	syncPending *syncBatch
	syncRunning bool

	lastActive time.Time // retMu
}

// syncBatch is one catch-up shared by the SUBSCRIBEs that joined it before
// it started.
type syncBatch struct {
	done chan struct{}
	err  error
}

func (c *retainedCache) markReady() { c.readyOnce.Do(func() { close(c.ready) }) }

// ensureRetained starts loading tenantID's retained messages into this node's
// cache unless that has started already. It does not wait: a SUBSCRIBE does
// (awaitRetained). Called with every CONNECT of the tenant, so the idle sweep
// counts from the last one.
func (n *clusterNode) ensureRetained(tenantID string) {
	n.retMu.Lock()
	defer n.retMu.Unlock()

	if n.retClosed {
		return
	}

	if c := n.retCaches[tenantID]; c != nil {
		c.lastActive = time.Now()

		return
	}

	ctx, cancel := context.WithCancel(n.retCtx)
	c := &retainedCache{
		tenant: tenantID, ready: make(chan struct{}), cancel: cancel,
		view: make(map[string]packets.Packet), lastActive: time.Now(),
	}
	n.retCaches[tenantID] = c

	n.retLoads.Add(1)

	go n.loadRetained(ctx, c)
}

// cacheOf returns tenantID's cache, nil when the node does not keep one.
func (n *clusterNode) cacheOf(tenantID string) *retainedCache {
	n.retMu.Lock()
	defer n.retMu.Unlock()

	return n.retCaches[tenantID]
}

// loadRetained loads c's tenant through a retained watch, retrying with
// backoff until the watch starts or the cache stops. The watch's first pass
// fills the cache before Watch returns; then c is ready, and the watch keeps
// applying changes until the cache stops.
func (n *clusterNode) loadRetained(ctx context.Context, c *retainedCache) {
	defer n.retLoads.Done()

	if hook := n.retainedLoadHook; hook != nil {
		hook(c.tenant)
	}

	delay := retainedRetry

	for {
		h, err := n.Retained.Watch(ctx, c.tenant, func(name string, msg *cluster.LogMessage, deleted bool) {
			n.applyWatched(c, name, msg, deleted)
		})
		if err == nil {
			c.mu.Lock()
			stopped := c.stopped

			if !stopped {
				c.watch = h
			}
			c.mu.Unlock()

			if stopped {
				h.Stop()

				return
			}

			n.retainedLoads.Add(1)
			c.markReady()

			return
		}

		n.log.Warn("tether: loading retained messages failed, retrying", slog.String("tenant", c.tenant),
			slog.Duration("in", delay), slog.Any("error", err))

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		delay = min(2*delay, retainedRetryMax)
	}
}

// applyWatched applies one report of c's watch to mochi's store. The report
// came off NATS, so it is checked like a routed message: a name that is not
// a valid topic of at most 1024 bytes, or is under $SYS in any casing, is
// dropped and logged.
func (n *clusterNode) applyWatched(c *retainedCache, name string, msg *cluster.LogMessage, deleted bool) {
	if topic.Valid(name, false, subject.MaxTopicBytes) != nil || hasSysPrefix(name) || !deleted && msg.QoS > 2 {
		n.log.Warn("tether: retained message dropped, not a tenant topic", slog.String("tenant", c.tenant), slog.String("topic", name))

		return
	}

	if d := n.retainedApplyDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}

	ns := topic.Namespace(c.tenant, name)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}

	if deleted {
		delete(c.view, ns)
		setRetained(n.mochi, clearedPacket(ns))
	} else {
		pk := retainedPacket(c.tenant, msg, n.maxMessageExpiry())
		c.view[ns] = pk
		setRetained(n.mochi, pk)
	}

	n.retainedApplied.Add(1)
}

// awaitRetained holds a SUBSCRIBE of cl, when one of its filters can get
// retained messages (not shared, Retain Handling not 2), until this node's
// cache of its tenant is current: first loaded, then caught up with the
// cell's store as of the SUBSCRIBE (syncCache), so a write any node
// acknowledged before the SUBSCRIBE arrived is in it (Review Focus 5). Both
// share one retainedWait bound; past it the SUBSCRIBE goes on with the cache
// as it is. It runs in OnSubscribe, on the client's read loop, and holds no
// lock.
func (n *clusterNode) awaitRetained(cl *mqtt.Client, filters packets.Subscriptions) {
	want := false

	for _, sub := range filters {
		if sub.Filter != "" && !mqtt.IsSharedFilter(sub.Filter) && sub.RetainHandling != 2 {
			want = true

			break
		}
	}

	if !want {
		return
	}

	tenantID, ok := tenantOfID(cl.ID)
	if !ok {
		return
	}

	c := n.cacheOf(tenantID)
	if c == nil {
		// Every CONNECT starts it; this is a fallback.
		n.ensureRetained(tenantID)

		if c = n.cacheOf(tenantID); c == nil {
			return
		}
	}

	timer := time.NewTimer(n.retainedWait)
	defer timer.Stop()

	select {
	case <-c.ready:
	case <-timer.C:
		n.retainedWaitTimeouts.Add(1)
		n.log.Info("tether: retained messages not loaded in time, subscribing without them", slog.String("client", cl.ID))

		return
	}

	if err := n.syncCache(c, timer.C); err != nil {
		n.retainedWaitTimeouts.Add(1)
		n.log.Info("tether: retained messages not caught up with the cell, subscribing with what this node has",
			slog.String("client", cl.ID), slog.Any("error", err))
	}
}

// errSyncBound is what syncCache returns when the SUBSCRIBE's bound ran out.
var errSyncBound = errors.New("tether/broker: retained catch-up: bound reached")

// syncCache catches c's watch up with the cell's store (RetainedWatch.Sync)
// for a SUBSCRIBE, waiting at most until bound fires. SUBSCRIBEs that arrive
// while a catch-up runs join the next one, which starts, and reads the
// store's mark, only after the running one is done, so every joiner arrived
// before the mark it waits for was read. One goroutine per cache works
// through the batches, each bounded by retainedWait.
func (n *clusterNode) syncCache(c *retainedCache, bound <-chan time.Time) error {
	c.syncMu.Lock()

	b := c.syncPending
	if b == nil {
		b = &syncBatch{done: make(chan struct{})}
		c.syncPending = b
	}

	if !c.syncRunning {
		c.syncRunning = true

		go n.runSyncs(c)
	}

	c.syncMu.Unlock()

	select {
	case <-b.done:
		return b.err
	case <-bound:
		return errSyncBound
	}
}

// runSyncs runs c's pending catch-ups one batch at a time until none is
// pending.
func (n *clusterNode) runSyncs(c *retainedCache) {
	for {
		c.syncMu.Lock()

		b := c.syncPending
		c.syncPending = nil

		if b == nil {
			c.syncRunning = false
			c.syncMu.Unlock()

			return
		}

		c.syncMu.Unlock()

		c.mu.Lock()
		h, stopped := c.watch, c.stopped
		c.mu.Unlock()

		switch {
		case stopped || h == nil:
			b.err = fmt.Errorf("tether/broker: retained catch-up: %w: no watch", tether.ErrNotFound)
		default:
			ctx, cancel := context.WithTimeout(context.Background(), n.retainedWait)
			b.err = h.Sync(ctx)

			cancel()
			n.retainedSyncs.Add(1)
		}

		close(b.done)
	}
}

// retire removes the caches pick chooses (pick runs under retMu) and stops
// them. Each one is marked stopped and its tenant's entries leave mochi's
// store under retMu, before it leaves the map: a CONNECT that makes the
// tenant a new cache can only do so after that, so the clear never touches
// what the new cache applies. The watches stop afterwards, outside the
// locks (stopping waits for a report being applied, which takes the
// cache's mu).
func (n *clusterNode) retire(pick func(tenantID string, c *retainedCache) bool) {
	type gone struct {
		c *retainedCache
		h *cluster.RetainedWatch
	}

	var out []gone

	n.retMu.Lock()

	for tenantID, c := range n.retCaches {
		if !pick(tenantID, c) {
			continue
		}

		c.mu.Lock()
		c.stopped = true
		h := c.watch
		c.view = nil

		if n.mochi != nil {
			clearTenantRetained(n.mochi, c.tenant)
		}

		c.mu.Unlock()

		delete(n.retCaches, tenantID)
		out = append(out, gone{c: c, h: h})
	}

	n.retMu.Unlock()

	for _, g := range out {
		if hook := n.retainedAfterRetire; hook != nil {
			hook(g.c.tenant)
		}

		g.c.cancel()

		if g.h != nil {
			g.h.Stop()
		}

		g.c.markReady()
	}
}

// sweepRetained stops the caches of tenants with no live connection on this
// node for readerIdle, with the readers (sweepReaders).
func (n *clusterNode) sweepRetained(live map[string]int, now time.Time) {
	n.retire(func(tenantID string, c *retainedCache) bool {
		if live[tenantID] > 0 {
			c.lastActive = now

			return false
		}

		return now.Sub(c.lastActive) >= n.readerIdle
	})
}

// closeRetainedCaches stops every cache, for Stop. None starts after it.
func (n *clusterNode) closeRetainedCaches() {
	n.retMu.Lock()
	closed := n.retClosed
	n.retClosed = true
	n.retMu.Unlock()

	if closed {
		return
	}

	n.retire(func(string, *retainedCache) bool { return true })

	n.retCancel()
	n.retLoads.Wait()
}

// retainWrite is one retained publish on its way to the cell's store. msg
// carries the tenant-relative topic, the payload (empty: delete), QoS, props,
// origin node, publisher and publish time.
type retainWrite struct {
	tenant string
	msg    cluster.LogMessage

	// done, when a caller waits (storeRetained), gets the outcome once;
	// cancelled makes the writer skip a write that has not started.
	done      chan error
	cancelled atomic.Bool
}

// report hands the outcome to a waiting caller, if any.
func (w *retainWrite) report(err error) {
	if w.done != nil {
		w.done <- err
	}
}

// newRetainWrite is the write of a retained publish of tenantID on the
// tenant-relative name. payload is not copied.
func (n *clusterNode) newRetainWrite(tenantID, name string, payload []byte, qos byte, props cluster.Props, clientID string) *retainWrite {
	return &retainWrite{
		tenant: tenantID,
		msg: cluster.LogMessage{
			Tenant: tenantID, Topic: name, Payload: payload, QoS: qos, Retain: true, Props: props,
			Origin: n.Config.NodeID, ClientID: clientID, PublishedAt: time.Now(),
		},
	}
}

// retainPublish handles the retain flag of a network client's accepted QoS 0
// publish (OnPublish): the message goes into this node's cache and, through
// a queue, to the cell's store, and pk goes on to mochi untouched by its
// store (retain flag clear, tagged for Retain As Published).
func (n *clusterNode) retainPublish(st *clientState, rel string, pk *packets.Packet) {
	w := n.newRetainWrite(st.tenantID, rel, slices.Clone(pk.Payload), pk.FixedHeader.Qos, clusterProps(pk.Properties), st.rawID)
	n.applyLocal(w)
	if err := n.enqueueRetained(w); err != nil {
		// Logged, counted and repaired there; QoS 0 owes nobody an answer.
		n.log.Debug("tether: qos 0 retained write refused", slog.Any("error", err))
	}

	pk.FixedHeader.Retain = false
	tagRetained(pk)
}

// applyLocal puts w into this node's cache at once, when the node keeps one
// for the tenant. It must come before w's write is issued.
func (n *clusterNode) applyLocal(w *retainWrite) {
	c := n.cacheOf(w.tenant)
	if c == nil {
		return
	}

	ns := topic.Namespace(w.tenant, w.msg.Topic)
	pk := clearedPacket(ns)

	if _, expired := n.retainedTTL(&w.msg); len(w.msg.Payload) > 0 && !expired {
		pk = retainedPacket(w.tenant, &w.msg, n.maxMessageExpiry())
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.stopped {
		setRetained(n.mochi, pk)
	}
}

// repairRetained puts back, for a write that failed for good, what the
// watch last reported for its topic, so the local apply does not outlive it.
func (n *clusterNode) repairRetained(w *retainWrite) {
	c := n.cacheOf(w.tenant)
	if c == nil {
		return
	}

	ns := topic.Namespace(w.tenant, w.msg.Topic)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}

	n.retainedRepairs.Add(1)

	if pk, ok := c.view[ns]; ok && !packetExpired(pk, time.Now().Unix(), n.maxMessageExpiry()) {
		setRetained(n.mochi, pk)

		return
	}

	setRetained(n.mochi, clearedPacket(ns))
}

// putRetained writes w to the cell's store: a Put that expires with the
// message (retainedTTL), or a Delete for an empty payload or a message that
// has expired already.
func (n *clusterNode) putRetained(ctx context.Context, w *retainWrite) error {
	// Ruling R13: the cell keys retained messages by topic, which it caps.
	if err := subject.CheckTopic(w.msg.Topic); err != nil {
		return fmt.Errorf("tether/broker: retained %q: %w", w.msg.Topic, err)
	}

	ttl, expired := n.retainedTTL(&w.msg)
	if len(w.msg.Payload) == 0 || expired {
		return n.Retained.Delete(ctx, w.tenant, w.msg.Topic)
	}

	return n.Retained.Put(ctx, w.tenant, w.msg.Topic, w.msg, ttl)
}

// storeRetained applies w to this node's cache and has its topic's shard
// write it to the cell's store (enqueueRetained), behind every earlier write
// of the topic from this node, then waits for the outcome under ctx. When
// ctx ends first it stops waiting: with handoff (the QoS 1 and 2 pipe,
// wills: the publish is in the cell already) the write goes on; without (the
// host's Publish and DeleteRetained, which return the error) a write that
// has not started is skipped and repaired, and one that has started finishes
// regardless. The error is the write's, or ctx's.
func (n *clusterNode) storeRetained(ctx context.Context, w *retainWrite, handoff bool) error {
	n.applyLocal(w)

	w.done = make(chan error, 1)

	if err := n.enqueueRetained(w); err != nil {
		return err
	}

	select {
	case err := <-w.done:
		return err
	case <-ctx.Done():
		if !handoff {
			w.cancelled.Store(true)
		}

		return fmt.Errorf("tether/broker: store retained %q: %w", w.msg.Topic, ctx.Err())
	}
}

// enqueueRetained queues w for its topic's shard (by tenant and topic), whose
// writer stores the writes of one topic in the order they were queued. A full
// queue, or one whose writer has stopped, drops w: it is counted, logged and
// repaired, and the error is returned (and reported to a waiter).
func (n *clusterNode) enqueueRetained(w *retainWrite) error {
	h := fnv.New32a()
	_, _ = h.Write([]byte(w.tenant))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(w.msg.Topic))
	i := h.Sum32() % retainedShards

	n.retMu.Lock()

	if n.retWritersStopped || n.retQueued[i].Load() >= retainedQueueMax {
		n.retMu.Unlock()
		n.retainedDropped.Add(1)
		n.log.Warn("tether: retained message not stored in the cell, its queue is full or closed",
			slog.String("tenant", w.tenant), slog.String("topic", w.msg.Topic))
		n.repairRetained(w)

		return fmt.Errorf("tether/broker: store retained %q: %w: queue full or closed", w.msg.Topic, cluster.ErrUnavailable)
	}

	n.retQueued[i].Add(1)
	n.retQueues[i].push(w)
	n.retMu.Unlock()

	return nil
}

// stopRetainedWriters stops taking writes and lets the writers finish what
// is queued, each write bounded by retainedDrain from now. For Stop, after
// the wills are done.
func (n *clusterNode) stopRetainedWriters() {
	n.retMu.Lock()

	if n.retWritersStopped {
		n.retMu.Unlock()

		return
	}

	n.retWritersStopped = true
	n.retDrainBy = time.Now().Add(retainedDrain)
	close(n.retQuit)
	n.retMu.Unlock()

	n.retWriters.Wait()
}

func (n *clusterNode) runRetainedWriter(i int) {
	defer n.retWriters.Done()

	for {
		// pop returns what is queued before it reports the quit.
		w, ok := n.retQueues[i].pop(n.retQuit)
		if !ok {
			return
		}

		w.report(n.writeQueued(w))
		n.retQueued[i].Add(-1)
	}
}

// writeQueued stores one write, retrying with backoff for retainedRetryWindow
// from when it starts here; once the writers are stopping, one attempt within
// the drain bound. A write given up on, or cancelled before it started, is
// logged, counted and repaired. It returns the outcome.
func (n *clusterNode) writeQueued(w *retainWrite) error {
	if hook := n.retainedBeforeWrite.Load(); hook != nil {
		(*hook)(w)
	}

	if w.cancelled.Load() {
		n.retainedDropped.Add(1)
		n.repairRetained(w)

		return fmt.Errorf("tether/broker: store retained %q: %w", w.msg.Topic, context.Canceled)
	}

	start := time.Now()
	delay := retainedRetry

	for {
		draining := false

		select {
		case <-n.retQuit:
			draining = true
		default:
		}

		var (
			ctx    context.Context
			cancel context.CancelFunc
		)

		if draining {
			ctx, cancel = context.WithDeadline(context.Background(), n.retDrainBy)
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), retainedWriteTimeout)
		}

		err := n.putRetained(ctx, w)

		cancel()

		if err == nil {
			n.retainedWrites.Add(1)

			return nil
		}

		if draining || errors.Is(err, tether.ErrInvalidInput) || time.Since(start) > n.retainedRetryWindow {
			n.retainedDropped.Add(1)
			n.log.Warn("tether: retained message not stored in the cell, giving up", slog.String("tenant", w.tenant),
				slog.String("topic", w.msg.Topic), slog.Any("error", err))
			n.repairRetained(w)

			return fmt.Errorf("tether/broker: store retained %q: %w", w.msg.Topic, err)
		}

		n.log.Warn("tether: storing a retained message failed, retrying", slog.String("tenant", w.tenant),
			slog.String("topic", w.msg.Topic), slog.Duration("in", delay), slog.Any("error", err))

		select {
		case <-n.retQuit:
		case <-time.After(delay):
		}

		delay = min(2*delay, retainedRetryMax)
	}
}

// maxMessageExpiry is mochi's MaximumMessageExpiryInterval in seconds, which
// it applies to every message, retained ones included (a retained message
// older than that is dropped from its store). The cell's store applies the
// same cap, so no node's cache keeps what the store dropped or the reverse.
func (n *clusterNode) maxMessageExpiry() int64 {
	if n.mochi == nil {
		return 0
	}

	return n.mochi.Options.Capabilities.MaximumMessageExpiryInterval
}

// retainedTTL is how long the cell's store keeps msg from now: what is left
// of its message expiry interval, capped at mochi's maximum, both counted
// from its publish time. 0 means no expiry; expired is true when nothing is
// left.
func (n *clusterNode) retainedTTL(msg *cluster.LogMessage) (ttl time.Duration, expired bool) {
	life := lifetime(msg.Props.MessageExpiry, n.maxMessageExpiry())
	if life == 0 {
		return 0, false
	}

	left := time.Until(msg.PublishedAt.Add(time.Duration(life) * time.Second))
	if left <= 0 {
		return 0, true
	}

	return left, false
}

// lifetime is a message's expiry interval held to the maximum, in seconds,
// 0 for none: mochi's rule (minimum in its server.go).
func lifetime(expiry uint32, maxExpiry int64) int64 {
	life := int64(expiry)

	if maxExpiry > 0 && (life == 0 || life > maxExpiry) {
		life = maxExpiry
	}

	return life
}

// retainedPacket is the packet mochi's store keeps for msg, a retained
// message of tenantID: the namespaced topic, the payload, QoS and props, the
// publisher's namespaced client ID as origin (No Local), created at its
// publish time and expiring with its message expiry interval held to mochi's
// maximum (the maximum when it has none), as mochi stores a retained publish
// itself. ProtocolVersion 5 lets mochi's expiry sweep see the expiry.
func retainedPacket(tenantID string, msg *cluster.LogMessage, maxExpiry int64) packets.Packet {
	created := msg.PublishedAt
	if created.IsZero() {
		created = time.Now()
	}

	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: msg.QoS, Retain: true},
		ProtocolVersion: 5,
		TopicName:       topic.Namespace(tenantID, msg.Topic),
		Payload:         msg.Payload,
		Properties:      packetProps(msg.Props),
		Created:         created.Unix(),
	}

	if msg.ClientID != "" {
		pk.Origin = namespacedID(tenantID, msg.ClientID)
	}

	if life := lifetime(msg.Props.MessageExpiry, maxExpiry); life > 0 {
		pk.Expiry = pk.Created + life
	}

	return pk
}

// clearedPacket is the empty retained publish that removes ns from mochi's
// store.
func clearedPacket(ns string) packets.Packet {
	return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true}, TopicName: ns}
}

// packetExpired reports whether mochi's expiry sweep would drop pk at now.
func packetExpired(pk packets.Packet, now, maxExpiry int64) bool {
	return pk.Expiry > 0 && pk.Expiry < now || maxExpiry > 0 && now-pk.Created > maxExpiry
}

// setRetained stores pk in mochi's retained store, or removes its topic for
// an empty payload, without mochi's OnRetainMessage.
func setRetained(srv *mqtt.Server, pk packets.Packet) {
	if srv == nil || srv.Options.Capabilities.RetainAvailable == 0 {
		return
	}

	srv.Topics.RetainMessage(pk)
	atomic.StoreInt64(&srv.Info.Retained, int64(srv.Topics.Retained.Len()))
}

// clearTenantRetained removes every retained message of tenantID from mochi's
// store.
func clearTenantRetained(srv *mqtt.Server, tenantID string) {
	for name := range srv.Topics.Retained.GetAll() {
		if t, ok := tenantOfName(name); ok && t == tenantID {
			srv.Topics.RetainMessage(clearedPacket(name))
		}
	}

	atomic.StoreInt64(&srv.Info.Retained, int64(srv.Topics.Retained.Len()))
}

// tagRetained marks a packet the hook took the retain flag off. mochi copies
// the field into every subscriber's packet and its inflight entry and never
// encodes it in a PUBLISH (like tagSeq).
func tagRetained(pk *packets.Packet) { pk.Connect.WillRetain = true }

// retainedTag reports whether tagRetained marked pk.
func retainedTag(pk packets.Packet) bool { return pk.Connect.WillRetain }

// retainAsPublished reports whether cl holds a subscription with Retain As
// Published that matches the namespaced name: a tagged delivery to it then
// carries the retain flag ([MQTT-3.3.1-13]). Of overlapping subscriptions,
// one with the option is enough.
func retainAsPublished(cl *mqtt.Client, name string) bool {
	for f, sub := range cl.State.Subscriptions.GetAll() {
		if sub.RetainAsPublished && topic.Match(withoutShare(f), name) {
			return true
		}
	}

	return false
}

// clusterRetained is Retained in cluster mode: the tenant's messages in the
// cell's store. When the store cannot be read it answers from this node's
// cache and logs.
func (b *Broker) clusterRetained(tenantID, filter string) ([]RetainedInfo, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), retainedAdminWait)
	defer cancel()

	msgs, err := b.cluster.Retained.Load(ctx, tenantID)
	if err != nil {
		b.cluster.log.Warn("tether: listing the cell's retained messages failed, listing this node's", slog.String("tenant", tenantID), slog.Any("error", err))

		return nil, false
	}

	var out []RetainedInfo

	for _, m := range msgs {
		if topic.Valid(m.Topic, false, subject.MaxTopicBytes) != nil || hasSysPrefix(m.Topic) || !topic.Match(filter, m.Topic) {
			continue
		}

		out = append(out, RetainedInfo{Topic: m.Topic, Size: len(m.Payload), QoS: m.QoS})
	}

	slices.SortFunc(out, func(x, y RetainedInfo) int { return strings.Compare(x.Topic, y.Topic) })

	return out, true
}

// clusterDeleteRetained is DeleteRetained in cluster mode: the topic must
// hold a retained message in the cell (this node's cache answers when it has
// it, the store otherwise); the delete is written to the store, then the
// empty retained publish goes to the topic's subscribers.
func (b *Broker) clusterDeleteRetained(srv *mqtt.Server, tenantID, topicName, ns string) error {
	n := b.cluster

	if err := subject.CheckTopic(topicName); err != nil {
		return fmt.Errorf("tether/broker: topic %q: %w", topicName, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), retainedAdminWait)
	defer cancel()

	if !n.cachedRetained(tenantID, ns) {
		msgs, err := n.Retained.Load(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("tether/broker: delete retained %q: %w", topicName, err)
		}

		if !slices.ContainsFunc(msgs, func(m cluster.LogMessage) bool { return m.Topic == topicName }) {
			return noRetained(topicName)
		}
	}

	if err := n.storeRetained(ctx, n.newRetainWrite(tenantID, topicName, nil, 0, cluster.Props{}, ""), false); err != nil {
		return fmt.Errorf("tether/broker: delete retained %q: %w", topicName, err)
	}

	// The clearing message, as over MQTT: the hook takes the retain flag off
	// (the store has the delete already) and routes it.
	if err := srv.Publish(ns, nil, true, 0); err != nil {
		return fmt.Errorf("tether/broker: delete retained %q: %w", topicName, err)
	}

	return nil
}

// cachedRetained reports whether this node's loaded cache of tenantID holds a
// retained message on the namespaced name.
func (n *clusterNode) cachedRetained(tenantID, ns string) bool {
	c := n.cacheOf(tenantID)
	if c == nil {
		return false
	}

	select {
	case <-c.ready:
	default:
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.view[ns]

	return ok
}

// retainedView lists the namespaced topics tenantID's cache holds from its
// watch, for tests; nil when there is no cache.
func (n *clusterNode) retainedView(tenantID string) []string {
	c := n.cacheOf(tenantID)
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]string, 0, len(c.view))
	for ns := range c.view {
		out = append(out, ns)
	}

	slices.Sort(out)

	return out
}
