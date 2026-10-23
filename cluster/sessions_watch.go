package cluster

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
)

// WatchOwned watches the whole sessions bucket and calls fn when a session that
// nodeID owned is no longer its: another node took it over (the owner or the
// epoch changed), or the key was deleted, purged or expired (JetStream reports
// an expiry as a purge with the reason MaxAge, and fn sees deleted == true).
// For a takeover rec is the new record, for a removal rec is nil.
//
// The watcher learns which sessions nodeID owns from this Sessions' own
// successful Acquire, Update and Expire calls and from what the watch itself
// reads, records whose Owner is nodeID. After fn reports a session lost, the
// watcher forgets it until nodeID owns it again. A write by this Sessions never
// calls fn, and neither does a delete it made.
//
// WatchOwned returns once the watcher has read the sessions that exist now, so
// a session nodeID already owns is known by then. fn runs on one goroutine, one
// call at a time, and must not block for long. The watch ends when ctx ends or
// stop is called. stop is safe to call more than once and from fn; it does not
// wait for a running fn, though fn is not called for events that arrive after
// stop returns.
func (s *Sessions) WatchOwned(ctx context.Context, nodeID string, fn func(tenant, clientID string, rec *SessionRecord, deleted bool)) (stop func(), err error) {
	if nodeID == "" || fn == nil {
		return nil, fmt.Errorf("cluster: watch sessions: needs a node id and a callback: %w", tether.ErrInvalidInput)
	}

	h, err := s.handles(ctx)
	if err != nil {
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)

	w := &ownedWatch{
		nodeID:  nodeID,
		fn:      fn,
		s:       s,
		owned:   make(map[string]ownedState),
		flights: make(map[string]*watchEvent),
		floors:  make(map[string]uint64),
		kick:    make(chan struct{}, 1),
		ready:   make(chan struct{}),
	}

	// Register before the first read, so a write that lands during the initial
	// pass is seen by the watcher too.
	s.wmu.Lock()
	s.watchers = append(s.watchers, w)
	s.wmu.Unlock()

	stop = func() {
		cancel()
		s.unregister(w)
	}

	kw, err := h.kv.WatchAll(wctx)
	if err != nil {
		stop()

		return nil, s.wrapErr("watch sessions", err)
	}

	go w.run(wctx, kw)

	select {
	case <-w.ready:
		return stop, nil
	case <-ctx.Done():
		stop()

		return nil, s.wrapErr("watch sessions", ctx.Err())
	}
}

func (s *Sessions) unregister(w *ownedWatch) {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	s.watchers = slices.DeleteFunc(s.watchers, func(x *ownedWatch) bool { return x == w })
}

// ownedState is what the watcher knows about a session its node owns: the
// revision and epoch of the newest write it has accounted for.
type ownedState struct {
	rev   uint64
	epoch uint64
}

// watchEvent is one change to a key, as the watch delivered it.
type watchEvent struct {
	key     string
	rev     uint64
	deleted bool
	rec     *SessionRecord
}

type fireItem struct {
	key     string
	rec     *SessionRecord
	deleted bool
}

// ownedWatch tracks the sessions one node owns.
//
// Two things make the bookkeeping subtle. The watch delivers changes in
// revision order, but a write of this node can be acknowledged before or after
// the watch delivers it. And the watch can lag behind a write the node already
// knows about. So every decision compares revisions: an event at or below the
// revision the node already accounted for is ignored. For a write in flight
// (between begin and end), the watch's event is parked until the write's
// revision is known, then dropped if it was the write itself.
type ownedWatch struct {
	s      *Sessions
	nodeID string
	fn     func(tenant, clientID string, rec *SessionRecord, deleted bool)

	mu      sync.Mutex
	owned   map[string]ownedState
	flights map[string]*watchEvent // keys with a write in flight, and the newest event parked for each
	floors  map[string]uint64      // keys this node deleted, and the marker's revision
	fires   []fireItem

	kick      chan struct{}
	ready     chan struct{}
	readyOnce sync.Once
}

// begin marks a write on key as in flight.
func (w *ownedWatch) begin(key string) {
	w.mu.Lock()
	w.flights[key] = nil
	w.mu.Unlock()
}

// end finishes the write begun on key. ok says it succeeded; rev is then its
// revision. A delete passes deleted == true and rec == nil.
func (w *ownedWatch) end(key string, rev uint64, rec *SessionRecord, deleted, ok bool) {
	w.mu.Lock()

	parked := w.flights[key]
	delete(w.flights, key)

	if ok {
		switch {
		case deleted:
			delete(w.owned, key)

			// The floor makes the watch skip the marker when it arrives. If the
			// watch delivered it already (it is parked, at this revision or
			// later), nothing is left to skip, and a floor would stay forever.
			if parked == nil || parked.rev < rev {
				w.floors[key] = rev
			}
		case rec != nil && rec.Owner == w.nodeID:
			w.owned[key] = ownedState{rev: rev, epoch: rec.Epoch}
		default:
			delete(w.owned, key)
		}

		if parked != nil && parked.rev <= rev {
			parked = nil // it was this write, or older
		}
	}

	if parked != nil {
		w.evaluate(*parked)
	}

	pending := len(w.fires) > 0

	w.mu.Unlock()

	if pending {
		w.wake()
	}
}

// handle accounts for one event from the watch.
func (w *ownedWatch) handle(ev watchEvent) {
	w.mu.Lock()

	if parked, inFlight := w.flights[ev.key]; inFlight {
		if parked == nil || ev.rev > parked.rev {
			w.flights[ev.key] = &ev
		}

		w.mu.Unlock()

		return
	}

	w.evaluate(ev)

	pending := len(w.fires) > 0

	w.mu.Unlock()

	if pending {
		w.wake()
	}
}

// evaluate decides what an event means for the node. Callers hold w.mu.
func (w *ownedWatch) evaluate(ev watchEvent) {
	if floor, ok := w.floors[ev.key]; ok {
		if ev.rev <= floor {
			if ev.rev == floor {
				delete(w.floors, ev.key)
			}

			return
		}

		delete(w.floors, ev.key)
	}

	st, tracked := w.owned[ev.key]
	if tracked && ev.rev <= st.rev {
		return
	}

	switch {
	case ev.deleted:
		if tracked {
			delete(w.owned, ev.key)
			w.fires = append(w.fires, fireItem{key: ev.key, deleted: true})
		}
	case ev.rec.Owner == w.nodeID && (!tracked || ev.rec.Epoch == st.epoch):
		// This node's own record: a write of its own that the watch delivered
		// before the write returned, or a session it owned before it started
		// watching.
		w.owned[ev.key] = ownedState{rev: ev.rev, epoch: ev.rec.Epoch}
	case tracked:
		// Another owner, or this node's id at another epoch: the session moved.
		delete(w.owned, ev.key)
		w.fires = append(w.fires, fireItem{key: ev.key, rec: ev.rec})
	}
}

func (w *ownedWatch) wake() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// run reads the watch until ctx ends and calls fn for what evaluate decided.
func (w *ownedWatch) run(ctx context.Context, kw jetstream.KeyWatcher) {
	defer func() {
		w.s.stopWatcher(kw)
		w.s.unregister(w)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.kick:
		case e, ok := <-kw.Updates():
			if !ok {
				return
			}

			if e == nil { // the sessions that existed at the start have been read
				w.readyOnce.Do(func() { close(w.ready) })
			} else {
				w.consume(e)
			}
		}

		w.deliver(ctx)
	}
}

func (w *ownedWatch) consume(e jetstream.KeyValueEntry) {
	ev := watchEvent{key: e.Key(), rev: e.Revision()}

	if e.Operation() != jetstream.KeyValuePut {
		ev.deleted = true
	} else {
		rec, err := decodeSession(e.Value())
		if err != nil {
			w.s.c.logger.Warn("watch: skipping undecodable session record", "key", ev.key, "error", err)

			return
		}

		ev.rec = rec
	}

	w.handle(ev)
}

// deliver calls fn for every decision waiting, on this goroutine.
func (w *ownedWatch) deliver(ctx context.Context) {
	w.mu.Lock()
	fires := w.fires
	w.fires = nil
	w.mu.Unlock()

	for _, f := range fires {
		if ctx.Err() != nil {
			return
		}

		tenant, clientID, err := splitSessionKey(f.key)
		if err != nil {
			w.s.c.logger.Warn("watch: skipping undecodable session key", "key", f.key, "error", err)

			continue
		}

		w.fn(tenant, clientID, f.rec, f.deleted)
	}
}
