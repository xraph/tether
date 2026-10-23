package cluster

import (
	"testing"
	"time"
)

// testKey is the key every bare-watch test works on.
const testKey = "t.k"

// newBareWatch returns an ownedWatch for node n1 with no NATS behind it. The
// tests feed it begin, handle and end calls in the orders the real code can
// produce and read what it decided from w.fires.
func newBareWatch() *ownedWatch {
	return &ownedWatch{
		nodeID:  "n1",
		owned:   make(map[string]ownedState),
		flights: make(map[string]*watchEvent),
		floors:  make(map[string]uint64),
		kick:    make(chan struct{}, 1),
	}
}

func put(rev uint64, owner string, epoch uint64) watchEvent {
	return watchEvent{key: testKey, rev: rev, rec: &SessionRecord{Owner: owner, Epoch: epoch}}
}

// purge is the delete marker at revision 9, which every test uses.
func purge() watchEvent {
	return watchEvent{key: testKey, rev: 9, deleted: true}
}

func (w *ownedWatch) drain() []fireItem {
	w.mu.Lock()
	defer w.mu.Unlock()

	f := w.fires
	w.fires = nil

	return f
}

func (w *ownedWatch) checkClean(t *testing.T) {
	t.Helper()

	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.floors) != 0 || len(w.flights) != 0 {
		t.Fatalf("leftover state: floors %v, flights %v", w.floors, w.flights)
	}
}

func TestOwnedWatchOwnDelete(t *testing.T) {
	for name, order := range map[string]string{
		"event before end": "event,end",
		"end before event": "end,event",
	} {
		t.Run(name, func(t *testing.T) {
			w := newBareWatch()
			w.owned[testKey] = ownedState{rev: 5, epoch: 1}

			w.begin(testKey)

			if order == "event,end" {
				w.handle(purge()) // parked: the write is in flight
				w.end(testKey, 9, nil, true, true)
			} else {
				w.end(testKey, 9, nil, true, true)
				w.handle(purge())
			}

			if f := w.drain(); len(f) != 0 {
				t.Fatalf("own delete fired %+v", f)
			}

			if _, ok := w.owned[testKey]; ok {
				t.Fatal("deleted key still tracked")
			}

			w.checkClean(t)
		})
	}
}

func TestOwnedWatchOwnDeleteLaggingPut(t *testing.T) {
	w := newBareWatch()
	w.owned[testKey] = ownedState{rev: 5, epoch: 1}

	// The delete is acknowledged before the watch has delivered an older put of
	// the same node. The put must not bring the key back, and the marker after
	// it must not fire.
	w.begin(testKey)
	w.end(testKey, 9, nil, true, true)

	w.handle(put(7, "n1", 1))
	w.handle(purge())

	if f := w.drain(); len(f) != 0 {
		t.Fatalf("fired %+v", f)
	}

	if len(w.owned) != 0 {
		t.Fatalf("tracked %v", w.owned)
	}

	w.checkClean(t)
}

func TestOwnedWatchDeleteFailsTakeoverParked(t *testing.T) {
	w := newBareWatch()
	w.owned[testKey] = ownedState{rev: 5, epoch: 1}

	// Our delete is in flight while another node takes the session over. Our
	// delete loses; the parked takeover must then fire.
	w.begin(testKey)
	w.handle(put(8, "n2", 2))
	w.end(testKey, 0, nil, true, false)

	f := w.drain()
	if len(f) != 1 || f[0].deleted || f[0].rec == nil || f[0].rec.Owner != "n2" {
		t.Fatalf("fires = %+v, want one takeover by n2", f)
	}

	if len(w.owned) != 0 {
		t.Fatalf("still tracked: %v", w.owned)
	}

	w.checkClean(t)
}

func TestOwnedWatchWriteFailsTakeoverParked(t *testing.T) {
	w := newBareWatch()
	w.owned[testKey] = ownedState{rev: 5, epoch: 1}

	w.begin(testKey)
	w.handle(put(8, "n2", 2))
	w.end(testKey, 0, &SessionRecord{Owner: "n1", Epoch: 1}, false, false)

	if f := w.drain(); len(f) != 1 || f[0].rec == nil || f[0].rec.Owner != "n2" {
		t.Fatalf("fires = %+v, want one takeover by n2", f)
	}

	w.checkClean(t)
}

func TestOwnedWatchOwnWriteThenTakeover(t *testing.T) {
	for name, eventFirst := range map[string]bool{"event before end": true, "end before event": false} {
		t.Run(name, func(t *testing.T) {
			w := newBareWatch()
			w.owned[testKey] = ownedState{rev: 5, epoch: 1}

			own := &SessionRecord{Owner: "n1", Epoch: 1}

			w.begin(testKey)

			if eventFirst {
				w.handle(put(6, "n1", 1)) // our own write, parked
			}

			w.end(testKey, 6, own, false, true)

			if !eventFirst {
				w.handle(put(6, "n1", 1))
			}

			if f := w.drain(); len(f) != 0 {
				t.Fatalf("own write fired %+v", f)
			}

			if st := w.owned[testKey]; st.rev != 6 || st.epoch != 1 {
				t.Fatalf("tracked %+v, want rev 6 epoch 1", st)
			}

			// A takeover after our write fires once.
			w.handle(put(7, "n2", 2))

			f := w.drain()
			if len(f) != 1 || f[0].rec == nil || f[0].rec.Owner != "n2" || f[0].deleted {
				t.Fatalf("fires = %+v, want one takeover by n2", f)
			}

			if len(w.owned) != 0 {
				t.Fatalf("still tracked: %v", w.owned)
			}

			w.checkClean(t)
		})
	}
}

func TestOwnedWatchParkedNewerThanWrite(t *testing.T) {
	w := newBareWatch()
	w.owned[testKey] = ownedState{rev: 5, epoch: 1}

	// Our write lands at 6 and a takeover at 7 is parked behind it.
	w.begin(testKey)
	w.handle(put(6, "n1", 1))
	w.handle(put(7, "n2", 2))
	w.end(testKey, 6, &SessionRecord{Owner: "n1", Epoch: 1}, false, true)

	f := w.drain()
	if len(f) != 1 || f[0].rec == nil || f[0].rec.Owner != "n2" {
		t.Fatalf("fires = %+v, want the takeover", f)
	}

	w.checkClean(t)
}

func TestOwnedWatchPurgeOfOwnedKey(t *testing.T) {
	w := newBareWatch()
	w.owned[testKey] = ownedState{rev: 5, epoch: 1}

	// An expiry marker for a key the node owns fires once, then the key is
	// forgotten.
	w.handle(purge())

	if f := w.drain(); len(f) != 1 || !f[0].deleted || f[0].rec != nil {
		t.Fatalf("fires = %+v, want one deletion", f)
	}

	w.checkClean(t)
}

// killWatch ends the running KV watcher of a's only WatchOwned under it, the
// way a closed subscription would, after setting the hook that runs before
// the watch is started again.
func killWatch(t *testing.T, a *Sessions, beforeRestart func()) {
	t.Helper()

	a.wmu.Lock()

	if len(a.watchers) != 1 {
		a.wmu.Unlock()
		t.Fatalf("%d watchers, want 1", len(a.watchers))
	}

	w := a.watchers[0]
	a.wmu.Unlock()

	w.beforeRestart = beforeRestart

	w.mu.Lock()
	kw := w.kw
	w.mu.Unlock()

	if err := kw.Stop(); err != nil {
		t.Fatal(err)
	}
}

// Ruling R26: a watch whose subscription closes under it is started again,
// and its fresh first pass catches up. A takeover after the restart is
// reported, and so is an owned key deleted while no watch ran.
func TestWatchOwnedRestarts(t *testing.T) {
	cell := startCell(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	type lost struct {
		clientID string
		deleted  bool
	}

	events := make(chan lost, 16)

	stop, err := a.WatchOwned(ctx, "n1", func(_, clientID string, _ *SessionRecord, deleted bool) {
		events <- lost{clientID, deleted}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	expect := func(want lost) {
		t.Helper()

		select {
		case ev := <-events:
			if ev != want {
				t.Fatalf("event %+v, want %+v", ev, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no event, want %+v", want)
		}
	}

	if _, _, _, aerr := a.Acquire(ctx, testTenant, "moved", "n1"); aerr != nil {
		t.Fatal(aerr)
	}

	_, goneRev, _, err := a.Acquire(ctx, testTenant, "gone", "n1")
	if err != nil {
		t.Fatal(err)
	}

	// The key is deleted while no watch runs. b's delete leaves a marker,
	// which the new watch reads in its first pass.
	killWatch(t, a, func() {
		if err := b.Delete(ctx, testTenant, "gone", goneRev); err != nil {
			t.Error(err)
		}
	})

	expect(lost{"gone", true})

	// The restarted watch still reports a takeover.
	if _, _, _, err := b.Acquire(ctx, testTenant, "moved", "n2"); err != nil {
		t.Fatal(err)
	}

	expect(lost{"moved", false})

	// And it survives another restart.
	if _, _, _, err := a.Acquire(ctx, testTenant, "again", "n1"); err != nil {
		t.Fatal(err)
	}

	killWatch(t, a, nil)

	if _, _, _, err := b.Acquire(ctx, testTenant, "again", "n2"); err != nil {
		t.Fatal(err)
	}

	expect(lost{"again", false})
}

// endFirstPass reports an owned key the restarted watch's first pass did not
// read, unless a write of it is in flight.
func TestOwnedWatchResyncDropsUnseen(t *testing.T) {
	w := newBareWatch()
	w.owned["t.seen"] = ownedState{rev: 1, epoch: 1}
	w.owned["t.unseen"] = ownedState{rev: 2, epoch: 1}
	w.owned["t.flying"] = ownedState{rev: 3, epoch: 1}
	w.flights["t.flying"] = nil
	w.resync = map[string]struct{}{"t.seen": {}}

	w.endFirstPass()

	f := w.drain()
	if len(f) != 1 || f[0].key != "t.unseen" || !f[0].deleted {
		t.Fatalf("fires %+v, want t.unseen deleted", f)
	}

	if _, ok := w.owned["t.seen"]; !ok {
		t.Fatal("a key the pass read was dropped")
	}

	if w.resync != nil {
		t.Fatal("resync still set")
	}
}
