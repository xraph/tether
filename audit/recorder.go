package audit

import (
	"context"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// writeTimeout bounds one tenant lookup and Sink.Record.
const writeTimeout = 10 * time.Second

// failureLogEvery is how often a failing sink is logged at most.
const failureLogEvery = 10 * time.Second

// denyKey is what makes two ACL denials the same one.
type denyKey struct {
	tenant, client, op, topic string
}

// denyEntry is the open window of one denial: until it closes, more of the
// same are counted in suppressed and last keeps the latest.
type denyEntry struct {
	until      time.Time
	suppressed int
	last       Event
}

// Recorder is the Emitter the broker and the admin layers share. Emit puts
// an event on a bounded queue and returns; Writers goroutines take events
// off it, look the tenant up and hand both to the Sink.
//
// An ACL denial of one (tenant, client, operation, topic) is recorded once
// per DenyWindow. The ones after it within the window are counted, and when
// the window closes one more event, the latest of them, is recorded with
// Meta["suppressed"] set to the count.
type Recorder struct {
	cfg     Config
	sink    Sink
	tenants Tenants
	log     *slog.Logger
	kinds   map[string]struct{}
	now     func() time.Time

	queue   chan Event
	dropped atomic.Uint64

	// qmu guards closed and the close of queue: Emit sends under the read
	// lock, so Stop never closes the queue under a send.
	qmu    sync.RWMutex
	closed bool

	dmu    sync.Mutex
	denies map[denyKey]*denyEntry

	// base is the writers' context, canceled when Stop gives up waiting.
	base   context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	quit      chan struct{}
	writers   sync.WaitGroup
	sweeper   sync.WaitGroup

	failures atomic.Uint64
	lastLog  atomic.Int64
}

var _ Emitter = (*Recorder)(nil)

// NewRecorder returns a Recorder that is not running yet. Zero sizes in cfg
// take their defaults; nil tenants records every event with its Tether ID
// alone; nil logger discards.
func NewRecorder(cfg Config, sink Sink, tenants Tenants, logger *slog.Logger) *Recorder {
	cfg = cfg.withDefaults()

	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	ks := make(map[string]struct{}, len(cfg.Events))
	for _, k := range cfg.Events {
		ks[k] = struct{}{}
	}

	base, cancel := context.WithCancel(context.Background())

	return &Recorder{
		cfg:     cfg,
		sink:    sink,
		tenants: tenants,
		log:     logger,
		kinds:   ks,
		now:     time.Now,
		queue:   make(chan Event, cfg.QueueSize),
		denies:  make(map[denyKey]*denyEntry),
		base:    base,
		cancel:  cancel,
		quit:    make(chan struct{}),
	}
}

// Start runs the writers and the deny window sweep. It does nothing when
// auditing is off, on a second call and after Stop.
func (r *Recorder) Start() {
	if !r.cfg.Enabled || r.sink == nil {
		return
	}

	r.startOnce.Do(func() {
		r.qmu.RLock()
		closed := r.closed
		r.qmu.RUnlock()

		if closed {
			return
		}

		r.started.Store(true)

		for range r.cfg.Writers {
			r.writers.Go(r.write)
		}

		r.sweeper.Go(r.sweep)
	})
}

// Stop takes no more events, records the summaries of the deny windows still
// open, and waits for the writers to write everything queued, until ctx ends.
// Then it cancels what the writers are doing and returns ctx's error.
func (r *Recorder) Stop(ctx context.Context) error {
	var err error

	r.stopOnce.Do(func() {
		close(r.quit)
		r.sweeper.Wait()

		r.closeWindows(time.Time{})

		r.qmu.Lock()
		r.closed = true
		close(r.queue)
		r.qmu.Unlock()

		done := make(chan struct{})

		go func() {
			r.writers.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			err = ctx.Err()
		}

		r.cancel()
	})

	return err
}

// Dropped is how many events Emit dropped because the queue was full or the
// Recorder had stopped, and how many the writers gave up when Stop ran out
// of time.
func (r *Recorder) Dropped() uint64 { return r.dropped.Load() }

// Emit queues ev and returns at once. An event of a kind Config.Events does
// not list is ignored, so is everything while auditing is off. It never
// waits on the sink.
func (r *Recorder) Emit(ev Event) {
	if r == nil || !r.cfg.Enabled || r.sink == nil {
		return
	}

	if _, ok := r.kinds[Kind(ev.Action)]; !ok {
		return
	}

	if ev.At.IsZero() {
		ev.At = r.now()
	}

	if ev.Action == ActionACLDenied && r.collapse(ev) {
		return
	}

	r.enqueue(ev)
}

// enqueue puts ev on the queue, or drops and counts it.
func (r *Recorder) enqueue(ev Event) {
	r.qmu.RLock()
	defer r.qmu.RUnlock()

	if r.closed {
		r.dropped.Add(1)

		return
	}

	select {
	case r.queue <- ev:
	default:
		r.dropped.Add(1)
	}
}

// collapse reports whether ev repeats a denial whose window is open, and
// counts it if so. Otherwise it opens a window for ev, which the caller
// records. The windows are bounded by the queue size: past that, a new
// denial is recorded without one.
func (r *Recorder) collapse(ev Event) bool {
	k := denyKey{tenant: ev.TenantID, client: ev.ClientID}

	if op, ok := ev.Meta[MetaOp].(string); ok {
		k.op = op
	}

	if t, ok := ev.Meta[MetaTopic].(string); ok {
		k.topic = t
	}

	r.dmu.Lock()
	defer r.dmu.Unlock()

	if e, ok := r.denies[k]; ok && ev.At.Before(e.until) {
		e.suppressed++
		e.last = ev

		return true
	}

	if len(r.denies) < r.cfg.QueueSize {
		r.denies[k] = &denyEntry{until: ev.At.Add(r.cfg.DenyWindow)}
	}

	return false
}

// sweep closes the deny windows as they end, several times per window.
func (r *Recorder) sweep() {
	every := max(r.cfg.DenyWindow/4, time.Millisecond)

	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-r.quit:
			return
		case <-t.C:
			r.closeWindows(r.now())
		}
	}
}

// closeWindows ends the deny windows that closed before now (all of them
// for the zero time) and queues a summary for each that counted repeats.
func (r *Recorder) closeWindows(now time.Time) {
	var summaries []Event

	r.dmu.Lock()

	for k, e := range r.denies {
		if !now.IsZero() && now.Before(e.until) {
			continue
		}

		delete(r.denies, k)

		if e.suppressed == 0 {
			continue
		}

		ev := e.last
		ev.Meta = maps.Clone(ev.Meta)

		if ev.Meta == nil {
			ev.Meta = make(map[string]any, 1)
		}

		ev.Meta[MetaSuppressed] = e.suppressed
		summaries = append(summaries, ev)
	}

	r.dmu.Unlock()

	for _, ev := range summaries {
		r.enqueue(ev)
	}
}

// write is one writer: it records events until the queue is closed and
// empty. Once Stop has given up, what is left is counted as dropped.
func (r *Recorder) write() {
	for ev := range r.queue {
		if r.base.Err() != nil {
			r.dropped.Add(1)

			continue
		}

		r.record(ev)
	}
}

// record looks ev's tenant up and hands both to the sink. A failed lookup
// still records, with the Tether ID alone: a deleted tenant's last events
// matter most.
func (r *Recorder) record(ev Event) {
	ctx, cancel := context.WithTimeout(r.base, writeTimeout)
	defer cancel()

	t := Tenant{ID: ev.TenantID}

	if ev.TenantID != "" && r.tenants != nil {
		got, err := r.tenants.Get(ctx, ev.TenantID)
		if err == nil {
			t = got
		} else {
			r.log.Debug("tether/audit: tenant lookup failed, recording the Tether ID alone",
				slog.String("tenant", ev.TenantID), slog.Any("error", err))
		}
	}

	if err := r.sink.Record(ctx, ev, t); err != nil {
		n := r.failures.Add(1)
		now := r.now().UnixNano()
		last := r.lastLog.Load()

		if now-last >= int64(failureLogEvery) && r.lastLog.CompareAndSwap(last, now) {
			r.log.Warn("tether/audit: recording an event failed", slog.String("action", ev.Action),
				slog.Uint64("failures", n), slog.Any("error", err))
		}
	}
}
