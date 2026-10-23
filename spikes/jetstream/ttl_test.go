package main

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// markerTTL is the bucket's LimitMarkerTTL: how long the marker left by an
// expired key stays in the stream before it expires too.
const markerTTL = 3 * time.Second

type seen struct {
	op      jetstream.KeyValueOp
	rev     uint64
	at      time.Time // when the watcher got it
	created time.Time // the stream's timestamp for the message
}

// watchKey records every update a watcher on a different server sees for key.
type watchLog struct {
	mu  sync.Mutex
	all []seen
	ch  chan seen
}

func watchKey(t *testing.T, kv jetstream.KeyValue, key string) *watchLog {
	t.Helper()

	w, err := kv.Watch(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = w.Stop() })

	l := &watchLog{ch: make(chan seen, 64)}

	go func() {
		for e := range w.Updates() {
			if e == nil {
				continue // end of initial values
			}

			s := seen{op: e.Operation(), rev: e.Revision(), at: time.Now(), created: e.Created()}

			l.mu.Lock()
			l.all = append(l.all, s)
			l.mu.Unlock()

			l.ch <- s
		}
	}()

	return l
}

// next waits for the next update that isn't a put.
func (l *watchLog) nextMarker(t *testing.T, timeout time.Duration) (seen, bool) {
	t.Helper()

	deadline := time.After(timeout)

	for {
		select {
		case s := <-l.ch:
			if s.op != jetstream.KeyValuePut {
				return s, true
			}
		case <-deadline:
			return seen{}, false
		}
	}
}

// rawPut is the cluster layer's session write: a publish straight to the KV
// stream with a per-message TTL and an expected last subject sequence, so it
// stays a compare-and-set. expect < 0 leaves the expected-sequence header out.
func rawPut(t *testing.T, js jetstream.JetStream, bucket, key string, ttl time.Duration, expect int64, val string) (uint64, error) {
	t.Helper()

	m := nats.NewMsg("$KV." + bucket + "." + key)
	m.Data = []byte(val)

	if ttl > 0 {
		m.Header.Set(jetstream.MsgTTLHeader, ttl.String())
	}

	if expect >= 0 {
		m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmt.Sprint(expect))
	}

	ack, err := js.PublishMsg(ctxT(t, 10*time.Second), m)
	if err != nil {
		return 0, err
	}

	return ack.Sequence, nil
}

// Review Focus 5: a KV key expires with no writer present, and watchers see it.
func TestTTLExpiryReachesWatcher(t *testing.T) {
	_, js := connect(t, 0)  // the writer
	_, jsW := connect(t, 2) // the watcher, on another server

	_, err := js.CreateKeyValue(ctxT(t, 30*time.Second), jetstream.KeyValueConfig{
		Bucket: "SESS", History: 1, Replicas: 3, Storage: jetstream.FileStorage,
		LimitMarkerTTL: markerTTL,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { cleanupDelete(t, js.DeleteKeyValue, "SESS") })

	kvW, err := jsW.KeyValue(ctxT(t, 10*time.Second), "SESS")
	if err != nil {
		t.Fatal(err)
	}

	stream, err := js.Stream(ctxT(t, 10*time.Second), "KV_SESS")
	if err != nil {
		t.Fatal(err)
	}

	info := stream.CachedInfo()
	t.Logf("KV_SESS: allow_msg_ttl=%v subject_delete_marker_ttl=%v max_msgs_per_subject=%d",
		info.Config.AllowMsgTTL, info.Config.SubjectDeleteMarkerTTL, info.Config.MaxMsgsPerSubject)

	const key1 = "s.ttnt_x.dev-1"

	t.Run("expiry delivers a marker to a watcher", func(t *testing.T) {
		w := watchKey(t, kvW, key1)

		seq, err := rawPut(t, js, "SESS", key1, 2*time.Second, 0, "session")
		if err != nil {
			t.Fatalf("raw publish with Nats-TTL and expected sequence 0: %v", err)
		}

		acked := time.Now()

		m, ok := w.nextMarker(t, 15*time.Second)
		if !ok {
			t.Fatal("no marker within 15 s of a 2 s TTL")
		}

		delay := m.at.Sub(acked)

		raw, err := stream.GetLastMsgForSubject(ctxT(t, 10*time.Second), "$KV.SESS."+key1)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("put seq %d; marker seq %d seen by the watcher %v after the ack, op %v; marker headers %v",
			seq, m.rev, delay.Round(time.Millisecond), m.op, raw.Header)

		if delay > 3*time.Second {
			t.Errorf("marker arrived %v after the ack, plan expects within 3 s", delay)
		}

		// The TTL runs from the stream's timestamp for the put, which is
		// earlier than the ack the client sees. Same machine, same clock.
		var put seen

		w.mu.Lock()
		for _, s := range w.all {
			if s.rev == seq {
				put = s
			}
		}
		w.mu.Unlock()

		sinceStored := m.at.Sub(put.created)
		t.Logf("marker %v after the stream stored the put; marker's own stream timestamp %v after it",
			sinceStored.Round(time.Millisecond), m.created.Sub(put.created).Round(time.Millisecond))

		if put.created.IsZero() || sinceStored < 2*time.Second {
			t.Errorf("marker %v after the put was stored, before the 2 s TTL", sinceStored)
		}

		if _, err := kvW.Get(t.Context(), key1); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Errorf("Get after expiry: %v, want key not found", err)
		}
	})

	t.Run("stale expected sequence is rejected", func(t *testing.T) {
		const key = "s.ttnt_x.dev-2"

		s1, err := rawPut(t, js, "SESS", key, 30*time.Second, 0, "v1")
		if err != nil {
			t.Fatal(err)
		}

		s2, err := rawPut(t, js, "SESS", key, 30*time.Second, int64(s1), "v2")
		if err != nil {
			t.Fatalf("CAS from the current sequence: %v", err)
		}

		_, err = rawPut(t, js, "SESS", key, 30*time.Second, int64(s1), "stale")
		if apiCode(err) != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Errorf("CAS from a stale sequence: %v, want wrong last sequence", err)
		}

		_, err = rawPut(t, js, "SESS", key, 30*time.Second, 0, "create")
		if apiCode(err) != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Errorf("create (expected 0) over a live key: %v, want wrong last sequence", err)
		}

		t.Logf("s1=%d s2=%d; stale CAS and create-over-live both refused with %d", s1, s2, jetstream.JSErrCodeStreamWrongLastSequence)
	})

	t.Run("a CAS with a new TTL renews the expiry", func(t *testing.T) {
		const key = "s.ttnt_x.dev-3"

		w := watchKey(t, kvW, key)

		s1, err := rawPut(t, js, "SESS", key, 2*time.Second, 0, "v1")
		if err != nil {
			t.Fatal(err)
		}

		t0 := time.Now()

		time.Sleep(1200 * time.Millisecond)

		s2, err := rawPut(t, js, "SESS", key, 2*time.Second, int64(s1), "v2")
		if err != nil {
			t.Fatal(err)
		}

		renewed := time.Now()

		time.Sleep(time.Until(t0.Add(2500 * time.Millisecond)))

		e, err := kvW.Get(t.Context(), key)
		if err != nil {
			t.Fatalf("Get 2.5 s after the first write, 1.3 s after the renewal: %v", err)
		}

		if e.Revision() != s2 {
			t.Errorf("Get revision %d, want %d", e.Revision(), s2)
		}

		m, ok := w.nextMarker(t, 15*time.Second)
		if !ok {
			t.Fatal("no marker after the renewed TTL")
		}

		t.Logf("first write %d, renewal %d at +%v; marker %d at +%v after the first write, +%v after the renewal",
			s1, s2, renewed.Sub(t0).Round(time.Millisecond), m.rev, m.at.Sub(t0).Round(time.Millisecond), m.at.Sub(renewed).Round(time.Millisecond))

		if m.at.Sub(renewed) < 2*time.Second {
			t.Errorf("marker %v after the renewal, before its 2 s TTL", m.at.Sub(renewed))
		}

		w.mu.Lock()
		defer w.mu.Unlock()

		markers := 0

		for _, s := range w.all {
			if s.op != jetstream.KeyValuePut {
				markers++
			}
		}

		if markers != 1 {
			t.Errorf("watcher saw %d markers, want 1 (the replaced message must not leave one)", markers)
		}
	})

	t.Run("writing after expiry", func(t *testing.T) {
		const key4 = "s.ttnt_x.dev-4"

		s, err := rawPut(t, js, "SESS", key4, 2*time.Second, 0, "v")
		if err != nil {
			t.Fatal(err)
		}

		w := watchKey(t, kvW, key4)

		m, ok := w.nextMarker(t, 15*time.Second)
		if !ok {
			t.Fatal("no marker for key4")
		}

		// While the marker is in the stream it is the subject's last message, so
		// a create (expected 0) is refused and the CAS must name the marker.
		_, err = rawPut(t, js, "SESS", key4, 2*time.Second, 0, "create while marker present")
		if apiCode(err) != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Errorf("expected 0 while the marker is present: %v, want wrong last sequence", err)
		}

		t.Logf("expected 0 while the marker is present: %v", err)

		s2, err := rawPut(t, js, "SESS", key4, 2*time.Second, int64(m.rev), "after expiry")
		if err != nil {
			t.Errorf("CAS from the marker's sequence %d: %v", m.rev, err)
		}

		t.Logf("key4 put %d, marker %d, CAS from the marker's sequence gave %d", s, m.rev, s2)

		// After the marker's own TTL the subject is empty again: expected 0 works.
		const key6 = "s.ttnt_x.dev-6"

		if _, err := rawPut(t, js, "SESS", key6, 2*time.Second, 0, "v"); err != nil {
			t.Fatal(err)
		}

		w6 := watchKey(t, kvW, key6)

		m6, ok := w6.nextMarker(t, 15*time.Second)
		if !ok {
			t.Fatal("no marker for key6")
		}

		var (
			emptyAfter time.Duration
			lastErr    error
		)

		for {
			_, lastErr = stream.GetLastMsgForSubject(ctxT(t, 10*time.Second), "$KV.SESS."+key6)
			if errors.Is(lastErr, jetstream.ErrMsgNotFound) {
				emptyAfter = time.Since(m6.at)

				break
			}

			if time.Since(m6.at) > markerTTL+15*time.Second {
				t.Fatalf("marker still there %v after it appeared: %v", time.Since(m6.at), lastErr)
			}

			time.Sleep(100 * time.Millisecond)
		}

		s6, err := rawPut(t, js, "SESS", key6, 2*time.Second, 0, "fresh")
		if err != nil {
			t.Errorf("expected 0 after the marker expired: %v", err)
		}

		t.Logf("key6 marker %d gone %v after it appeared (LimitMarkerTTL %v); expected-0 create then gave %d",
			m6.rev, emptyAfter.Round(time.Millisecond), markerTTL, s6)

		w6.mu.Lock()
		t.Logf("key6 watcher saw %+v", w6.all)
		w6.mu.Unlock()
	})
}

// Without limit markers the stream has no AllowMsgTTL, so Nats-TTL is refused.
// This pins why the spec needs LimitMarkerTTL on every session bucket.
func TestTTLNeedsMarkersBucket(t *testing.T) {
	_, js := connect(t, 0)

	if _, err := js.CreateKeyValue(ctxT(t, 30*time.Second), jetstream.KeyValueConfig{
		Bucket: "PLAIN", History: 1, Replicas: 3, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { cleanupDelete(t, js.DeleteKeyValue, "PLAIN") })

	_, err := rawPut(t, js, "PLAIN", "s.ttnt_x.dev-1", 2*time.Second, 0, "v")
	if err == nil {
		t.Fatal("Nats-TTL accepted on a bucket without LimitMarkerTTL")
	}

	t.Logf("Nats-TTL on a bucket without markers: %v (code %d)", err, apiCode(err))
}

// kv.Create is meant to treat a marker as a deleted key and write over it. It
// reads the key first, and that read is a direct get, which any replica may
// answer. Right after expiry a replica that has not applied the marker yet
// answers wrongly, so the first Create can fail with "key exists". This test
// measures how often, over 20 keys created the moment a watcher sees each
// marker, and whether a retry gets through.
func TestCreateAfterExpiry(t *testing.T) {
	_, js := connect(t, 0)
	_, jsW := connect(t, 2)

	kv, err := js.CreateKeyValue(ctxT(t, 30*time.Second), jetstream.KeyValueConfig{
		Bucket: "SESSC", History: 1, Replicas: 3, Storage: jetstream.FileStorage,
		LimitMarkerTTL: markerTTL,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { cleanupDelete(t, js.DeleteKeyValue, "SESSC") })

	kvW, err := jsW.KeyValue(ctxT(t, 10*time.Second), "SESSC")
	if err != nil {
		t.Fatal(err)
	}

	const n = 20

	type result struct {
		firstErr error
		getErr   error
		retries  int
		ok       bool
	}

	results := make([]result, n)

	var wg sync.WaitGroup

	for i := range n {
		key := fmt.Sprintf("s.ttnt_x.ce-%d", i)
		w := watchKey(t, kvW, key)

		if _, err := rawPut(t, js, "SESSC", key, 2*time.Second, 0, "v"); err != nil {
			t.Fatal(err)
		}

		wg.Go(func() {
			if _, ok := w.nextMarker(t, 15*time.Second); !ok {
				results[i].firstErr = errors.New("no marker")

				return
			}

			_, err := kv.Create(t.Context(), key, []byte("recreated"))
			if err == nil {
				results[i].ok = true

				return
			}

			results[i].firstErr = err
			_, results[i].getErr = kv.Get(t.Context(), key)

			for range 40 {
				time.Sleep(50 * time.Millisecond)

				results[i].retries++

				if _, err := kv.Create(t.Context(), key, []byte("recreated")); err == nil {
					results[i].ok = true

					return
				}
			}
		})
	}

	wg.Wait()

	firstOK, failed := 0, 0

	for i, r := range results {
		switch {
		case r.firstErr == nil:
			firstOK++
		case r.ok:
			t.Logf("ce-%d: first Create failed (%v; Get then said %v), ok after %d retries of 50 ms", i, r.firstErr, r.getErr, r.retries)
		default:
			failed++
			t.Errorf("ce-%d: Create never succeeded: %v", i, r.firstErr)
		}
	}

	t.Logf("%d of %d Creates succeeded on the first try, %d never", firstOK, n, failed)
}
