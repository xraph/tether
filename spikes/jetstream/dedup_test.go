package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// dedupWindow is the stream's duplicate window in TestDedupWindow. It is short
// so the test can wait it out; production would use minutes.
const dedupWindow = 2 * time.Second

func pubID(t *testing.T, js jetstream.JetStream, subj, id string) *jetstream.PubAck {
	t.Helper()

	m := nats.NewMsg(subj)
	m.Data = []byte(id)
	m.Header.Set(jetstream.MsgIDHeader, id)

	ack, err := js.PublishMsg(ctxT(t, 10*time.Second), m)
	if err != nil {
		t.Fatal(err)
	}

	return ack
}

// The QoS 2 path: a DUP resend reuses the pending entry's random ID, so
// JetStream must report it as a duplicate inside the window and store it again
// only after the window.
func TestDedupWindow(t *testing.T) {
	_, js := connect(t, 0)

	if _, err := js.CreateStream(ctxT(t, 30*time.Second), jetstream.StreamConfig{
		Name: "DEDUP", Subjects: []string{"d.>"}, Replicas: 3, Storage: jetstream.FileStorage,
		Duplicates: dedupWindow,
	}); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { cleanupDelete(t, js.DeleteStream, "DEDUP") })

	const subj = "d.ttnt_x.devices.1.cmd"

	first := pubID(t, js, subj, "m-1")
	t0 := time.Now()

	if first.Duplicate {
		t.Fatal("first publish reported as duplicate")
	}

	dup := pubID(t, js, subj, "m-1")
	if !dup.Duplicate || dup.Sequence != first.Sequence {
		t.Errorf("resend inside the window: duplicate=%v seq=%d, want duplicate of %d", dup.Duplicate, dup.Sequence, first.Sequence)
	}

	// A different ID inside the window is stored.
	other := pubID(t, js, subj, "m-2")
	if other.Duplicate {
		t.Error("a different ID was reported as duplicate")
	}

	// Probe the boundary: resend every 100 ms from just before the window ends
	// until it is stored. A duplicate does not refresh the window (checked by
	// the first resend above not extending it: the boundary lands at about t0+window).
	time.Sleep(time.Until(t0.Add(dedupWindow - 500*time.Millisecond)))

	var (
		stored  *jetstream.PubAck
		probes  int
		lastDup time.Duration
	)

	for time.Since(t0) < dedupWindow+10*time.Second {
		a := pubID(t, js, subj, "m-1")
		probes++

		if !a.Duplicate {
			stored = a

			break
		}

		lastDup = time.Since(t0)

		time.Sleep(100 * time.Millisecond)
	}

	if stored == nil {
		t.Fatalf("same ID still a duplicate %v after first publish (window %v)", time.Since(t0), dedupWindow)
	}

	storedAt := time.Since(t0)

	t.Logf("window %v: duplicate at +%v (last), stored again at +%v as seq %d (first was %d), %d probes",
		dedupWindow, lastDup.Round(time.Millisecond), storedAt.Round(time.Millisecond), stored.Sequence, first.Sequence, probes)

	if storedAt < dedupWindow {
		t.Errorf("stored again at %v, inside the %v window", storedAt, dedupWindow)
	}
}

// Dedup across a stream leader change: a QoS 2 resend can land after a NATS
// server failover, so the new leader must still know the ID.
func TestDedupSurvivesLeaderChange(t *testing.T) {
	nc, js := connect(t, 0)

	s, err := js.CreateStream(ctxT(t, 30*time.Second), jetstream.StreamConfig{
		Name: "DEDUPLC", Subjects: []string{"q.>"}, Replicas: 3, Storage: jetstream.FileStorage,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { cleanupDelete(t, js.DeleteStream, "DEDUPLC") })

	first := pubID(t, js, "q.ttnt_x.a", "qos2-1")

	info, err := s.Info(ctxT(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	before := info.Cluster.Leader

	if _, err := nc.Request("$JS.API.STREAM.LEADER.STEPDOWN.DEDUPLC", nil, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	var after string

	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		info, err := s.Info(ctxT(t, 10*time.Second))
		if err == nil && info.Cluster.Leader != "" && info.Cluster.Leader != before {
			after = info.Cluster.Leader

			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if after == "" {
		t.Fatalf("leader did not change from %s", before)
	}

	var dup *jetstream.PubAck

	for time.Now().Before(deadline) {
		m := nats.NewMsg("q.ttnt_x.a")
		m.Header.Set(jetstream.MsgIDHeader, "qos2-1")

		a, err := js.PublishMsg(ctxT(t, 5*time.Second), m)
		if err == nil {
			dup = a

			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if dup == nil {
		t.Fatal("no publish accepted after the leader change")
	}

	t.Logf("leader %s -> %s; resend after the change: duplicate=%v seq=%d (first %d)", before, after, dup.Duplicate, dup.Sequence, first.Sequence)

	if !dup.Duplicate {
		t.Errorf("the new leader stored the resend as seq %d", dup.Sequence)
	}
}
