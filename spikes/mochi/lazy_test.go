package main

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
)

// storedDev1 is a session the cluster layer would hold in KV: one
// subscription and one QoS 1 message sent but never acknowledged.
func storedDev1() map[string]storedSession {
	return map[string]storedSession{
		"dev-1": {
			ProtocolVersion: 5,
			Expiry:          3600,
			Subs:            []packets.Subscription{{Filter: "a/#", Qos: 1}},
			Inflight: []packets.Packet{{
				FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
				PacketID:    7,
				TopicName:   "a/pending",
				Payload:     []byte("pending"),
				Created:     time.Now().Unix(),
			}},
		},
	}
}

// Question 1: a session loaded in OnSessionEstablish is one mochi takes over.
func TestLazySessionMaterialization(t *testing.T) {
	hook := newSessionHook(storedDev1())
	rec := new(recorder)
	srv, addr := start(t, hook.wire, hook, rec)

	c, ca := connect(t, addr, "dev-1", false, false)

	if !ca.SessionPresent {
		t.Fatal("CONNACK session present = false, want true")
	}

	if got := hook.materialized.Load(); got != 1 {
		t.Fatalf("materialized %d sessions, want 1", got)
	}

	// The stored inflight is resent after the CONNACK, flagged DUP, under
	// its stored packet ID, and the PUBACK clears it (OnQosComplete, where
	// the cluster layer would advance the cursor).
	m := c.next(t)
	if m.topic != "a/pending" || m.payload != "pending" || m.qos != 1 {
		t.Fatalf("first message = %+v, want the stored inflight", m)
	}

	resent := rec.sentPublish("a/pending")
	if len(resent) != 1 || !resent[0].FixedHeader.Dup || resent[0].PacketID != 7 {
		t.Fatalf("resent = %+v, want one DUP publish with packet ID 7", resent)
	}

	eventually(t, "PUBACK of packet 7", func() bool { return rec.completed(7) })

	// The stored subscription routes new messages.
	if err := srv.Publish("a/b", []byte("live"), false, 1); err != nil {
		t.Fatal(err)
	}

	if m := c.next(t); m.topic != "a/b" || m.payload != "live" || m.qos != 1 {
		t.Fatalf("live message = %+v", m)
	}

	if got := atomic.LoadInt64(&srv.Info.Subscriptions); got != 1 {
		t.Fatalf("Info.Subscriptions = %d, want 1 (no double count on takeover)", got)
	}

	if got := atomic.LoadInt64(&srv.Info.Inflight); got != 0 {
		t.Fatalf("Info.Inflight = %d, want 0 after the PUBACK", got)
	}
}

// Controls: no stored session, and a clean start that discards one.
func TestLazySessionControls(t *testing.T) {
	t.Run("nothing stored", func(t *testing.T) {
		hook := newSessionHook(nil)
		_, addr := start(t, hook.wire, hook)

		if _, ca := connect(t, addr, "dev-1", false, false); ca.SessionPresent {
			t.Fatal("session present with nothing stored")
		}
	})

	t.Run("clean start", func(t *testing.T) {
		hook := newSessionHook(storedDev1())
		_, addr := start(t, hook.wire, hook)

		c, ca := connect(t, addr, "dev-1", true, false)
		if ca.SessionPresent {
			t.Fatal("session present on clean start")
		}

		if _, ok := hook.stored("dev-1"); ok {
			t.Fatal("clean start kept the stored session")
		}

		select {
		case m := <-c.msgs:
			t.Fatalf("clean start got %+v", m)
		case <-time.After(200 * time.Millisecond):
		}
	})
}
