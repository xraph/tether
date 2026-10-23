package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Question 2: a persistent session is flushed and evicted at DISCONNECT with
// no storage events, nothing left in the topic index, and comes back from
// the store on the next CONNECT.
func TestEvictAfterDisconnect(t *testing.T) {
	hook := newSessionHook(nil)
	rec := new(recorder)
	srv, addr := start(t, hook.wire, hook, rec)

	// A persistent session with one subscription and one unacknowledged
	// QoS 1 message.
	c, _ := connect(t, addr, "dev-1", false, true)
	c.subscribe(t, "a/#")

	if err := srv.Publish("a/unacked", []byte("x"), false, 1); err != nil {
		t.Fatal(err)
	}

	c.next(t)

	if err := c.Disconnect(&paho.Disconnect{}); err != nil {
		t.Fatal(err)
	}

	eventually(t, "eviction", func() bool { return hook.evicted.Load() == 1 })

	if _, ok := srv.Clients.Get("dev-1"); ok {
		t.Fatal("client still in srv.Clients")
	}

	if subscribed(srv, "dev-1", "a/b") {
		t.Fatal("topic index still routes a/b to dev-1")
	}

	if n := atomic.LoadInt64(&srv.Info.Subscriptions); n != 0 {
		t.Fatalf("Info.Subscriptions = %d, want 0", n)
	}

	if n := atomic.LoadInt64(&srv.Info.Inflight); n != 0 {
		t.Fatalf("Info.Inflight = %d, want 0", n)
	}

	if u, d, e := rec.storageEvents(); u+d+e != 0 {
		t.Fatalf("storage events during eviction: unsubscribed %d, qos dropped %d, expired %d", u, d, e)
	}

	ss, ok := hook.stored("dev-1")
	if !ok || len(ss.Subs) != 1 || len(ss.Inflight) != 1 {
		t.Fatalf("flushed session = %+v, want 1 subscription and 1 inflight", ss)
	}

	// A publish while the session lives only in the store reaches no one
	// on this node; in Tether the cursor replay covers it.
	if err := srv.Publish("a/offline", []byte("y"), false, 1); err != nil {
		t.Fatal(err)
	}

	// Reconnect: loaded again, inflight resent, subscription live.
	c2, ca := connect(t, addr, "dev-1", false, false)
	if !ca.SessionPresent {
		t.Fatal("session present = false after evict and reload")
	}

	if m := c2.next(t); m.topic != "a/unacked" {
		t.Fatalf("first message after reload = %+v, want the inflight", m)
	}

	if err := srv.Publish("a/b", []byte("z"), false, 1); err != nil {
		t.Fatal(err)
	}

	if m := c2.next(t); m.topic != "a/b" {
		t.Fatalf("message after reload = %+v", m)
	}
}

// Control: mochi's own calls for dropping a session fire the events a
// storage hook acts on, which is why eviction can't use them.
func TestMochiDropFiresStorageEvents(t *testing.T) {
	rec := new(recorder)
	srv, addr := start(t, nil, rec)

	c, _ := connect(t, addr, "dev-1", false, true)
	c.subscribe(t, "a/#")

	if err := srv.Publish("a/unacked", []byte("x"), false, 1); err != nil {
		t.Fatal(err)
	}

	c.next(t)

	if err := c.Disconnect(&paho.Disconnect{}); err != nil {
		t.Fatal(err)
	}

	var cl *mqtt.Client

	eventually(t, "offline session", func() bool {
		var ok bool
		cl, ok = srv.Clients.Get("dev-1")

		return ok && cl.Closed()
	})

	srv.UnsubscribeClient(cl)
	cl.ClearInflights()
	srv.Clients.Delete(cl.ID)

	if u, d, _ := rec.storageEvents(); u != 1 || d != 1 {
		t.Fatalf("unsubscribed %d, qos dropped %d; want 1 and 1", u, d)
	}
}

// The OnDisconnect of a connection that was taken over must not evict:
// srv.Clients is keyed by client ID, so deleting by ID removes the new owner.
func TestEvictSkipsTakenOverSession(t *testing.T) {
	hook := newSessionHook(nil)
	rec := new(recorder)
	srv, addr := start(t, hook.wire, hook, rec)

	c1, _ := connect(t, addr, "dev-1", false, false)
	c1.subscribe(t, "a/#")

	c2, ca := connect(t, addr, "dev-1", false, false)
	if !ca.SessionPresent {
		t.Fatal("takeover: session present = false")
	}

	eventually(t, "old connection's OnDisconnect", func() bool { return hook.skipped.Load() == 1 })

	if hook.evicted.Load() != 0 {
		t.Fatal("evicted on a taken-over connection")
	}

	if err := srv.Publish("a/b", []byte("z"), false, 1); err != nil {
		t.Fatal(err)
	}

	if m := c2.next(t); m.topic != "a/b" {
		t.Fatalf("new owner got %+v", m)
	}

	// What an unguarded eviction of the old client would do.
	rec.mu.Lock()
	old := rec.established[0]
	rec.mu.Unlock()

	if !old.IsTakenOver() {
		t.Fatal("first client not marked taken over")
	}

	srv.Clients.Delete(old.ID)

	if _, ok := srv.Clients.Get("dev-1"); ok {
		t.Fatal("expected the ID-keyed delete to remove the new owner")
	}
}

// delayHook holds a CONNECT between OnSessionEstablish and mochi's session
// lookup, so a test can end the previous connection inside that window.
type delayHook struct {
	mqtt.HookBase

	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (h *delayHook) ID() string { return "spike-delay" }

func (h *delayHook) Provides(b byte) bool { return b == mqtt.OnSessionEstablish }

func (h *delayHook) OnSessionEstablish(*mqtt.Client, packets.Packet) {
	if h.armed.CompareAndSwap(true, false) {
		close(h.entered)
		<-h.release
	}
}

// A reconnect can reach OnSessionEstablish while the previous connection is
// still live (so nothing is loaded) and that connection can then end before
// mochi looks the session up. Evicting then would leave the reconnect with
// no session. The connecting count makes the eviction wait for the next
// disconnect instead.
func TestEvictRacesReconnect(t *testing.T) {
	for _, guard := range []bool{true, false} {
		name := "with connecting guard"
		if !guard {
			name = "without connecting guard"
		}

		t.Run(name, func(t *testing.T) {
			hook := newSessionHook(nil)
			hook.noConnectGuard = !guard
			delay := &delayHook{entered: make(chan struct{}), release: make(chan struct{})}
			srv, addr := start(t, hook.wire, hook, delay)

			c1, _ := connect(t, addr, "dev-1", false, false)
			c1.subscribe(t, "a/#")

			delay.armed.Store(true)

			type result struct {
				c  *client
				ca *paho.Connack
			}

			done := make(chan result, 1)

			go func() {
				c2, ca := connect(t, addr, "dev-1", false, false)
				done <- result{c2, ca}
			}()

			<-delay.entered

			if err := c1.Disconnect(&paho.Disconnect{}); err != nil {
				t.Error(err)
			}

			eventually(t, "first connection's OnDisconnect", func() bool {
				return hook.evicted.Load()+hook.skipped.Load() == 1
			})
			close(delay.release)

			r := <-done

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()

			delivered := false

			if err := srv.Publish("a/b", []byte("z"), false, 1); err != nil {
				t.Fatal(err)
			}

			select {
			case <-r.c.msgs:
				delivered = true
			case <-ctx.Done():
			}

			t.Logf("evicted %d, skipped %d, session present %v, subscription delivered %v",
				hook.evicted.Load(), hook.skipped.Load(), r.ca.SessionPresent, delivered)

			if guard && (!r.ca.SessionPresent || !delivered) {
				t.Fatal("guarded eviction lost the session")
			}

			if !guard && (r.ca.SessionPresent || delivered) {
				t.Fatal("expected the unguarded eviction to lose the session")
			}
		})
	}
}
