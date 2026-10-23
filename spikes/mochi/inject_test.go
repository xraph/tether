package main

import (
	"context"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Question 3: a message from the cluster is delivered locally with its 5.0
// properties, and the hook knows not to send it back.
func TestInjectFromCluster(t *testing.T) {
	hook := new(clusterHook)
	srv, addr := start(t, func(s *mqtt.Server) { hook.injector = newInjector(s) }, hook)

	sub, _ := connect(t, addr, "sub", true, false)
	sub.subscribe(t, "a/#")

	expiry := uint32(60)
	props := packets.Properties{
		ContentType:           "application/json",
		MessageExpiryInterval: expiry,
		User:                  []packets.UserProperty{{Key: "origin", Val: "node-2"}},
	}

	if err := inject(srv, hook.injector, "a/from-cluster", []byte(`{"v":1}`), 1, false, props); err != nil {
		t.Fatal(err)
	}

	m := sub.next(t)
	if m.topic != "a/from-cluster" || m.qos != 1 {
		t.Fatalf("injected message = %+v, want a/from-cluster at QoS 1", m)
	}

	if m.props == nil || m.props.ContentType != "application/json" ||
		len(m.props.User) != 1 || m.props.User[0] != (paho.UserProperty{Key: "origin", Value: "node-2"}) ||
		m.props.MessageExpiry == nil || *m.props.MessageExpiry == 0 {
		t.Fatalf("properties = %+v, want content type, user property and message expiry", m.props)
	}

	if n := hook.sentToCluster.Load(); n != 0 {
		t.Fatalf("sent to cluster %d times, want 0", n)
	}

	// mochi's own inline client (the admin and HTTP publish path) is inline
	// too, so cl.Net.Inline alone can't tell the two apart. Its messages
	// must reach the cluster.
	if err := srv.Publish("a/from-admin", []byte("x"), false, 1); err != nil {
		t.Fatal(err)
	}

	if m := sub.next(t); m.topic != "a/from-admin" {
		t.Fatalf("admin message = %+v", m)
	}

	if hook.inlineSeen.Load() != 2 || hook.sentToCluster.Load() != 1 {
		t.Fatalf("inline seen %d, sent %d; want 2 and 1", hook.inlineSeen.Load(), hook.sentToCluster.Load())
	}

	// A device publish goes to the cluster.
	pub, _ := connect(t, addr, "pub", true, false)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	if _, err := pub.Publish(ctx, &paho.Publish{Topic: "a/from-device", QoS: 1, Payload: []byte("d")}); err != nil {
		t.Fatal(err)
	}

	if m := sub.next(t); m.topic != "a/from-device" {
		t.Fatalf("device message = %+v", m)
	}

	if n := hook.sentToCluster.Load(); n != 2 {
		t.Fatalf("sent to cluster %d times, want 2", n)
	}

	// Retained: mochi stores an injected retained message in its local
	// cache, and the retained write-through sees the injector and skips it.
	if err := inject(srv, hook.injector, "a/r", []byte("kept"), 1, true, packets.Properties{}); err != nil {
		t.Fatal(err)
	}

	sub.next(t)

	if got := len(srv.Topics.Messages("a/r")); got != 1 {
		t.Fatalf("retained messages on a/r = %d, want 1", got)
	}

	if n := hook.retainForwards.Load(); n != 0 {
		t.Fatalf("retained forwards %d, want 0", n)
	}
}

// Not about clustering, found on the way: mochi drops user properties from
// PUBLISH to a client that sent Request Problem Information 0, and a hook
// can undo it in OnPacketEncode.
func TestUserPropertiesWithoutProblemInfo(t *testing.T) {
	for _, fix := range []bool{false, true} {
		name := "mochi as is"
		if fix {
			name = "with keepPublishProps"
		}

		t.Run(name, func(t *testing.T) {
			hook := new(clusterHook)

			hooks := []mqtt.Hook{hook}
			if fix {
				hooks = append(hooks, new(keepPublishProps))
			}

			_, addr := start(t, func(s *mqtt.Server) { hook.injector = newInjector(s) }, hooks...)

			sub, _ := connectWith(t, addr, &paho.Connect{
				ClientID: "quiet", CleanStart: true, KeepAlive: 30,
				Properties: &paho.ConnectProperties{RequestProblemInfo: false},
			}, false)
			sub.subscribe(t, "a/#")

			pub, _ := connect(t, addr, "pub", true, false)

			ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
			defer cancel()

			if _, err := pub.Publish(ctx, &paho.Publish{
				Topic: "a/b", QoS: 1, Payload: []byte("x"),
				Properties: &paho.PublishProperties{User: paho.UserProperties{{Key: "k", Value: "v"}}},
			}); err != nil {
				t.Fatal(err)
			}

			m := sub.next(t)

			got := m.props != nil && len(m.props.User) == 1
			if got != fix {
				t.Fatalf("user property delivered = %v, want %v", got, fix)
			}
		})
	}
}
