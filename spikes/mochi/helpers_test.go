package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

const waitTimeout = 5 * time.Second

func start(t *testing.T, wire func(*mqtt.Server), hooks ...mqtt.Hook) (*mqtt.Server, string) {
	t.Helper()

	srv, addr, err := startServer(nil, wire, hooks...)
	if err != nil {
		t.Fatal(err)
	}

	// Registered first, so it runs after every client's cleanup: mochi's
	// Close can deadlock while clients come and go.
	t.Cleanup(func() { closeServer(srv) })

	return srv, addr
}

type msg struct {
	topic   string
	payload string
	qos     byte
	props   *paho.PublishProperties
}

type client struct {
	*paho.Client
	msgs chan msg
}

// connect dials a 5.0 client. With manualAck the client never acks a
// QoS 1 message, which leaves it inflight in the session.
func connect(t *testing.T, addr, id string, clean, manualAck bool) (*client, *paho.Connack) {
	t.Helper()

	expiry := uint32(0)
	if !clean {
		expiry = 3600
	}

	return connectWith(t, addr, &paho.Connect{
		ClientID:   id,
		CleanStart: clean,
		KeepAlive:  30,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry, RequestProblemInfo: true},
	}, manualAck)
}

func connectWith(t *testing.T, addr string, cp *paho.Connect, manualAck bool) (*client, *paho.Connack) {
	t.Helper()

	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	msgs := make(chan msg, 64)
	c := paho.NewClient(paho.ClientConfig{
		Conn:                       conn,
		EnableManualAcknowledgment: manualAck,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				msgs <- msg{topic: pr.Packet.Topic, payload: string(pr.Packet.Payload), qos: pr.Packet.QoS, props: pr.Packet.Properties}

				return true, nil
			},
		},
	})

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	ca, err := c.Connect(ctx, cp)
	if err != nil {
		t.Fatalf("connect %s: %v", cp.ClientID, err)
	}

	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })

	return &client{Client: c, msgs: msgs}, ca
}

func (c *client) subscribe(t *testing.T, filter string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	if _, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}}}); err != nil {
		t.Fatal(err)
	}
}

func (c *client) next(t *testing.T) msg {
	t.Helper()

	select {
	case m := <-c.msgs:
		return m
	case <-time.After(waitTimeout):
		t.Fatal("no message")

		return msg{}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// recorder stands in for a storage hook such as badger, and records the
// packets mochi writes to clients.
type recorder struct {
	mqtt.HookBase

	mu           sync.Mutex
	unsubscribed int
	qosDropped   int
	expired      int
	qosComplete  []uint16
	sent         []packets.Packet
	established  []*mqtt.Client
}

func (r *recorder) ID() string { return "spike-recorder" }

func (r *recorder) Provides(b byte) bool {
	switch b {
	case mqtt.OnUnsubscribed, mqtt.OnQosDropped, mqtt.OnClientExpired, mqtt.OnQosComplete,
		mqtt.OnPacketSent, mqtt.OnSessionEstablished:
		return true
	}

	return false
}

func (r *recorder) OnUnsubscribed(*mqtt.Client, packets.Packet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unsubscribed++
}

func (r *recorder) OnQosDropped(*mqtt.Client, packets.Packet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.qosDropped++
}

func (r *recorder) OnClientExpired(*mqtt.Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expired++
}

func (r *recorder) OnQosComplete(_ *mqtt.Client, pk packets.Packet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.qosComplete = append(r.qosComplete, pk.PacketID)
}

func (r *recorder) OnPacketSent(_ *mqtt.Client, pk packets.Packet, _ []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, pk)
}

func (r *recorder) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.established = append(r.established, cl)
}

func (r *recorder) storageEvents() (unsub, dropped, expired int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.unsubscribed, r.qosDropped, r.expired
}

func (r *recorder) completed(id uint16) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, v := range r.qosComplete {
		if v == id {
			return true
		}
	}

	return false
}

// sentPublish returns the PUBLISH packets written with topic.
func (r *recorder) sentPublish(topic string) []packets.Packet {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []packets.Packet

	for _, pk := range r.sent {
		if pk.FixedHeader.Type == packets.Publish && pk.TopicName == topic {
			out = append(out, pk)
		}
	}

	return out
}

// subscribed reports whether the topic index routes topic to client id.
func subscribed(srv *mqtt.Server, id, topic string) bool {
	_, ok := srv.Topics.Subscribers(topic).Subscriptions[id]

	return ok
}
