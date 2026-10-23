package encoding

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/xraph/tether/topic"
)

func startServer(t *testing.T) (*server.Server, *nats.Conn) {
	t.Helper()

	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatal(err)
	}

	go srv.Start()

	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server not ready")
	}

	t.Cleanup(srv.Shutdown)

	return srv, dial(t, srv)
}

func dial(t *testing.T, srv *server.Server) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(srv.ClientURL(), nats.MaxReconnects(0))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	return nc
}

// TestAgainstRealServer subscribes the encoded filters to a real nats-server,
// publishes the encoded topics, and checks that what the server delivers is
// what MQTT matching says. It validates the hand-written matcher too.
func TestAgainstRealServer(t *testing.T) {
	_, nc := startServer(t)
	t.Logf("nats-server %s", server.VERSION)

	levels := []string{"a", "b", "", "$SYS", ".", " ", "*", ">", "é", "=a", "a.b"}
	wild := append([]string{"+"}, levels...)

	var topics, filters []string

	for _, a := range levels {
		topics = append(topics, a)

		for _, b := range levels {
			topics = append(topics, a+"/"+b)
		}
	}

	for _, a := range wild {
		filters = append(filters, a, a+"/#")

		for _, b := range wild {
			filters = append(filters, a+"/"+b)
		}
	}

	filters = append(filters, "#", "+/#", "$SYS/#")

	type subscription struct {
		filter string
		ch     chan *nats.Msg
	}

	var subs []subscription

	for _, f := range filters {
		if topic.Valid(f, true, 0) != nil {
			continue
		}

		ch := make(chan *nats.Msg, len(topics)*2+8)

		for _, s := range EncodeFilter(tenantID, f) {
			sub, err := nc.ChanSubscribe("f."+s, ch)
			if err != nil {
				t.Fatalf("subscribe %q for filter %q: %v", s, f, err)
			}

			_ = sub.SetPendingLimits(-1, -1)
		}

		subs = append(subs, subscription{f, ch})
	}

	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, name := range topics {
		if topic.Valid(name, false, 0) != nil {
			continue
		}

		if err := nc.Publish("f."+EncodeTopic(tenantID, name), []byte(name)); err != nil {
			t.Fatalf("publish topic %q: %v", name, err)
		}
	}

	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	var compared int

	for _, s := range subs {
		// A filter with two subjects can see one message twice; MQTT dedup is the
		// receiver's job, so compare sets.
		got := map[string]bool{}

		for len(s.ch) > 0 {
			got[string((<-s.ch).Data)] = true
		}

		for _, name := range topics {
			if !mqttValid(s.filter, name) {
				continue
			}

			compared++

			if want := topic.Match(s.filter, name); got[name] != want {
				t.Fatalf("filter %q topic %q: mqtt %v, nats-server delivered %v", s.filter, name, want, got[name])
			}
		}
	}

	t.Logf("%d filters, %d topics, %d pairs agree with a real nats-server", len(subs), len(topics), compared)
}

// worstTopic is the longest encoded form for n bytes: n slashes, n+1 empty levels.
func worstTopic(n int) string { return strings.Repeat("/", n) }

func TestLongTopicOnRealServer(t *testing.T) {
	srv, nc := startServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)

	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "LOG", Subjects: []string{"d.>"}}); err != nil {
		t.Fatal(err)
	}

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "RETAINED"})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("nats-server %s, max_control_line %d", server.VERSION, server.MAX_CONTROL_LINE_SIZE)

	probe := func(n int) error {
		name := worstTopic(n)
		subj := EncodeTopic(tenantID, name)

		// Core publish and a wildcard subscription.
		nc2 := dial(t, srv)

		sub, err := nc2.SubscribeSync("f." + EncodeFilter(tenantID, "#")[0])
		if err != nil {
			return fmt.Errorf("core subscribe: %w", err)
		}

		if err := nc2.Flush(); err != nil {
			return fmt.Errorf("core subscribe flush: %w", err)
		}

		if err := nc2.Publish("f."+subj, []byte("x")); err != nil {
			return fmt.Errorf("core publish: %w", err)
		}

		if _, err := sub.NextMsg(2 * time.Second); err != nil {
			return fmt.Errorf("core deliver: %w", err)
		}

		// QoS 1 log: publish with a header (HPUB), read back through a filtered consumer.
		msg := &nats.Msg{Subject: "d." + subj, Data: []byte("x"), Header: nats.Header{"Nats-Msg-Id": {"0123456789abcdef0123456789abcdef"}}}
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("jetstream publish: %w", err)
		}

		cons, err := js.OrderedConsumer(ctx, "LOG", jetstream.OrderedConsumerConfig{FilterSubjects: []string{"d." + EncodeFilter(tenantID, "#")[0]}})
		if err != nil {
			return fmt.Errorf("ordered consumer: %w", err)
		}

		if _, err := cons.Next(jetstream.FetchMaxWait(2 * time.Second)); err != nil {
			return fmt.Errorf("consumer next: %w", err)
		}

		// Retained KV.
		key := "r." + subj
		if _, err := kv.Put(ctx, key, []byte("x")); err != nil {
			return fmt.Errorf("kv put: %w", err)
		}

		if e, err := kv.Get(ctx, key); err != nil || e.Key() != key {
			return fmt.Errorf("kv get: %w", err)
		}

		return nil
	}

	for _, n := range []int{1, 64, 1024} {
		if err := probe(n); err != nil {
			t.Fatalf("%d byte worst-case topic: %v", n, err)
		}
	}

	t.Logf("1024 byte worst-case topic (subject %d bytes): core pub/sub, JetStream HPUB + ordered consumer, KV put/get all pass", len("d.")+len(EncodeTopic(tenantID, worstTopic(1024))))

	// Find where the stricter operations stop working, to size the headroom.
	last := 1024

	for _, n := range []int{1500, 1700, 1800, 1900, 1950, 2000, 2020, 2040, 2100, 4000} {
		if err := probe(n); err != nil {
			t.Logf("first failing worst-case topic length: %d bytes (subject %d bytes): %v", n, len("d.")+len(EncodeTopic(tenantID, worstTopic(n))), err)

			break
		}

		last = n
	}

	t.Logf("largest worst-case topic that passed every probe: %d bytes", last)
}
