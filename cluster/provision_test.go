package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether/cluster/clustertest"
)

// flakyJS is a JetStream whose stream reads and KV puts go unanswered a set
// number of times before they reach NATS, the way they do while a stream's
// leader is being elected (a member that cannot lead drops the request) or
// before its interest has reached the server the client is on (no
// responders). Everything else goes straight through.
type flakyJS struct {
	jetstream.JetStream

	mu sync.Mutex
	// readMisses is how many more Stream calls for a name go unanswered.
	readMisses map[string]int
	// readErr, when set, is what every Stream call returns instead.
	readErr error
	// putMisses is how many more KV puts get no responders; puts counts the
	// puts made.
	putMisses int
	puts      int
}

func (f *flakyJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	f.mu.Lock()

	err := f.readErr
	if err == nil && f.readMisses[name] > 0 {
		f.readMisses[name]--
		err = context.DeadlineExceeded
	}

	f.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return f.JetStream.Stream(ctx, name)
}

func (f *flakyJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := f.JetStream.KeyValue(ctx, bucket)
	if err != nil {
		return nil, err
	}

	return &flakyKV{KeyValue: kv, f: f}, nil
}

func (f *flakyJS) putCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.puts
}

// flakyKV is a bucket of a flakyJS.
type flakyKV struct {
	jetstream.KeyValue

	f *flakyJS
}

func (k *flakyKV) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	k.f.mu.Lock()

	k.f.puts++

	miss := k.f.putMisses > 0
	if miss {
		k.f.putMisses--
	}

	k.f.mu.Unlock()

	if miss {
		return 0, jetstream.ErrNoStreamResponse
	}

	return k.KeyValue.Put(ctx, key, value)
}

// A node with automatic replicas that counts fewer servers than the cell has
// reads each stream before it provisions, so it keeps the count the stream
// has. A read that goes unanswered (the stream's leader is being elected, as
// on a loaded CI runner) is read again, not taken for a stream that does not
// exist: that scaled the cell's five streams and buckets down to the late
// node's one replica. A stream that cannot be read at all fails Provision and
// keeps its replicas.
func TestProvisionAutoReplicasSurviveUnansweredReads(t *testing.T) {
	urls := clustertest.Start(t, 3)

	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.NodeID = "n1"
	cfg.Cell = "c1"
	cfg.Mode = ModeExternal
	cfg.URLs = urls
	cfg.Replicas = 3

	first, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(first.Close)

	if perr := Provision(tctx(t, 30*time.Second), first, cfg, time.Hour); perr != nil {
		t.Fatal(perr)
	}

	lateCfg := cfg
	lateCfg.NodeID = "n2"
	lateCfg.URLs = urls[:1]
	lateCfg.NoDiscovery = true
	lateCfg.Replicas = 0

	late, err := Connect(tctx(t, 20*time.Second), lateCfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(late.Close)

	if late.Replicas() != 1 {
		t.Fatalf("late node counts %d replicas, want 1 for this test", late.Replicas())
	}

	names := []string{
		cfg.LogStream(), cfg.ControlStream(),
		kvStreamPrefix + cfg.SessionsBucket(), kvStreamPrefix + cfg.RetainedBucket(), kvStreamPrefix + cfg.MembersBucket(),
	}

	misses := make(map[string]int, len(names))
	for _, name := range names {
		misses[name] = 2
	}

	flaky := &flakyJS{JetStream: late.js, readMisses: misses}
	late.js = flaky

	checkReplicas := func(when string) {
		t.Helper()

		for _, name := range names {
			st, err := first.js.Stream(tctx(t, 10*time.Second), name)
			if err != nil {
				t.Fatal(err)
			}

			if got := st.CachedInfo().Config.Replicas; got != 3 {
				t.Fatalf("%s has %d replicas %s, want 3", name, got, when)
			}
		}
	}

	if err := Provision(tctx(t, 30*time.Second), late, lateCfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	checkReplicas("after the late node provisioned through unanswered reads")

	for name, left := range flaky.readMisses {
		if left != 0 {
			t.Fatalf("%s: %d unanswered reads left, so Provision did not read it again", name, left)
		}
	}

	refused := errors.New("stream info refused")

	flaky.mu.Lock()
	flaky.readErr = refused
	flaky.mu.Unlock()

	if err := Provision(tctx(t, 30*time.Second), late, lateCfg, time.Hour); !errors.Is(err, refused) {
		t.Fatalf("Provision with streams it cannot read: want the read's error, got %v", err)
	}

	checkReplicas("after a provision that could not read them")
}
