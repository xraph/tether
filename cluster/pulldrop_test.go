package cluster

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/tether/cluster/clustertest"
)

// pullPrefix starts the subject of a JetStream pull request.
const pullPrefix = "$JS.API.CONSUMER.MSG.NEXT."

// pullDropProxy sits between a NATS client and its server and drops the
// client's next drop pull requests, the way a pull is lost when it reaches
// the client's server before the consumer's interest does: the server
// answers it 503 or sends it nowhere, nats.go keeps counting it as
// outstanding, and nothing pulls again until two idle heartbeats are missed.
// Everything else passes through as is.
type pullDropProxy struct {
	ln      net.Listener
	target  string
	drop    atomic.Int32
	dropped atomic.Int32
}

func newPullDropProxy(t *testing.T, target string) *pullDropProxy {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	p := &pullDropProxy{ln: ln, target: target}
	t.Cleanup(func() { _ = ln.Close() })

	go p.accept(t)

	return p
}

func (p *pullDropProxy) url() string { return "nats://" + p.ln.Addr().String() }

func (p *pullDropProxy) accept(t *testing.T) {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}

		server, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", p.target)
		if err != nil {
			_ = client.Close()

			continue
		}

		go func() {
			_, _ = io.Copy(client, server)
			_ = client.Close()
		}()

		go func() {
			p.filter(client, server)
			_ = server.Close()
		}()
	}
}

// filter copies the client's protocol to the server one operation at a time,
// leaving out the pull requests it is told to drop.
func (p *pullDropProxy) filter(client io.Reader, server io.Writer) {
	r := bufio.NewReader(client)

	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}

		var payload []byte

		f := bytes.Fields(line)
		if len(f) >= 3 && (strings.EqualFold(string(f[0]), "PUB") || strings.EqualFold(string(f[0]), "HPUB")) {
			n, perr := strconv.Atoi(string(f[len(f)-1]))
			if perr != nil {
				return
			}

			payload = make([]byte, n+2)
			if _, err := io.ReadFull(r, payload); err != nil {
				return
			}

			if strings.HasPrefix(string(f[1]), pullPrefix) && p.drop.Load() > 0 {
				p.drop.Add(-1)
				p.dropped.Add(1)

				continue
			}
		}

		if _, err := server.Write(append(line, payload...)); err != nil {
			return
		}
	}
}

// A replay or a reader whose first pull request is lost gets its messages
// once the consumer misses two idle heartbeats and starts over. With nats.go's
// default heartbeat for an ordered consumer, 5 s, that was 10 s of silence
// with nothing wrong, which is what CI kept running into: the pull reached
// the client's server before the new consumer's interest had, a window that
// is milliseconds on a quiet machine and seconds on a loaded runner. The
// Log's pull heartbeat (pullHeartbeat, 1 s) makes it about 2 s.
func TestLostPullRecoveredPromptly(t *testing.T) {
	url := clustertest.Start(t, 1)[0]
	proxy := newPullDropProxy(t, strings.TrimPrefix(url, "nats://"))

	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.NodeID = "n1"
	cfg.Cell = "c1"
	cfg.Mode = ModeExternal
	cfg.URLs = []string{proxy.url()}
	cfg.NoDiscovery = true
	cfg.Replicas = 1

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	if err := Provision(tctx(t, 30*time.Second), c, cfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	l := NewLog(c, cfg)

	var last uint64
	for range 3 {
		last = mustAppend(t, l, LogMessage{Tenant: testTenant, Topic: "t"})
	}

	// Two missed heartbeats and a new consumer: a lost pull costs about two
	// heartbeats, so this leaves room for a loaded machine while failing at
	// nats.go's default.
	const bound = 5 * time.Second

	t.Run("replay", func(t *testing.T) {
		proxy.drop.Store(1)

		before := proxy.dropped.Load()
		began := time.Now()
		col := newCollector()

		if err := l.Replay(tctx(t, 30*time.Second), testTenant, 1, last, col.fn); err != nil {
			t.Fatal(err)
		}

		took := time.Since(began)

		if got := len(col.snapshot()); got != 3 {
			t.Fatalf("replayed %d messages, want 3", got)
		}

		if proxy.dropped.Load() == before {
			t.Fatal("no pull request was dropped, so this tested nothing")
		}

		if took > bound {
			t.Fatalf("the replay waited %s after its first pull was lost", took.Round(time.Millisecond))
		}
	})

	t.Run("reader", func(t *testing.T) {
		proxy.drop.Store(1)

		before := proxy.dropped.Load()
		col := newCollector()

		r, err := l.Reader(testTenant, 1, col.fn)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(r.Stop)

		began := time.Now()

		for len(col.snapshot()) < 3 {
			if time.Since(began) > 30*time.Second {
				t.Fatal("the reader never delivered")
			}

			time.Sleep(10 * time.Millisecond)
		}

		took := time.Since(began)

		if proxy.dropped.Load() == before {
			t.Fatal("no pull request was dropped, so this tested nothing")
		}

		if took > bound {
			t.Fatalf("the reader waited %s after its first pull was lost", took.Round(time.Millisecond))
		}
	})
}
