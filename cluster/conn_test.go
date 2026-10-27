package cluster_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"

	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
)

func ctxT(t *testing.T, d time.Duration) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)

	return ctx
}

func externalConfig(urls []string) cluster.Config {
	cfg := cluster.DefaultConfig()
	cfg.Enabled = true
	cfg.NodeID = "n1"
	cfg.Cell = "c1"
	cfg.Mode = cluster.ModeExternal
	cfg.URLs = urls

	return cfg
}

func embeddedConfig(t *testing.T, node string) cluster.Config {
	t.Helper()

	cfg := cluster.DefaultConfig()
	cfg.Enabled = true
	cfg.NodeID = node
	cfg.Cell = "c1"
	cfg.Embedded.StoreDir = t.TempDir()

	return cfg
}

func TestConnectEmbeddedSingle(t *testing.T) {
	c, err := cluster.Connect(ctxT(t, 10*time.Second), embeddedConfig(t, "n1"), nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	if err := c.Healthy(ctxT(t, 5*time.Second)); err != nil {
		t.Fatalf("Healthy: %v", err)
	}

	if got := c.Replicas(); got != 1 {
		t.Fatalf("Replicas = %d, want 1", got)
	}

	if err := cluster.Provision(ctxT(t, 10*time.Second), c, embeddedConfig(t, "n1"), time.Hour); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	c.Close()
	c.Close() // idempotent

	if err := c.Healthy(ctxT(t, time.Second)); !errors.Is(err, cluster.ErrUnavailable) {
		t.Fatalf("Healthy after Close = %v, want ErrUnavailable", err)
	}
}

func TestConnectEmbeddedThree(t *testing.T) {
	const n = 3

	ports := clustertest.FreePorts(t, n)
	cfgs := make([]cluster.Config, n)

	for i := range n {
		cfg := embeddedConfig(t, fmt.Sprintf("n%d", i))
		cfg.Embedded.ClusterName = "embedded3"
		cfg.Embedded.ClusterListen = fmt.Sprintf("127.0.0.1:%d", ports[i])

		for j := range n {
			if j != i {
				cfg.Embedded.Routes = append(cfg.Embedded.Routes, clustertest.RouteURL(ports[j]))
			}
		}

		cfgs[i] = cfg
	}

	var (
		wg    sync.WaitGroup
		conns = make([]*cluster.Conn, n)
		errs  = make([]error, n)
	)

	start := time.Now()
	ctx := ctxT(t, 10*time.Second)

	for i := range n {
		wg.Add(1)

		go func() {
			defer wg.Done()

			conns[i], errs[i] = cluster.Connect(ctx, cfgs[i], nil)
		}()
	}

	wg.Wait()

	for i, c := range conns {
		if errs[i] != nil {
			t.Fatalf("node %d: %v", i, errs[i])
		}

		t.Cleanup(c.Close)
	}

	t.Logf("three embedded nodes had JetStream after %s", time.Since(start))

	for i, c := range conns {
		if err := c.Healthy(ctxT(t, 5*time.Second)); err != nil {
			t.Fatalf("node %d Healthy: %v", i, err)
		}

		if got := c.Replicas(); got != 3 {
			t.Fatalf("node %d Replicas = %d, want 3", i, got)
		}
	}

	if err := cluster.Provision(ctxT(t, 15*time.Second), conns[0], cfgs[0], time.Hour); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	// Another node sees what the first one provisioned.
	if _, err := conns[2].JS().Stream(ctxT(t, 5*time.Second), cfgs[0].LogStream()); err != nil {
		t.Fatalf("stream from node 2: %v", err)
	}
}

func TestProvisionIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		servers  int
		replicas int
	}{
		{"single", 1, 1},
		{"three", 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := externalConfig(clustertest.Start(t, tc.servers))

			c, err := cluster.Connect(ctxT(t, 15*time.Second), cfg, nil)
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(c.Close)

			if got := c.Replicas(); got != tc.replicas {
				t.Fatalf("Replicas = %d, want %d", got, tc.replicas)
			}

			const expiry = 36 * time.Hour

			ctx := ctxT(t, 30*time.Second)

			for i := range 2 {
				if perr := cluster.Provision(ctx, c, cfg, expiry); perr != nil {
					t.Fatalf("Provision #%d: %v", i+1, perr)
				}
			}

			js := c.JS()

			log, err := js.Stream(ctx, "TETHER_LOG_C1")
			if err != nil {
				t.Fatal(err)
			}

			lc := log.CachedInfo().Config
			if lc.Replicas != tc.replicas || lc.MaxAge != expiry || lc.Duplicates != 2*time.Minute ||
				lc.Storage != jetstream.FileStorage || len(lc.Subjects) != 1 || lc.Subjects[0] != "d.>" {
				t.Fatalf("log stream config = %+v", lc)
			}

			ctl, err := js.Stream(ctx, "TETHER_CTL_C1")
			if err != nil {
				t.Fatal(err)
			}

			cc := ctl.CachedInfo().Config
			if cc.Replicas != tc.replicas || cc.MaxAge != time.Hour || len(cc.Subjects) != 1 || cc.Subjects[0] != "ctl.>" {
				t.Fatalf("control stream config = %+v", cc)
			}

			for _, b := range []struct {
				name    string
				markers bool
			}{
				{cfg.SessionsBucket(), true},
				{cfg.RetainedBucket(), true},
				{cfg.MembersBucket(), false},
			} {
				kv, kerr := js.KeyValue(ctx, b.name)
				if kerr != nil {
					t.Fatalf("%s: %v", b.name, kerr)
				}

				st, kerr := kv.Status(ctx)
				if kerr != nil {
					t.Fatal(kerr)
				}

				sc := st.(*jetstream.KeyValueBucketStatus).StreamInfo().Config
				if sc.Replicas != tc.replicas || st.History() != 1 {
					t.Errorf("%s: replicas %d history %d", b.name, sc.Replicas, st.History())
				}

				if b.markers && (!sc.AllowMsgTTL || sc.SubjectDeleteMarkerTTL != time.Minute) {
					t.Errorf("%s: limit markers off (AllowMsgTTL %v, marker TTL %v)", b.name, sc.AllowMsgTTL, sc.SubjectDeleteMarkerTTL)
				}

				if !b.markers && sc.AllowMsgTTL {
					t.Errorf("%s: unexpected AllowMsgTTL", b.name)
				}
			}

			// A second call with a new expiry updates the stream in place.
			if perr := cluster.Provision(ctx, c, cfg, 0); perr != nil {
				t.Fatalf("Provision with no expiry: %v", perr)
			}

			info, err := log.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if got := info.Config.MaxAge; got != 0 {
				t.Fatalf("MaxAge after update = %s, want 0", got)
			}
		})
	}
}

func TestConnectNoEcho(t *testing.T) {
	cfg := externalConfig(clustertest.Start(t, 1))

	c, err := cluster.Connect(ctxT(t, 10*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	got := make(chan *nats.Msg, 1)

	sub, err := c.NC().ChanSubscribe("echo.test", got)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	// A second connection proves the subscription is live.
	other, err := nats.Connect(cfg.URLs[0])
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(other.Close)

	if err := c.NC().Flush(); err != nil {
		t.Fatal(err)
	}

	if err := c.NC().Publish("echo.test", []byte("own")); err != nil {
		t.Fatal(err)
	}

	if err := other.Publish("echo.test", []byte("peer")); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if string(m.Data) != "peer" {
			t.Fatalf("first message = %q, want the peer's (own publishes must not echo)", m.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message from the peer")
	}

	select {
	case m := <-got:
		t.Fatalf("unexpected extra message %q", m.Data)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestConnectNoDiscovery checks that NoDiscovery keeps a node on the servers
// it was given: it never learns the cluster's other URLs, so when its server
// goes away it waits for that server instead of reconnecting to another one.
// A node behind the fault proxy relies on this. A node without the flag is the
// control: it learns the other servers and fails over to one of them.
func TestConnectNoDiscovery(t *testing.T) {
	nc := clustertest.StartCluster(t, 3)
	urls := nc.URLs()

	pinnedCfg := externalConfig(urls[:1])
	pinnedCfg.NoDiscovery = true
	pinnedCfg.Replicas = 3

	pinned, err := cluster.Connect(ctxT(t, 20*time.Second), pinnedCfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(pinned.Close)

	freeCfg := externalConfig(urls[:1])
	freeCfg.NodeID = "n2"
	freeCfg.Replicas = 3

	free, err := cluster.Connect(ctxT(t, 20*time.Second), freeCfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(free.Close)

	// The control learns the other two servers from the cluster's INFO.
	deadline := time.Now().Add(10 * time.Second)
	for len(free.NC().DiscoveredServers()) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("control discovered %v, want two servers", free.NC().DiscoveredServers())
		}

		time.Sleep(20 * time.Millisecond)
	}

	if got := pinned.NC().Servers(); len(got) != 1 || len(pinned.NC().DiscoveredServers()) != 0 {
		t.Fatalf("pinned connection knows %v (discovered %v), want only %s", got, pinned.NC().DiscoveredServers(), urls[0])
	}

	nc.Stop(0)

	deadline = time.Now().Add(10 * time.Second)
	for !free.NC().IsConnected() || free.NC().ConnectedUrl() == urls[0] {
		if time.Now().After(deadline) {
			t.Fatal("control did not fail over to another server")
		}

		time.Sleep(20 * time.Millisecond)
	}

	// The pinned node has had the same time and more: it must still be
	// waiting for its own server.
	time.Sleep(time.Second)

	if pinned.NC().IsConnected() {
		t.Fatalf("pinned connection reconnected to %s, want it to wait for %s", pinned.NC().ConnectedUrl(), urls[0])
	}
}

func TestNoDiscoveryConfigKey(t *testing.T) {
	var cfg cluster.Config
	if err := json.Unmarshal([]byte(`{"no_discovery": true}`), &cfg); err != nil {
		t.Fatal(err)
	}

	if !cfg.NoDiscovery {
		t.Fatal("json key no_discovery did not set NoDiscovery")
	}

	if err := yaml.Unmarshal([]byte("no_discovery: true\n"), &cfg); err != nil || !cfg.NoDiscovery {
		t.Fatalf("yaml key no_discovery: %v, NoDiscovery = %v", err, cfg.NoDiscovery)
	}
}

func TestConnectUnavailable(t *testing.T) {
	ports := clustertest.FreePorts(t, 1)
	cfg := externalConfig([]string{clustertest.RouteURL(ports[0])})

	_, err := cluster.Connect(ctxT(t, 2*time.Second), cfg, nil)
	if !errors.Is(err, cluster.ErrUnavailable) {
		t.Fatalf("Connect to nothing = %v, want ErrUnavailable", err)
	}
}

// Every node of a fresh cell provisions at once. The meta leader leaves
// concurrent creates of one stream unanswered, so Provision bounds each
// attempt and sends it again.
func TestProvisionConcurrentFresh(t *testing.T) {
	const n = 2

	ports := clustertest.FreePorts(t, n)
	conns := make([]*cluster.Conn, n)
	cfgs := make([]cluster.Config, n)
	errs := make([]error, n)

	for i := range n {
		cfgs[i] = embeddedConfig(t, fmt.Sprintf("n%d", i))
		cfgs[i].Embedded.ClusterName = "fresh"
		cfgs[i].Embedded.ClusterListen = fmt.Sprintf("127.0.0.1:%d", ports[i])
		cfgs[i].Embedded.Routes = []string{clustertest.RouteURL(ports[1-i])}
	}

	ctx := ctxT(t, 40*time.Second)

	var wg sync.WaitGroup

	for i := range n {
		wg.Go(func() {
			conns[i], errs[i] = cluster.Connect(ctx, cfgs[i], nil)
			if errs[i] == nil {
				errs[i] = cluster.Provision(ctx, conns[i], cfgs[i], time.Hour)
			}
		})
	}

	wg.Wait()

	for i := range n {
		if conns[i] != nil {
			t.Cleanup(conns[i].Close)
		}

		if errs[i] != nil {
			t.Fatalf("node %d: %v", i, errs[i])
		}
	}
}

// An embedded server with a user and password takes no client without them,
// and the node's own connection uses them.
func TestEmbeddedCredentials(t *testing.T) {
	port := clustertest.FreePorts(t, 1)[0]

	cfg := embeddedConfig(t, "n1")
	cfg.Embedded.Listen = fmt.Sprintf("127.0.0.1:%d", port)
	cfg.Embedded.User, cfg.Embedded.Password = "tether", "s3cret"

	c, err := cluster.Connect(ctxT(t, 10*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	if perr := cluster.Provision(ctxT(t, 10*time.Second), c, cfg, time.Hour); perr != nil {
		t.Fatalf("Provision over the authenticated connection: %v", perr)
	}

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)

	for _, opt := range []nats.Option{nats.Name("anonymous"), nats.UserInfo("tether", "wrong")} {
		if nc, cerr := nats.Connect(url, opt, nats.MaxReconnects(0)); cerr == nil {
			nc.Close()
			t.Fatal("a client without the right credentials connected")
		}
	}

	nc, err := nats.Connect(url, nats.UserInfo("tether", "s3cret"))
	if err != nil {
		t.Fatalf("the right credentials: %v", err)
	}

	nc.Close()
}

func TestConnectNeedsEnabled(t *testing.T) {
	if _, err := cluster.Connect(ctxT(t, time.Second), cluster.DefaultConfig(), nil); err == nil {
		t.Fatal("Connect on a disabled config succeeded")
	}
}

func TestNames(t *testing.T) {
	cfg := cluster.Config{Cell: "Ab9"}

	for got, want := range map[string]string{
		cfg.LogStream():      "TETHER_LOG_AB9",
		cfg.ControlStream():  "TETHER_CTL_AB9",
		cfg.SessionsBucket(): "tether-sessions-ab9",
		cfg.RetainedBucket(): "tether-retained-ab9",
		cfg.MembersBucket():  "tether-members-ab9",
	} {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := cluster.DefaultConfig()
	if cfg.Enabled || cfg.Mode != cluster.ModeEmbedded || cfg.Heartbeat != 2*time.Second || cfg.Lease != 24*time.Second {
		t.Fatalf("DefaultConfig = %+v", cfg)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("a disabled config must validate: %v", err)
	}
}

func TestValidate(t *testing.T) {
	good := func(mutate func(*cluster.Config)) cluster.Config {
		cfg := cluster.DefaultConfig()
		cfg.Enabled = true
		cfg.NodeID = "node-1_a"
		cfg.Cell = "cell01"
		cfg.Embedded.StoreDir = "/tmp/x"

		if mutate != nil {
			mutate(&cfg)
		}

		return cfg
	}

	external := func(c *cluster.Config) {
		c.Mode = cluster.ModeExternal
		c.URLs = []string{"nats://127.0.0.1:4222"}
	}

	tests := []struct {
		name    string
		cfg     cluster.Config
		wantErr string // substring; empty means valid
	}{
		{"valid embedded", good(nil), ""},
		{"valid external", good(external), ""},
		{"disabled ignores the rest", cluster.Config{}, ""},
		{"valid embedded cluster", good(func(c *cluster.Config) {
			c.Embedded.ClusterName = "x"
			c.Embedded.ClusterListen = "0.0.0.0:6222"
			c.Embedded.Routes = []string{"nats://a:6222", "nats://b:6222"}
			c.Embedded.User, c.Embedded.Password = "u", "p"
		}), ""},
		// Ruling R43: no listener past loopback without credentials or
		// an explicit insecure.
		{"open route listen", good(func(c *cluster.Config) {
			c.Embedded.ClusterName = "x"
			c.Embedded.ClusterListen = "0.0.0.0:6222"
		}), "loopback"},
		{"route listen on every interface", good(func(c *cluster.Config) {
			c.Embedded.ClusterName = "x"
			c.Embedded.ClusterListen = ":6222"
		}), "loopback"},
		{"open client listen", good(func(c *cluster.Config) { c.Embedded.Listen = "10.0.0.5:4222" }), "loopback"},
		{"open listen, insecure", good(func(c *cluster.Config) {
			c.Embedded.Listen = ":4222"
			c.Embedded.ClusterName = "x"
			c.Embedded.ClusterListen = "0.0.0.0:6222"
			c.Embedded.Insecure = true
		}), ""},
		{"loopback listens", good(func(c *cluster.Config) {
			c.Embedded.Listen = "localhost:4222"
			c.Embedded.ClusterName = "x"
			c.Embedded.ClusterListen = "[::1]:6222"
		}), ""},
		{"user without password", good(func(c *cluster.Config) { c.Embedded.User = "u" }), "password"},
		{"password without user", good(func(c *cluster.Config) { c.Embedded.Password = "p" }), "user"},
		{"negative log age", good(func(c *cluster.Config) { c.LogMaxAge = -time.Second }), "log_max_age"},
		{"no node id", good(func(c *cluster.Config) { c.NodeID = "" }), "node_id"},
		{"node id with dot", good(func(c *cluster.Config) { c.NodeID = "a.b" }), "node_id"},
		{"node id with space", good(func(c *cluster.Config) { c.NodeID = "a b" }), "node_id"},
		{"node id with wildcard", good(func(c *cluster.Config) { c.NodeID = "a*" }), "node_id"},
		{"node id too long", good(func(c *cluster.Config) { c.NodeID = strings.Repeat("a", 65) }), "node_id"},
		{"node id at the limit", good(func(c *cluster.Config) { c.NodeID = strings.Repeat("a", 64) }), ""},
		{"no cell", good(func(c *cluster.Config) { c.Cell = "" }), "cell"},
		{"cell uppercase", good(func(c *cluster.Config) { c.Cell = "Cell" }), "cell"},
		{"cell with dash", good(func(c *cluster.Config) { c.Cell = "a-b" }), "cell"},
		{"cell too long", good(func(c *cluster.Config) { c.Cell = strings.Repeat("a", 17) }), "cell"},
		{"cell at the limit", good(func(c *cluster.Config) { c.Cell = strings.Repeat("a", 16) }), ""},
		{"bad mode", good(func(c *cluster.Config) { c.Mode = "hybrid" }), "mode"},
		{"empty mode", good(func(c *cluster.Config) { c.Mode = "" }), "mode"},
		{"external without urls", good(func(c *cluster.Config) { c.Mode = cluster.ModeExternal }), "url"},
		{"external with blank url", good(func(c *cluster.Config) {
			c.Mode = cluster.ModeExternal
			c.URLs = []string{" "}
		}), "url"},
		{"embedded without store dir", good(func(c *cluster.Config) { c.Embedded.StoreDir = "" }), "store_dir"},
		{"routes without cluster name", good(func(c *cluster.Config) {
			c.Embedded.ClusterListen = "0.0.0.0:6222"
			c.Embedded.Routes = []string{"nats://a:6222"}
		}), "cluster_name"},
		{"cluster name without listen", good(func(c *cluster.Config) { c.Embedded.ClusterName = "x" }), "cluster_listen"},
		{"lease equals twice heartbeat", good(func(c *cluster.Config) {
			c.Heartbeat = 5 * time.Second
			c.Lease = 10 * time.Second
		}), "lease"},
		{"lease just above", good(func(c *cluster.Config) {
			c.Heartbeat = 5 * time.Second
			c.Lease = 10*time.Second + time.Millisecond
		}), ""},
		{"zero heartbeat", good(func(c *cluster.Config) { c.Heartbeat = 0 }), "heartbeat"},
		{"negative replicas", good(func(c *cluster.Config) { c.Replicas = -1 }), "replicas"},
		{"too many replicas", good(func(c *cluster.Config) { c.Replicas = 4 }), "replicas"},
		{"pinned replicas", good(func(c *cluster.Config) { c.Replicas = 3 }), ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()

			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("want an error mentioning %q, got nil", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}
