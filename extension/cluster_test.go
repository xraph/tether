package extension_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	"github.com/xraph/keysmith"
	keysmithext "github.com/xraph/keysmith/extension"
	kmemory "github.com/xraph/keysmith/store/memory"
	"github.com/xraph/vessel"
	"github.com/xraph/warden"
	wardenext "github.com/xraph/warden/extension"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/extension"
	"github.com/xraph/tether/internal/chaos"
	"github.com/xraph/tether/store/memory"
)

// cellWait bounds a node joining its cell: an embedded cell has JetStream
// only once its peers are up, and the Mac the tests run on is often busy.
const cellWait = 30 * time.Second

// externalCell is the cluster config of node n1 of cell "ext" on the given
// NATS URLs, with one replica.
func externalCell(urls []string) cluster.Config {
	ccfg := cluster.DefaultConfig()
	ccfg.NodeID = "n1"
	ccfg.Cell = "ext"
	ccfg.Mode = cluster.ModeExternal
	ccfg.URLs = urls
	ccfg.Replicas = 1

	return ccfg
}

// startCellNode boots the whole stack as the one node of a cell on a
// one-server NATS, through WithCluster.
func startCellNode(t *testing.T) *env {
	t.Helper()

	return startApp(t, newApp(t), false, extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()),
		extension.WithCluster(externalCell(clustertest.Start(t, 1))))
}

// In a cell, Start puts the control bus between the plugins and the broker:
// a revoke and a tenant disable made through REST still disconnect the
// device, now by way of the bus.
func TestClusterControlWiring(t *testing.T) {
	e := startCellNode(t)

	tid := e.tenant()
	e.role(tid, "sensor", []string{"up"}, nil)
	d1, d2 := e.device(tid, "sensor"), e.device(tid, "sensor")

	c1, err := e.connect(d1, "dev-1")
	require.NoError(t, err)

	e.do(http.MethodPost, e.base+"/v1/tenants/"+tid+"/devices/"+d1.id+"/revoke", map[string]any{"reason": "lost"}, http.StatusNoContent)

	select {
	case <-c1.lost:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "the revoked device is still connected")
	}

	_, err = e.connect(d1, "dev-1")
	require.Error(t, err, "a revoked key cannot connect again")

	c2, err := e.connect(d2, "dev-2")
	require.NoError(t, err)

	e.do(http.MethodPatch, e.base+"/v1/tenants/"+tid, map[string]any{"enabled": false}, http.StatusOK)

	select {
	case <-c2.lost:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "the disabled tenant's device is still connected")
	}

	_, err = e.connect(d2, "dev-2")
	require.Error(t, err, "a disabled tenant's device cannot connect again")
}

// cellStores are the stores two apps of one cell share, as the nodes of a
// real cell share one database: Tether's, keysmith's and warden's.
type cellStores struct {
	tether   *memory.Store
	keysmith *kmemory.Store
	warden   *wmemory.Store
}

func newCellStores(t *testing.T) cellStores {
	t.Helper()

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	return cellStores{tether: st, keysmith: kmemory.New(), warden: wmemory.New()}
}

// cellApp is one app of a cell, registered and not yet started.
type cellApp struct {
	app forge.App
	ext *extension.Extension
}

// newCellApp registers keysmith, warden and Tether on a quiet app over the
// shared stores, as one node of the cell ccfg names.
func newCellApp(t *testing.T, s cellStores, ccfg cluster.Config) cellApp {
	t.Helper()

	app := forge.New(
		forge.WithAppName("tether-"+ccfg.NodeID),
		forge.WithAppLogger(forge.NewNoopLogger()),
		forge.WithEnableConfigAutoDiscovery(false),
	)

	ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()),
		extension.WithStore(s.tether), extension.WithCluster(ccfg))

	for _, x := range []forge.Extension{
		ext.Keysmith(keysmithext.WithDisableRoutes(), keysmithext.WithEngineOptions(keysmith.WithStore(s.keysmith))),
		ext.Warden(wardenext.WithDisableRoutes(), wardenext.WithStore(s.warden)),
		ext,
	} {
		require.NoError(t, app.RegisterExtension(x))
	}

	return cellApp{app: app, ext: ext}
}

// startCell starts the apps together (an embedded cell has JetStream only
// once its peers are up), stops them at cleanup, and returns an env for
// each.
func startCell(t *testing.T, apps ...cellApp) []*env {
	t.Helper()

	errs := make([]error, len(apps))

	var wg sync.WaitGroup

	for i, a := range apps {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), cellWait)
			defer cancel()

			errs[i] = a.app.Start(ctx)
		})
	}

	wg.Wait()

	t.Cleanup(func() {
		var stop sync.WaitGroup

		for _, a := range apps {
			stop.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), cellWait)
				defer cancel()

				_ = a.app.Stop(ctx)
			})
		}

		stop.Wait()
	})

	envs := make([]*env, len(apps))

	for i, a := range apps {
		require.NoError(t, errs[i], "node %d", i)

		wd, err := vessel.Inject[*warden.Engine](a.app.Container())
		require.NoError(t, err)

		srv := httptest.NewServer(a.app.Router())
		t.Cleanup(srv.Close)

		envs[i] = &env{t: t, base: "/tether", app: a.app, ext: a.ext, srv: srv, wd: wd}
	}

	return envs
}

// embeddedPair is the cluster config of two embedded nodes routed to each
// other on loopback, with route and client credentials.
func embeddedPair(t *testing.T) [2]cluster.Config {
	t.Helper()

	ports := clustertest.FreePorts(t, 2)

	var cfgs [2]cluster.Config

	for i := range cfgs {
		ccfg := cluster.DefaultConfig()
		ccfg.NodeID = fmt.Sprintf("n%d", i+1)
		ccfg.Cell = "pair"
		ccfg.Embedded.StoreDir = t.TempDir()
		ccfg.Embedded.ClusterName = "tetherpair"
		ccfg.Embedded.ClusterListen = "127.0.0.1:" + strconv.Itoa(ports[i])
		ccfg.Embedded.Routes = []string{clustertest.RouteURL(ports[1-i])}
		ccfg.Embedded.User = "tether"
		ccfg.Embedded.Password = "route-secret"
		cfgs[i] = ccfg
	}

	return cfgs
}

// Two Forge apps in one process, each with an embedded NATS server routed
// to the other's, form one cell. A device provisioned through A's REST API
// connects to B and gets its own QoS 1 message back, and a QoS 1 message a
// device on A publishes reaches it on B.
func TestTwoExtensionsFormACell(t *testing.T) {
	s := newCellStores(t)
	cfgs := embeddedPair(t)
	envs := startCell(t, newCellApp(t, s, cfgs[0]), newCellApp(t, s, cfgs[1]))
	a, b := envs[0], envs[1]

	tid := a.tenant()
	a.role(tid, "sensor", []string{"t/%c"}, []string{"t/#"})
	da, db := a.device(tid, "sensor"), a.device(tid, "sensor")

	onB, err := b.connect(db, "dev-b")
	require.NoError(t, err)

	onB.subscribe(t, "t/#")
	onB.publish(t, "t/dev-b", "round trip")
	assert.Equal(t, [2]string{"t/dev-b", "round trip"}, onB.next(t))

	onA, err := a.connect(da, "dev-a")
	require.NoError(t, err)

	onA.publish(t, "t/dev-a", "across the cell")
	assert.Equal(t, [2]string{"t/dev-a", "across the cell"}, onB.next(t))

	for _, e := range envs {
		require.NoError(t, e.ext.Health(t.Context()))
		assert.Equal(t, "0", metricValue(t, e.app, extension.MetricClusterFenced))
	}

	assert.Equal(t, "1", metricValue(t, a.app, extension.MetricLogAppendSeconds+"_count"), "A's publish went through the log")
	assert.Equal(t, "1", metricValue(t, b.app, extension.MetricLogAppendSeconds+"_count"), "B's publish went through the log")
	assert.Equal(t, "0", metricValue(t, b.app, extension.MetricCASLost))
	assert.Equal(t, "0", metricValue(t, b.app, extension.MetricReplayMessages))
}

// The embedded server takes no client without the cell's credentials: a
// NATS client that knows only the address is refused.
func TestEmbeddedCellRefusesStrangers(t *testing.T) {
	s := newCellStores(t)
	cfgs := embeddedPair(t)

	port := clustertest.FreePorts(t, 1)[0]
	cfgs[0].Embedded.Listen = "127.0.0.1:" + strconv.Itoa(port)

	startCell(t, newCellApp(t, s, cfgs[0]), newCellApp(t, s, cfgs[1]))

	_, err := natsDial(t, port, "", "")
	require.Error(t, err, "no credentials")

	_, err = natsDial(t, port, "tether", "wrong")
	require.Error(t, err, "a wrong password")

	nc, err := natsDial(t, port, "tether", "route-secret")
	require.NoError(t, err, "the cell's credentials")
	nc.Close()
}

// Ruling R43: an embedded server listening beyond loopback with no
// credentials would let anyone who reaches the port read every tenant's
// traffic or join the cell, so Start refuses it unless insecure is set.
func TestClusterRefusesOpenListen(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*cluster.Config)
	}{
		{"client listen on every interface", func(c *cluster.Config) { c.Embedded.Listen = ":4222" }},
		{"route listen on every interface", func(c *cluster.Config) {
			c.Embedded.ClusterName = "x"
			c.Embedded.ClusterListen = "0.0.0.0:6222"
		}},
		{"route listen on a public address", func(c *cluster.Config) {
			c.Embedded.ClusterName = "x"
			c.Embedded.ClusterListen = "192.0.2.10:6222"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ccfg := cluster.DefaultConfig()
			ccfg.NodeID = "n1"
			ccfg.Cell = "open"
			ccfg.Embedded.StoreDir = t.TempDir()
			tc.mutate(&ccfg)

			ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()),
				extension.WithCluster(ccfg))

			err := boot(t, newApp(t), ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()), ext)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "loopback")
			assert.Nil(t, ext.Broker(), "nothing started")
		})
	}
}

// A node cut off from NATS fences itself: Health reports cluster.ErrFenced
// and the fenced gauge reads 1, until the link heals.
func TestClusterHealthReportsFenced(t *testing.T) {
	urls := clustertest.Start(t, 1)

	p, err := chaos.NewProxy(strings.TrimPrefix(urls[0], "nats://"))
	require.NoError(t, err)
	t.Cleanup(p.Close)

	ccfg := externalCell([]string{"nats://" + p.Addr()})
	ccfg.NoDiscovery = true
	ccfg.Heartbeat = 100 * time.Millisecond
	ccfg.Lease = time.Second
	ccfg.LoadInterval = 300 * time.Millisecond // Validate: at most Lease/3.

	e := startApp(t, newApp(t), false, extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()),
		extension.WithCluster(ccfg))

	// Registered after the app's stop, so it runs first: the node stops
	// over a working link.
	t.Cleanup(func() { p.SetMode(chaos.Both, chaos.Pass) })

	require.NoError(t, e.ext.Health(t.Context()))
	assert.Equal(t, "0", metricValue(t, e.app, extension.MetricClusterFenced))

	p.SetMode(chaos.Both, chaos.Blackhole)

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()

		return errors.Is(e.ext.Health(ctx), cluster.ErrFenced)
	}, 10*time.Second, 20*time.Millisecond, "Health reports the fence")

	require.Eventually(t, func() bool {
		return metricValue(t, e.app, extension.MetricClusterFenced) == "1"
	}, 5*time.Second, 20*time.Millisecond, "the fenced gauge")

	p.SetMode(chaos.Both, chaos.Pass)

	require.Eventually(t, func() bool {
		return e.ext.Health(t.Context()) == nil
	}, 15*time.Second, 50*time.Millisecond, "healthy again once the link heals")

	assert.Equal(t, "0", metricValue(t, e.app, extension.MetricClusterFenced))
}

// natsDial connects a plain NATS client to the embedded server's client port.
func natsDial(t *testing.T, port int, user, pass string) (*nats.Conn, error) {
	t.Helper()

	opts := []nats.Option{nats.Timeout(2 * time.Second), nats.MaxReconnects(0)}
	if user != "" {
		opts = append(opts, nats.UserInfo(user, pass))
	}

	return nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", port), opts...)
}
