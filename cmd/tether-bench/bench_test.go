package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	"github.com/xraph/keysmith"
	keysmithext "github.com/xraph/keysmith/extension"
	kmemory "github.com/xraph/keysmith/store/memory"
	wardenext "github.com/xraph/warden/extension"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/extension"
	"github.com/xraph/tether/store/memory"
)

// cellWait bounds a node joining its cell: an embedded cell has JetStream only
// once its peers are up, and the Mac the tests run on is often busy.
const cellWait = 60 * time.Second

// pairNode is one running broker of the in-process cell.
type pairNode struct {
	app  forge.App
	ext  *extension.Extension
	http *httptest.Server
}

// startPair runs two Forge apps in one process, each with an embedded NATS
// server routed to the other's, as one cell sharing their stores. It returns
// them once both have started, and stops them at cleanup.
func startPair(t *testing.T) []*pairNode {
	t.Helper()

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	km, wd := kmemory.New(), wmemory.New()
	ports := clustertest.FreePorts(t, 2)
	nodes := make([]*pairNode, 2)

	for i := range nodes {
		ccfg := cluster.DefaultConfig()
		ccfg.NodeID = fmt.Sprintf("n%d", i+1)
		ccfg.Cell = "benchsmoke"
		ccfg.Embedded.StoreDir = filepath.Join(t.TempDir(), "nats")
		ccfg.Embedded.ClusterName = "benchsmoke"
		ccfg.Embedded.ClusterListen = "127.0.0.1:" + strconv.Itoa(ports[i])
		ccfg.Embedded.Routes = []string{clustertest.RouteURL(ports[1-i])}
		ccfg.Embedded.User = "tether"
		ccfg.Embedded.Password = "route-secret"

		bcfg := broker.DefaultConfig()
		bcfg.TCPAddress = "127.0.0.1:0"

		app := forge.New(
			forge.WithAppName("tether-"+ccfg.NodeID),
			forge.WithAppLogger(forge.NewNoopLogger()),
			forge.WithEnableConfigAutoDiscovery(false),
		)

		ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(bcfg),
			extension.WithStore(st), extension.WithCluster(ccfg))

		for _, x := range []forge.Extension{
			ext.Keysmith(keysmithext.WithDisableRoutes(), keysmithext.WithEngineOptions(keysmith.WithStore(km))),
			ext.Warden(wardenext.WithDisableRoutes(), wardenext.WithStore(wd)),
			ext,
		} {
			require.NoError(t, app.RegisterExtension(x))
		}

		nodes[i] = &pairNode{app: app, ext: ext}
	}

	// An embedded cell has JetStream only once its peers are up, so start them together.
	errs := make([]error, len(nodes))

	var wg sync.WaitGroup

	for i, n := range nodes {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), cellWait)
			defer cancel()

			errs[i] = n.app.Start(ctx)
		})
	}

	wg.Wait()

	t.Cleanup(func() {
		var stop sync.WaitGroup

		for _, n := range nodes {
			stop.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), cellWait)
				defer cancel()

				_ = n.app.Stop(ctx)
			})
		}

		stop.Wait()
	})

	for i, n := range nodes {
		require.NoError(t, errs[i], "node %d", i)

		n.http = httptest.NewServer(n.app.Router())
		t.Cleanup(n.http.Close)
	}

	return nodes
}

// A pubsub run against an in-process cell of two nodes: 50 clients for five
// seconds at QoS 1, each pair's publisher on one node and its subscriber on the
// other. The histogram has samples, nothing is lost, and the credentials come
// through a CSV file the way a real run's do.
func TestBenchSmoke(t *testing.T) {
	nodes := startPair(t)

	p, err := provision(t.Context(), newAdminClient(nodes[0].http.URL+"/tether/v1", ""), 4)
	require.NoError(t, err)
	require.Len(t, p.Creds, 4)

	csvPath := filepath.Join(t.TempDir(), "creds.csv")
	require.NoError(t, writeCreds(csvPath, p.Creds))

	creds, err := readCreds(csvPath)
	require.NoError(t, err)
	assert.Equal(t, p.Creds, creds)

	tg := &target{
		addrs:   []string{nodes[0].ext.Broker().Addr().String(), nodes[1].ext.Broker().Addr().String()},
		creds:   creds,
		prefix:  newPrefix(),
		timeout: 20 * time.Second,
		admins:  []string{nodes[0].http.URL + "/tether/v1", nodes[1].http.URL + "/tether/v1"},
		tenant:  p.Tenant,
	}

	o := pubsubOpts{
		QoS: 1, Clients: 50, Rate: 200, Duration: 5 * time.Second, Warmup: time.Second,
		Placement: "cross", Size: 64, ConnRate: 0, Parallel: 32,
	}

	res, err := o.measure(t.Context(), tg)
	require.NoError(t, err)

	require.Len(t, res.Phases, 1)
	assert.Positive(t, res.Overall.Count, "the histogram has samples")
	assert.Positive(t, res.Overall.P50)
	assert.GreaterOrEqual(t, res.Overall.P99, res.Overall.P50)
	assert.Equal(t, 50, res.Connected)
	assert.Positive(t, res.AckLatency.Count)
	assert.EqualValues(t, 0, res.Lost, "%+v", res)
	assert.Equal(t, res.Acked, res.Received)
	require.Len(t, res.NodeSide, 2)
	assert.Equal(t, 25, res.NodeSide[0].Connected, "each node holds half of the clients")
	assert.Equal(t, 25, res.NodeSide[1].Connected, "each node holds half of the clients")

	// The text report carries the same numbers.
	var out strings.Builder

	printPubsub(&out, res)
	assert.Contains(t, out.String(), "publish-to-deliver")

	// A small reconnect storm on the same cell: the group is dropped and comes
	// back, the measured pairs lose nothing, and the report has three phases.
	t.Run("storm", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the storm takes 15 s")
		}

		so := o
		so.QoS, so.Clients, so.Rate = 0, 10, 100
		so.Duration, so.Warmup = 11*time.Second, time.Second
		so.Storm, so.StormAt, so.StormWindow = 20, time.Second, time.Second

		sres, err := so.measure(t.Context(), tg)
		require.NoError(t, err)

		require.Len(t, sres.Phases, 3)
		require.NotNil(t, sres.Storm)
		assert.Equal(t, 20, sres.Storm.Reconnect.OK, "every dropped client came back: %+v", sres.Storm.Reconnect.Errors)
		assert.Zero(t, sres.LostConns)

		for _, ph := range sres.Phases {
			assert.Positive(t, ph.Delivered.Count, ph.Name)
		}
	})

	// conn opens clients at a rate and counts them per node.
	t.Run("conn", func(t *testing.T) {
		cs, st := tg.connectMany(t.Context(), "c", 20, 0, 8, nil, nil)
		t.Cleanup(func() { closeAll(cs, false) })

		assert.Equal(t, 20, st.OK, "%v", st.Errors)
		assert.Equal(t, []int{10, 10}, st.PerNode)
		assert.Positive(t, st.Latency.P50)
	})
}

// The percentiles are nearest-rank over the samples.
func TestSummarize(t *testing.T) {
	d := make([]time.Duration, 0, 100)
	for i := 100; i >= 1; i-- {
		d = append(d, time.Duration(i)*time.Millisecond)
	}

	s := summarize(d)
	assert.Equal(t, 100, s.Count)
	assert.InDelta(t, 50, s.P50, 0.001)
	assert.InDelta(t, 99, s.P99, 0.001)
	assert.InDelta(t, 100, s.Max, 0.001)
	assert.Equal(t, summary{}, summarize(nil))
}
