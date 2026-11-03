package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	"github.com/xraph/grove/drivers/pgdriver"
	kspostgres "github.com/xraph/keysmith/store/postgres"
	"github.com/xraph/vessel"
	wdpostgres "github.com/xraph/warden/store/postgres"

	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/extension"
	tpostgres "github.com/xraph/tether/store/postgres"
)

// cellWait bounds a node joining its cell: an embedded cell has JetStream
// only once its peers are up.
const cellWait = 30 * time.Second

// postgresConfig creates a throwaway database on the server
// TETHER_TEST_POSTGRES points at and returns a config on it, dropping the
// database when the test ends. The test skips when the variable is unset. The
// role needs CREATEDB.
//
//	TETHER_TEST_POSTGRES=postgres://postgres:tether@127.0.0.1:5432/postgres?sslmode=disable
func postgresConfig(t *testing.T) (config, string) {
	t.Helper()

	dsn := os.Getenv("TETHER_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("TETHER_TEST_POSTGRES is not set")
	}

	admin := pgdriver.New()
	require.NoError(t, admin.Open(t.Context(), dsn))

	suffix := make([]byte, 6)
	_, err := rand.Read(suffix)
	require.NoError(t, err)

	name := "tether_bin_" + hex.EncodeToString(suffix)
	_, err = admin.Exec(t.Context(), "CREATE DATABASE "+name)
	require.NoError(t, err)

	t.Cleanup(func() {
		// t.Context() is already cancelled here, so use a fresh one.
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close()
	})

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	u.Path = "/" + name

	cfg := testConfig(t)
	cfg.Server.Database = databaseConfig{Driver: driverPostgres, DSN: u.String()}

	return cfg, u.String()
}

// The binary's whole stack boots on PostgreSQL: a tenant and a device come
// from the admin API, the device round-trips a message, and a second process
// life on the same database still has the tenant and accepts the same key.
func TestBinaryOnPostgres(t *testing.T) {
	cfg, dsn := postgresConfig(t)
	require.NoError(t, applyDatabase(&cfg, ""))

	s := start(t, cfg, testToken, false)

	tid := s.tenant(t)
	s.role(t, tid, "sensor", []string{"sensors/%c/up"}, []string{"sensors/%c/#"})
	user, pass := s.device(t, tid, "pump", "sensor")

	msgs := make(chan [2]string, 4)
	c := s.dial(t, s.ext.Broker().Addr().String(), "pump-1", user, pass)

	subscribe(t, c, "sensors/pump-1/#", msgs)
	publish(t, c, "sensors/pump-1/up", "21.5")

	select {
	case m := <-msgs:
		assert.Equal(t, [2]string{"sensors/pump-1/up", "21.5"}, m)
	case <-time.After(waitTimeout):
		require.FailNow(t, "no message received")
	}

	// Nothing of the database sits on disk.
	assert.NoFileExists(t, cfg.Server.SQLitePath)
	require.NoError(t, s.ext.Health(t.Context()))

	// Stop it, and start the binary again on the same database. Migrations
	// run a second time over the schema they made, and the state is there.
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	require.NoError(t, s.app.Stop(ctx))

	cfg.Tether.Broker.DataDir = t.TempDir()
	cfg.Server.Database.DSN = dsn
	s2 := start(t, cfg, testToken, false)

	tenants := s2.do(t, http.MethodGet, "/tether/v1/tenants", testToken, nil, http.StatusOK)
	assert.Contains(t, fmt.Sprint(tenants), tid)

	c2 := s2.dial(t, s2.ext.Broker().Addr().String(), "pump-1", user, pass)
	assert.True(t, c2.IsConnected(), "the key made before the restart still works")
}

// Two binaries routed together over embedded NATS share one PostgreSQL. A
// device made through A's admin API connects to B, and a QoS 1 message a
// device publishes on A reaches a subscriber on B.
func TestBinaryCellOnOnePostgres(t *testing.T) {
	cfg, _ := postgresConfig(t)

	ports := clustertest.FreePorts(t, 2)
	nodes := make([]*started, 2)
	apps := make([]forge.App, 2)

	// The nodes are built and started at once on an empty database, the way
	// an orchestrator starts replicas, so they race to run the migrations.
	built := make([]error, len(apps))
	closers := make([]func(), len(apps))

	var wg sync.WaitGroup

	for i := range apps {
		c := cfg
		c.Tether.Broker.DataDir = t.TempDir()
		c.Tether.Broker.TCPAddress = "127.0.0.1:0"
		c.Tether.Cluster = cluster.DefaultConfig()
		c.Tether.Cluster.Enabled = true
		c.Tether.Cluster.Cell = "pgcell"
		c.Tether.Cluster.Embedded.StoreDir = t.TempDir()
		c.Tether.Cluster.Embedded.ClusterName = "tetherpg"
		c.Tether.Cluster.Embedded.ClusterListen = "127.0.0.1:" + strconv.Itoa(ports[i])
		c.Tether.Cluster.Embedded.Routes = []string{clustertest.RouteURL(ports[1-i])}
		c.Tether.Cluster.Embedded.User = "tether"
		c.Tether.Cluster.Embedded.Password = "route-secret"

		require.NoError(t, applyCluster(&c, "n"+strconv.Itoa(i+1), ""))
		require.NoError(t, applyDatabase(&c, ""))

		wg.Go(func() {
			apps[i], closers[i], built[i] = newApp(c, testToken, false,
				forge.WithAppLogger(forge.NewNoopLogger()),
				forge.WithEnableConfigAutoDiscovery(false),
			)
		})
	}

	wg.Wait()

	for i := range apps {
		require.NoError(t, built[i], "build node %d", i+1)
		t.Cleanup(closers[i])
	}

	// Each node waits for the other before it serves.
	errs := make([]error, len(apps))

	for i, app := range apps {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), cellWait)
			defer cancel()

			errs[i] = app.Start(ctx)
		})
	}

	wg.Wait()

	t.Cleanup(func() {
		var stop sync.WaitGroup

		for _, app := range apps {
			stop.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), cellWait)
				defer cancel()

				_ = app.Stop(ctx)
			})
		}

		stop.Wait()
	})

	for i, app := range apps {
		require.NoError(t, errs[i], "node %d", i+1)

		ext, err := vessel.Inject[*extension.Extension](app.Container())
		require.NoError(t, err)

		srv := httptest.NewServer(app.Router())
		t.Cleanup(srv.Close)

		nodes[i] = &started{app: app, ext: ext, srv: srv}
	}

	a, b := nodes[0], nodes[1]

	// Everything is made on A.
	tid := a.tenant(t)
	a.role(t, tid, "sensor", []string{"sensors/%c/up"}, nil)
	a.role(t, tid, "display", nil, []string{"sensors/+/up"})
	pumpUser, pumpPass := a.device(t, tid, "pump", "sensor")
	dispUser, dispPass := a.device(t, tid, "display", "display")

	// And B sees it: the tenant is in the shared database.
	assert.Contains(t, fmt.Sprint(b.do(t, http.MethodGet, "/tether/v1/tenants", testToken, nil, http.StatusOK)), tid)

	msgs := make(chan [2]string, 4)
	display := b.dial(t, b.ext.Broker().Addr().String(), "display-1", dispUser, dispPass)
	subscribe(t, display, "sensors/+/up", msgs)

	pump := a.dial(t, a.ext.Broker().Addr().String(), "pump-1", pumpUser, pumpPass)
	publish(t, pump, "sensors/pump-1/up", "21.5")

	select {
	case m := <-msgs:
		assert.Equal(t, [2]string{"sensors/pump-1/up", "21.5"}, m)
	case <-time.After(waitTimeout):
		require.FailNow(t, "the message did not cross from node 1 to node 2")
	}

	for i, n := range nodes {
		require.NoError(t, n.ext.Health(t.Context()), "node %d", i+1)
	}
}

// Several nodes opening one empty database at the same moment all end up
// with a working schema. Each opener runs the three migrations the
// extensions run at Start, keysmith's, warden's and tether's, on its own
// pool. They all go through grove's locked orchestrator, so the nodes take
// turns. keysmith v1.8.0 ran plain statements with no lock and failed here
// with a duplicate key on pg_type; the binary used to hold a lock around it.
func TestConcurrentOpenOnAFreshDatabase(t *testing.T) {
	_, dsn := postgresConfig(t)

	const nodes = 8

	errs := make([]error, nodes)

	var wg sync.WaitGroup

	for i := range nodes {
		wg.Go(func() {
			db, err := openPostgres(dsn)
			if err != nil {
				errs[i] = err

				return
			}

			defer func() { _ = db.Close() }()

			ctx := t.Context()

			if err = kspostgres.New(pgdriver.Unwrap(db)).Migrate(ctx); err != nil {
				errs[i] = fmt.Errorf("keysmith: %w", err)

				return
			}

			if err = wdpostgres.New(db).Migrate(ctx); err != nil {
				errs[i] = fmt.Errorf("warden: %w", err)

				return
			}

			errs[i] = tpostgres.New(db).Migrate(ctx)
		})
	}

	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "opener %d", i)
	}
}

// A wrong password stops startup with the server's refusal, the user and the
// host, and not the password that was tried.
func TestWrongPasswordIsNotEchoed(t *testing.T) {
	_, dsn := postgresConfig(t)

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	u.User = url.UserPassword("postgres", pgSecret)

	_, err = openPostgres(u.String())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres@127.0.0.1:")
	assert.NotContains(t, err.Error(), pgSecret)
}
