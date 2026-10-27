package extension_test

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/extension"
)

// startCellNode boots the whole stack as the one node of a cell on a
// one-server NATS, through the Task 14 seam (WithClusterDeps).
func startCellNode(t *testing.T) *env {
	t.Helper()

	urls := clustertest.Start(t, 1)

	ccfg := cluster.DefaultConfig()
	ccfg.Enabled = true
	ccfg.NodeID = "n1"
	ccfg.Cell = "ext"
	ccfg.Mode = cluster.ModeExternal
	ccfg.URLs = urls
	ccfg.Replicas = 1

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	conn, err := cluster.Connect(ctx, ccfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	require.NoError(t, cluster.Provision(ctx, conn, ccfg, loopback().MaxSessionExpiry))

	return startApp(t, newApp(t), false, extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()),
		extension.WithClusterDeps(&broker.Cluster{Conn: conn, Config: ccfg}))
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
