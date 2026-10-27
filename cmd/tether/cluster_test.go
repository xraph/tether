package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/cluster"
)

// --node-id and TETHER_NODE_ID name the node (the flag first, then the
// environment, then the file), an embedded node's JetStream data goes under
// ./data/nats/<node id> unless the file says otherwise, and an embedded cell
// past three nodes is refused. A one-node embedded cell then boots.
func TestBinaryClusterFlags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tether.yaml")
	yaml := "tether:\n  cluster:\n    enabled: true\n    node_id: from-file\n    cell: c1\n"
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	for _, tc := range []struct {
		name, flag, env, want string
	}{
		{"flag wins", "flag-node", "env-node", "flag-node"},
		{"environment over the file", "", "env-node", "env-node"},
		{"the file", "", "", "from-file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadConfig(path, true)
			require.NoError(t, err)
			require.NoError(t, applyCluster(&cfg, tc.flag, tc.env))

			c := cfg.Tether.Cluster
			assert.True(t, c.Enabled)
			assert.Equal(t, cluster.ModeEmbedded, c.Mode, "embedded is the default mode")
			assert.Equal(t, tc.want, c.NodeID)
			assert.Equal(t, filepath.Join(defaultNATSDir, tc.want), c.Embedded.StoreDir)
		})
	}

	t.Run("run takes the flag and the variable", func(t *testing.T) {
		big := filepath.Join(dir, "big.yaml")
		yaml := "tether:\n  cluster:\n    enabled: true\n    cell: c1\n    embedded:\n      cluster_name: x\n" +
			"      cluster_listen: 127.0.0.1:6222\n      routes: [nats://127.0.0.1:6223, nats://127.0.0.1:6224, nats://127.0.0.1:6225]\n"
		require.NoError(t, os.WriteFile(big, []byte(yaml), 0o600))

		env := map[string]string{adminTokenEnv: testToken, nodeIDEnv: "env-node"}

		// The config is checked before anything starts.
		err := run([]string{"--config", big, "--node-id", "n1"}, func(k string) string { return env[k] }, io.Discard, io.Discard)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "up to 3 nodes")

		err = run([]string{"--config", big}, func(k string) string { return env[k] }, io.Discard, io.Discard)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "up to 3 nodes")
	})

	t.Run("a store dir in the file stays", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.Tether.Cluster.Enabled = true
		cfg.Tether.Cluster.Embedded.StoreDir = "/var/lib/tether/nats"

		require.NoError(t, applyCluster(&cfg, "n1", ""))
		assert.Equal(t, "/var/lib/tether/nats", cfg.Tether.Cluster.Embedded.StoreDir)
	})

	t.Run("embedded up to three nodes", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.Tether.Cluster.Enabled = true
		cfg.Tether.Cluster.Embedded.Routes = []string{"nats://a:6222", "nats://b:6222"}
		require.NoError(t, applyCluster(&cfg, "n1", ""))

		cfg.Tether.Cluster.Embedded.Routes = append(cfg.Tether.Cluster.Embedded.Routes, "nats://c:6222")
		err := applyCluster(&cfg, "n1", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "external")
	})

	t.Run("external has no such cap", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.Tether.Cluster.Enabled = true
		cfg.Tether.Cluster.Mode = cluster.ModeExternal
		cfg.Tether.Cluster.URLs = []string{"nats://a", "nats://b", "nats://c", "nats://d", "nats://e"}

		require.NoError(t, applyCluster(&cfg, "n1", ""))
		assert.Empty(t, cfg.Tether.Cluster.Embedded.StoreDir)
	})

	t.Run("off leaves the cluster alone", func(t *testing.T) {
		cfg := defaultConfig()
		require.NoError(t, applyCluster(&cfg, "", ""))
		assert.Equal(t, defaultConfig(), cfg)
	})

	t.Run("an embedded node boots", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.Tether.Cluster.Enabled = true
		cfg.Tether.Cluster.Cell = "bin"
		cfg.Tether.Cluster.Embedded.StoreDir = filepath.Join(t.TempDir(), "nats")
		require.NoError(t, applyCluster(&cfg, "", "env-node"))

		s := start(t, cfg, testToken, false)

		require.NoError(t, s.ext.Health(t.Context()))
		require.NoError(t, s.ext.Broker().ClusterHealth(t.Context()), "the broker runs in its cell")
		assert.DirExists(t, filepath.Join(cfg.Tether.Cluster.Embedded.StoreDir, "jetstream"), "the embedded server's JetStream data")
	})
}
