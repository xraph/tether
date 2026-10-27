package extension

import (
	"testing"

	"github.com/xraph/tether/cluster"
)

// A WithCluster literal with only the fields a cell needs passes Validate:
// the zero mode, heartbeat, lease and log age take cluster's defaults (M7).
func TestWithClusterDefaultsZeroFields(t *testing.T) {
	e := New(WithCluster(cluster.Config{
		NodeID: "n1", Cell: "prod", Mode: cluster.ModeExternal, URLs: []string{"nats://nats-1:4222"},
	}))

	cfg := e.config.Cluster
	if err := cfg.Validate(); err != nil {
		t.Fatalf("WithCluster literal: %v", err)
	}

	def := cluster.DefaultConfig()
	if cfg.Heartbeat != def.Heartbeat || cfg.Lease != def.Lease || cfg.LogMaxAge != def.LogMaxAge || cfg.Mode != cluster.ModeExternal {
		t.Fatalf("got %+v", cfg)
	}

	e = New(WithCluster(cluster.Config{NodeID: "n1", Cell: "prod", Embedded: cluster.EmbeddedConfig{StoreDir: t.TempDir()}}))
	if got := e.config.Cluster.Mode; got != cluster.ModeEmbedded {
		t.Fatalf("zero mode became %q, want embedded", got)
	}
}
