package extension

import (
	"testing"

	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/metering"
)

// A WithCluster literal with only the fields a cell needs passes Validate:
// the zero mode, heartbeat, lease, log age, pull heartbeat and load interval take cluster's
// defaults (M7).
func TestWithClusterDefaultsZeroFields(t *testing.T) {
	e := New(WithCluster(cluster.Config{
		NodeID: "n1", Cell: "prod", Mode: cluster.ModeExternal, URLs: []string{"nats://nats-1:4222"},
	}))

	cfg := e.config.Cluster
	if err := cfg.Validate(); err != nil {
		t.Fatalf("WithCluster literal: %v", err)
	}

	def := cluster.DefaultConfig()
	if cfg.Heartbeat != def.Heartbeat || cfg.Lease != def.Lease || cfg.LogMaxAge != def.LogMaxAge ||
		cfg.PullHeartbeat != def.PullHeartbeat || cfg.LoadInterval != def.LoadInterval || cfg.Mode != cluster.ModeExternal {
		t.Fatalf("got %+v", cfg)
	}

	e = New(WithCluster(cluster.Config{NodeID: "n1", Cell: "prod", Embedded: cluster.EmbeddedConfig{StoreDir: t.TempDir()}}))
	if got := e.config.Cluster.Mode; got != cluster.ModeEmbedded {
		t.Fatalf("zero mode became %q, want embedded", got)
	}
}

// WithMetering(metering.Config{Enabled: true}) meters every feature under
// the default keys, rather than nothing. A config that names any key keeps
// the keys it gave, empty ones included.
func TestWithMeteringDefaultsEmptyFeatures(t *testing.T) {
	e := New(WithMetering(metering.Config{Enabled: true}))

	cfg := e.config.Metering
	if err := cfg.Validate(); err != nil {
		t.Fatalf("WithMetering literal: %v", err)
	}

	if def := metering.DefaultConfig(); cfg.Features != def.Features || cfg.Interval != def.Interval {
		t.Fatalf("got %+v", cfg)
	}

	one := metering.Features{MessagesIn: "my.messages"}

	e = New(WithMetering(metering.Config{Enabled: true, Features: one}))
	if got := e.config.Metering.Features; got != one {
		t.Fatalf("features became %+v, want %+v", got, one)
	}
}
