// Package metering sends each tenant's broker usage to a billing sink, ledger
// in the extension, once per interval.
//
// It reads the broker's cumulative counters, works out what grew since the
// sink last accepted a value, and sends the growth under a key the sink can
// use to count it once. The package does not import ledger: the extension
// adapts ledger to Sink.
package metering

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/tether/broker"
)

// ErrRejected is what a Sink wraps when the billing system will never accept
// a value, such as a malformed key or an unknown tenant. The Meter logs the
// delta at Error and drops it instead of retrying it every window.
var ErrRejected = errors.New("metering: sink rejected the usage")

// maxFeatureKeyLen is the longest feature key Validate accepts. Ledger's
// idempotency key (at most 255 bytes) carries it, behind the node ID, the
// start time, the window and the tenant ID.
const maxFeatureKeyLen = 64

// Tenant is the billing identity of one Tether tenant: its Tether ID, and the
// (app, external) pair the host application knows it by. Ledger's tenant is
// ExternalID and its app is AppID.
type Tenant struct {
	ID         string
	AppID      string
	ExternalID string
}

// Tenants looks a tenant's billing identity up by its Tether ID.
type Tenants interface {
	Get(ctx context.Context, tenantID string) (Tenant, error)
}

// Source is the broker's side of the contract. *broker.Broker satisfies it.
type Source interface {
	// Usage returns every tenant's cumulative usage since the broker started.
	Usage() []broker.TenantUsage

	// StartedAt returns when the counters began. It changes when the broker
	// restarts, and the counters start from zero again.
	StartedAt() time.Time
}

// Sink takes one tenant's usage for one feature.
//
// key counts the delta once: a Sink that sees the same key twice must add it
// once. An error wrapping ErrRejected means the value can never be accepted.
// Any other error is treated as temporary, and the delta is sent again in a
// later window under a new key.
type Sink interface {
	Meter(ctx context.Context, t Tenant, feature string, quantity int64, key string) error
}

// Features names the sink's feature key for each meter. An empty key turns
// that meter off.
type Features struct {
	MessagesIn        string `yaml:"messages_in" json:"messages_in"`
	MessagesOut       string `yaml:"messages_out" json:"messages_out"`
	BytesIn           string `yaml:"bytes_in" json:"bytes_in"`
	BytesOut          string `yaml:"bytes_out" json:"bytes_out"`
	ConnectionMinutes string `yaml:"connection_minutes" json:"connection_minutes"`
}

// Config configures a Meter.
type Config struct {
	// Enabled turns metering on. Start and Stop do nothing without it.
	Enabled bool `yaml:"enabled" json:"enabled"`

	// Interval is the time between windows. Default 60 s.
	Interval time.Duration `yaml:"interval" json:"interval"`

	// Features names the feature key each meter reports under.
	Features Features `yaml:"features" json:"features"`
}

// DefaultConfig returns metering off, with a 60 s window and the
// tether.* feature keys.
func DefaultConfig() Config {
	return Config{
		Interval: time.Minute,
		Features: Features{
			MessagesIn:        "tether.messages.in",
			MessagesOut:       "tether.messages.out",
			BytesIn:           "tether.bytes.in",
			BytesOut:          "tether.bytes.out",
			ConnectionMinutes: "tether.connection.minutes",
		},
	}
}

// Validate reports a feature key that is longer than 64 bytes, has a byte
// outside [-_.a-zA-Z0-9], or is shared with another meter (the sink counts a
// key once, so two meters under one feature would lose one of them). It also
// reports a non-positive Interval when metering is enabled.
func (c Config) Validate() error {
	if c.Enabled && c.Interval <= 0 {
		return fmt.Errorf("metering: interval must be positive, got %s", c.Interval)
	}

	seen := make(map[string]string, 5)

	for _, f := range c.Features.list() {
		if f.key == "" {
			continue
		}

		if err := checkFeatureKey(f.key); err != nil {
			return fmt.Errorf("metering: features.%s: %w", f.name, err)
		}

		if other, dup := seen[f.key]; dup {
			return fmt.Errorf("metering: features.%s and features.%s are both %q", other, f.name, f.key)
		}

		seen[f.key] = f.name
	}

	return nil
}

func checkFeatureKey(key string) error {
	if len(key) > maxFeatureKeyLen {
		return fmt.Errorf("%q is %d bytes, at most %d", key, len(key), maxFeatureKeyLen)
	}

	for i := range len(key) {
		b := key[i]

		switch {
		case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9', b == '-', b == '_', b == '.':
		default:
			return fmt.Errorf("%q has a byte outside [-_.a-zA-Z0-9]", key)
		}
	}

	return nil
}

type feature struct {
	name string
	key  string
	// read picks the meter's cumulative value out of a tenant's usage.
	read func(u broker.TenantUsage) uint64
}

// list returns the five meters in a fixed order.
func (f Features) list() []feature {
	return []feature{
		{"messages_in", f.MessagesIn, func(u broker.TenantUsage) uint64 { return u.MessagesIn }},
		{"messages_out", f.MessagesOut, func(u broker.TenantUsage) uint64 { return u.MessagesOut }},
		{"bytes_in", f.BytesIn, func(u broker.TenantUsage) uint64 { return u.BytesIn }},
		{"bytes_out", f.BytesOut, func(u broker.TenantUsage) uint64 { return u.BytesOut }},
		{"connection_minutes", f.ConnectionMinutes, func(u broker.TenantUsage) uint64 {
			return uint64(max(u.ConnectionTime, 0) / time.Minute)
		}},
	}
}
