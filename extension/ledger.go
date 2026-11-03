package extension

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/ledger"

	"github.com/xraph/tether/metering"
)

// ledgerSink sends metering deltas to ledger as keyed usage events.
type ledgerSink struct {
	l *ledger.Ledger
}

var _ metering.Sink = ledgerSink{}

// Meter records the usage under the tenant's ExternalID and AppID, counted
// once per key. Ledger's ErrInvalidInput (a key or tenant it will never take)
// is returned wrapped in metering.ErrRejected; a full buffer or a stopped
// ledger is returned as it is, for the next window to retry.
func (s ledgerSink) Meter(ctx context.Context, t metering.Tenant, feature string, quantity int64, key string) error {
	err := s.l.MeterWithKey(ledger.WithTenant(ctx, t.ExternalID, t.AppID), feature, quantity, key)
	if errors.Is(err, ledger.ErrInvalidInput) {
		return fmt.Errorf("%w: %w", metering.ErrRejected, err)
	}

	return err
}

// meteringTenants adapts the shared tenant cache to metering.Tenants.
type meteringTenants struct {
	c *tenantCache
}

var _ metering.Tenants = meteringTenants{}

// Get returns the tenant's billing identity.
func (a meteringTenants) Get(ctx context.Context, tenantID string) (metering.Tenant, error) {
	ref, err := a.c.lookup(ctx, tenantID)
	if err != nil {
		return metering.Tenant{}, err
	}

	return metering.Tenant{ID: ref.ID, AppID: ref.AppID, ExternalID: ref.ExternalID}, nil
}

// ledgerChecker answers metering's entitlement checks from ledger.
type ledgerChecker struct {
	l *ledger.Ledger
}

var _ metering.Checker = ledgerChecker{}

// Check asks ledger whether the tenant (its ExternalID in its AppID) is
// entitled to feature. Ledger answers a soft-limit feature over its limit as
// allowed. A store outage comes back as an error wrapping
// ledger.ErrUnavailable, and a canceled or expired ctx as its own error.
func (c ledgerChecker) Check(ctx context.Context, t metering.Tenant, feature string) (allowed bool, reason string, err error) {
	res, err := c.l.Check(ledger.WithTenant(ctx, t.ExternalID, t.AppID), feature)
	if err != nil {
		return false, "", err
	}

	if res == nil {
		return false, "", fmt.Errorf("ledger: no entitlement result for %s", feature)
	}

	return res.Allowed, res.Reason, nil
}
