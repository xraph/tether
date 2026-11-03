package extension

import (
	"context"
	"errors"
)

// FlushMeter runs one metering window now, as the meter's own ticker would.
// It fails when Start built no meter.
func FlushMeter(ctx context.Context, e *Extension) error {
	e.mu.RLock()
	p := e.product
	e.mu.RUnlock()

	if p == nil || p.meter == nil {
		return errors.New("tether: no meter running")
	}

	return p.meter.Flush(ctx)
}

// ProductParts reports which of the meter, the entitlement checks and the
// audit recorder Start built.
func ProductParts(e *Extension) (meter, entitlements, audit bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	p := e.product
	if p == nil {
		return false, false, false
	}

	return p.meter != nil, p.ent != nil, p.rec != nil
}
