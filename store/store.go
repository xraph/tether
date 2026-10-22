// Package store defines the composite persistence interface for Tether.
package store

import (
	"context"

	"github.com/xraph/tether/device"
	"github.com/xraph/tether/tenant"
)

// Store composes the tenant and device stores with lifecycle methods.
// Backends live in store/memory, store/sqlite and store/postgres, and each one
// must pass store/storetest.Run.
type Store interface {
	tenant.Store
	device.Store

	// Migrate creates or upgrades the schema. It is safe to call repeatedly.
	Migrate(ctx context.Context) error

	// Ping checks that the backend is reachable.
	Ping(ctx context.Context) error

	// Close releases the backend's resources.
	Close() error
}
