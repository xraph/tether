package tether

import "errors"

// Sentinel errors returned by Tether operations. Store backends wrap them as
// "tether/<backend>: <op>: %w", so callers match with errors.Is.
var (
	// ErrNotFound is returned when a tenant, device or other record does not exist.
	ErrNotFound = errors.New("tether: not found")

	// ErrConflict is returned when a write collides with an existing record.
	ErrConflict = errors.New("tether: conflict")

	// ErrInvalidInput is returned when a caller supplies a value Tether rejects.
	ErrInvalidInput = errors.New("tether: invalid input")

	// ErrTenantDisabled is returned when a tenant exists but has been disabled.
	ErrTenantDisabled = errors.New("tether: tenant disabled")

	// ErrNotAuthorized is returned when a client may not perform the requested action.
	ErrNotAuthorized = errors.New("tether: not authorized")
)
