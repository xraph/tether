// Package id defines the TypeID-based identity types for Tether entities.
//
// Every entity uses one ID struct, and the prefix says what kind of entity it
// names. IDs are K-sortable (UUIDv7-based), globally unique and URL-safe, in
// the form "prefix_suffix".
package id

import (
	"database/sql/driver"
	"fmt"

	"go.jetify.com/typeid/v2"
)

// Prefix identifies the entity type encoded in a TypeID.
type Prefix string

// Prefix constants for the Tether entity types.
const (
	PrefixTenant Prefix = "ttnt"
	PrefixDevice Prefix = "tdev"
)

// ID is the identifier type for all Tether entities. It wraps a TypeID, so it
// carries a prefix and sorts by creation time.
//
//nolint:recvcheck // Value receivers for read-only methods, pointer receivers for UnmarshalText and Scan.
type ID struct {
	inner typeid.TypeID
	valid bool
}

// Nil is the zero-value ID.
var Nil ID

// TenantID identifies a tenant (prefix "ttnt").
type TenantID = ID

// DeviceID identifies a device (prefix "tdev").
type DeviceID = ID

// New generates a new ID with the given prefix. It panics when the prefix is
// not a valid TypeID prefix, which is a programming error.
func New(prefix Prefix) ID {
	tid, err := typeid.Generate(string(prefix))
	if err != nil {
		panic(fmt.Sprintf("id: invalid prefix %q: %v", prefix, err))
	}

	return ID{inner: tid, valid: true}
}

// NewTenantID generates a new tenant ID.
func NewTenantID() TenantID { return New(PrefixTenant) }

// NewDeviceID generates a new device ID.
func NewDeviceID() DeviceID { return New(PrefixDevice) }

// Parse parses a TypeID string such as "ttnt_01h2xcejqtf2nbrexx3vqjhp41" into
// an ID. It returns an error when the string is empty or malformed.
func Parse(s string) (ID, error) {
	if s == "" {
		return Nil, fmt.Errorf("id: parse %q: empty string", s)
	}

	tid, err := typeid.Parse(s)
	if err != nil {
		return Nil, fmt.Errorf("id: parse %q: %w", s, err)
	}

	return ID{inner: tid, valid: true}, nil
}

// ParseWithPrefix parses a TypeID string and checks that its prefix matches
// the expected one.
func ParseWithPrefix(s string, expected Prefix) (ID, error) {
	parsed, err := Parse(s)
	if err != nil {
		return Nil, err
	}

	if parsed.Prefix() != expected {
		return Nil, fmt.Errorf("id: expected prefix %q, got %q", expected, parsed.Prefix())
	}

	return parsed, nil
}

// ParseTenantID parses a string and checks for the "ttnt" prefix.
func ParseTenantID(s string) (TenantID, error) { return ParseWithPrefix(s, PrefixTenant) }

// ParseDeviceID parses a string and checks for the "tdev" prefix.
func ParseDeviceID(s string) (DeviceID, error) { return ParseWithPrefix(s, PrefixDevice) }

// String returns the full TypeID string (prefix_suffix), or an empty string
// for the Nil ID.
func (i ID) String() string {
	if !i.valid {
		return ""
	}

	return i.inner.String()
}

// Prefix returns the prefix component of this ID.
func (i ID) Prefix() Prefix {
	if !i.valid {
		return ""
	}

	return Prefix(i.inner.Prefix())
}

// IsNil reports whether this ID is the zero value.
func (i ID) IsNil() bool {
	return !i.valid
}

// MarshalText implements encoding.TextMarshaler.
func (i ID) MarshalText() ([]byte, error) {
	if !i.valid {
		return []byte{}, nil
	}

	return []byte(i.inner.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (i *ID) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		*i = Nil

		return nil
	}

	parsed, err := Parse(string(data))
	if err != nil {
		return err
	}

	*i = parsed

	return nil
}

// Value implements driver.Valuer. It returns nil for the Nil ID so optional
// foreign key columns store NULL.
func (i ID) Value() (driver.Value, error) {
	if !i.valid {
		return nil, nil //nolint:nilnil // nil is the canonical NULL for driver.Valuer
	}

	return i.inner.String(), nil
}

// Scan implements sql.Scanner.
func (i *ID) Scan(src any) error {
	if src == nil {
		*i = Nil

		return nil
	}

	switch v := src.(type) {
	case string:
		return i.UnmarshalText([]byte(v))
	case []byte:
		return i.UnmarshalText(v)
	default:
		return fmt.Errorf("id: cannot scan %T into ID", src)
	}
}
