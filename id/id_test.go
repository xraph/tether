package id_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/id"
)

func TestTenantIDRoundTrip(t *testing.T) {
	a := id.NewTenantID()
	require.True(t, strings.HasPrefix(a.String(), "ttnt_"))
	b, err := id.ParseTenantID(a.String())
	require.NoError(t, err)
	require.Equal(t, a, b)
	_, err = id.ParseTenantID(id.NewDeviceID().String())
	require.Error(t, err) // wrong prefix
}

func TestDeviceIDRoundTrip(t *testing.T) {
	a := id.NewDeviceID()
	require.True(t, strings.HasPrefix(a.String(), "tdev_"))
	b, err := id.ParseDeviceID(a.String())
	require.NoError(t, err)
	require.Equal(t, a, b)
	_, err = id.ParseDeviceID(id.NewTenantID().String())
	require.Error(t, err)
}

func TestNilAndSQL(t *testing.T) {
	require.True(t, id.Nil.IsNil())
	require.Empty(t, id.Nil.String())

	v, err := id.Nil.Value()
	require.NoError(t, err)
	require.Nil(t, v)

	a := id.NewTenantID()
	val, err := a.Value()
	require.NoError(t, err)

	var scanned id.ID
	require.NoError(t, scanned.Scan(val))
	require.Equal(t, a, scanned)
	require.NoError(t, scanned.Scan(nil))
	require.True(t, scanned.IsNil())
	require.Error(t, scanned.Scan(42))
}

func TestTextRoundTrip(t *testing.T) {
	a := id.NewDeviceID()
	txt, err := a.MarshalText()
	require.NoError(t, err)

	var b id.ID
	require.NoError(t, b.UnmarshalText(txt))
	require.Equal(t, a, b)
	require.Equal(t, id.PrefixDevice, b.Prefix())
}
