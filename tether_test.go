package tether_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
)

func TestVersion(t *testing.T) {
	require.Equal(t, "0.1.0", tether.Version)
}

func TestSentinelsAreDistinctAndWrappable(t *testing.T) {
	all := []error{
		tether.ErrNotFound,
		tether.ErrConflict,
		tether.ErrInvalidInput,
		tether.ErrTenantDisabled,
		tether.ErrNotAuthorized,
	}

	for i, a := range all {
		require.Contains(t, a.Error(), "tether: ")

		wrapped := fmt.Errorf("tether/memory: get tenant: %w", a)
		require.ErrorIs(t, wrapped, a)

		for j, b := range all {
			if i != j {
				require.False(t, errors.Is(a, b))
			}
		}
	}
}
