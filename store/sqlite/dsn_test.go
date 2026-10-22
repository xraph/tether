package sqlite_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xraph/tether/store/sqlite"
)

func TestDSN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			"plain path",
			"tether.db",
			"tether.db?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_txlock=immediate",
		},
		{
			"file URI with a query",
			"file:tether.db?cache=shared",
			"file:tether.db?cache=shared&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_txlock=immediate",
		},
		{
			"caller busy timeout wins",
			"tether.db?_pragma=busy_timeout(100)",
			"tether.db?_pragma=busy_timeout(100)&_pragma=foreign_keys(1)&_txlock=immediate",
		},
		{
			"caller txlock wins",
			"tether.db?_txlock=deferred",
			"tether.db?_txlock=deferred&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)",
		},
		{
			"query that does not parse is left alone",
			"tether.db?a=%zz",
			"tether.db?a=%zz",
		},
		{
			"everything set",
			"tether.db?_pragma=busy_timeout(1)&_pragma=foreign_keys(0)&_txlock=exclusive",
			"tether.db?_pragma=busy_timeout(1)&_pragma=foreign_keys(0)&_txlock=exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sqlite.DSN(tt.in))
		})
	}
}
