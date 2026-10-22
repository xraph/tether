package sqlite

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// BusyTimeout is how long a connection opened with DSN waits for another
// connection's write lock before it gives up with SQLITE_BUSY.
const BusyTimeout = 5 * time.Second

// DSN returns the data source name to open the database at path with, for
// sqlitedriver.SqliteDB.Open. Open the database you hand to New with it:
//
//	sdb := sqlitedriver.New()
//	err := sdb.Open(ctx, sqlite.DSN("tether.db"))
//
// database/sql pools connections and every pooled connection has its own
// settings, so the DSN is the one place that reaches all of them. Grove's
// driver runs PRAGMA foreign_keys=ON once at Open, on whichever connection it
// gets, and sets no busy timeout. DSN adds three settings, applied by
// modernc.org/sqlite to each connection it opens:
//
//   - busy_timeout: a write waits up to BusyTimeout for the write lock
//     instead of failing at once with SQLITE_BUSY. Device and tenant writes
//     arrive from connection handlers and the admin API at the same time.
//   - foreign_keys: on for every connection. Tether's tables declare no
//     foreign keys, so this only matters to tables a host adds beside them.
//   - _txlock=immediate: transactions take the write lock at BEGIN. SQLite
//     skips the busy timeout when a transaction that has already read tries
//     to write, and fails at once.
//
// path is a file name or a file: URI. Query parameters already on it are
// kept, and so is any of the three settings it already sets: the caller's
// _txlock, busy_timeout or foreign_keys wins, and DSN adds only the ones that
// are missing. If you open the database some other way, such as from a grove
// extension's config, put the same three parameters on that DSN.
func DSN(path string) string {
	var hasTxLock, hasBusy, hasFK bool

	if i := strings.IndexByte(path, '?'); i >= 0 {
		q, err := url.ParseQuery(path[i+1:])
		if err != nil {
			// Leave a query that does not parse for modernc to reject at Open.
			return path
		}

		_, hasTxLock = q["_txlock"]

		for _, p := range q["_pragma"] {
			p = strings.ToLower(strings.TrimSpace(p))
			hasBusy = hasBusy || strings.HasPrefix(p, "busy_timeout")
			hasFK = hasFK || strings.HasPrefix(p, "foreign_keys")
		}
	}

	var add []string

	if !hasBusy {
		add = append(add, fmt.Sprintf("_pragma=busy_timeout(%d)", BusyTimeout.Milliseconds()))
	}

	if !hasFK {
		add = append(add, "_pragma=foreign_keys(1)")
	}

	if !hasTxLock {
		add = append(add, "_txlock=immediate")
	}

	if len(add) == 0 {
		return path
	}

	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}

	return path + sep + strings.Join(add, "&")
}
