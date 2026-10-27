package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// cellInfo is what the cell command writes to cell.json: everything the load
// commands need to find a running cell. --cell cell.json fills the target flags
// from it.
type cellInfo struct {
	Targets  []string `json:"targets"`     // MQTT listeners, one per broker node
	Admins   []string `json:"admins"`      // admin API base URLs, one per node
	Token    string   `json:"token"`       // admin bearer token
	Tenant   string   `json:"tenant"`      // the bench tenant ID
	Creds    string   `json:"creds"`       // path of the credentials CSV
	Brokers  []int    `json:"broker_pids"` // broker process IDs, in node order
	Servers  []int    `json:"nats_pids"`   // NATS server process IDs
	Dir      string   `json:"dir"`         // the cell's data and logs
	Binary   string   `json:"binary"`      // the tether binary the nodes run
	Replicas int      `json:"replicas"`    // JetStream replicas of the cell's streams
	Started  string   `json:"started_at"`  // RFC 3339
	Machine  *machine `json:"machine,omitempty"`
}

func readCell(path string) (cellInfo, error) {
	var ci cellInfo

	raw, err := os.ReadFile(path)
	if err != nil {
		return ci, err
	}

	if err := json.Unmarshal(raw, &ci); err != nil {
		return ci, fmt.Errorf("%s: %w", path, err)
	}

	return ci, nil
}

// targetFlags are the flags every load command shares.
type targetFlags struct {
	cell    *string
	targets *string
	creds   *string
	pids    *string
	natsPid *string
	admins  *string
	token   *string
	tenant  *string
	prefix  *string
	timeout *time.Duration
	only    *int
}

func addTargetFlags(fs *flag.FlagSet) *targetFlags {
	return &targetFlags{
		cell:    fs.String("cell", "", "cell.json written by `tether-bench cell`; fills in the flags below"),
		targets: fs.String("target", "", "MQTT listener(s) as host:port, comma separated; clients spread over them"),
		creds:   fs.String("creds", "", "credentials CSV (username,password) from `tether-bench provision`"),
		pids:    fs.String("pids", "", "broker process IDs, comma separated, to read memory and CPU from"),
		natsPid: fs.String("nats-pids", "", "NATS server process IDs, comma separated, to read CPU from"),
		admins:  fs.String("admin", "", "admin API base URLs, one per target, to read each node's connection count"),
		token:   fs.String("token", "", "admin bearer token (default $"+tokenEnv+", then the cell file)"),
		tenant:  fs.String("tenant", "", "tenant ID for the admin stats"),
		prefix:  fs.String("prefix", "", "client ID prefix (default: random per run)"),
		timeout: fs.Duration("timeout", 30*time.Second, "per connect and per packet timeout"),
		only:    fs.Int("only-node", -1, "use only this target (0 is the first) and leave the others idle"),
	}
}

// build turns the flags, and the cell file under them, into a target.
func (f *targetFlags) build() (*target, error) {
	t := &target{
		addrs:   splitList(*f.targets),
		prefix:  *f.prefix,
		admins:  splitList(*f.admins),
		token:   *f.token,
		tenant:  *f.tenant,
		timeout: *f.timeout,
	}

	if t.token == "" {
		t.token = os.Getenv(tokenEnv)
	}

	var err error

	if t.pids, err = parsePIDs(*f.pids); err != nil {
		return nil, err
	}

	if t.serverPids, err = parsePIDs(*f.natsPid); err != nil {
		return nil, err
	}

	credsPath := *f.creds

	if *f.cell != "" {
		ci, cerr := readCell(*f.cell)
		if cerr != nil {
			return nil, cerr
		}

		if len(t.addrs) == 0 {
			t.addrs = ci.Targets
		}

		if len(t.admins) == 0 {
			t.admins = ci.Admins
		}

		if len(t.pids) == 0 {
			t.pids = ci.Brokers
		}

		if len(t.serverPids) == 0 {
			t.serverPids = ci.Servers
		}

		if t.token == "" {
			t.token = ci.Token
		}

		if t.tenant == "" {
			t.tenant = ci.Tenant
		}

		if credsPath == "" {
			credsPath = ci.Creds
		}
	}

	if len(t.addrs) == 0 {
		return nil, errors.New("no target: give --target host:port or --cell cell.json")
	}

	if credsPath == "" {
		return nil, errors.New("no credentials: give --creds creds.csv (see `tether-bench provision`)")
	}

	if t.creds, err = readCreds(credsPath); err != nil {
		return nil, err
	}

	if t.prefix == "" {
		t.prefix = newPrefix()
	}

	if n := *f.only; n >= 0 {
		if n >= len(t.addrs) {
			return nil, fmt.Errorf("--only-node %d: there are %d targets", n, len(t.addrs))
		}

		t.addrs = t.addrs[n : n+1]
	}

	return t, nil
}

// nodeStats is one node's own count of what it holds, from its admin API.
type nodeStats struct {
	Connected int `json:"connected"`
	Sessions  int `json:"sessions"`
}

// adminStats asks each node for its tenant stats. It returns nil when the
// target has no admin URLs.
func (t *target) adminStats(ctx context.Context) ([]nodeStats, error) {
	if len(t.admins) == 0 || t.tenant == "" {
		return nil, nil
	}

	out := make([]nodeStats, len(t.admins))

	var errs []error

	for i, base := range t.admins {
		a := newAdminClient(strings.TrimRight(base, "/"), t.token)

		if _, err := a.do(ctx, "GET", "/tenants/"+t.tenant+"/stats", nil, &out[i]); err != nil {
			errs = append(errs, err)
		}
	}

	return out, errors.Join(errs...)
}
