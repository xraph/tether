// Command interop starts a Tether broker, alone or as a three-node embedded
// cell, for the Eclipse Paho interoperability suite.
//
// The suite connects with no username or password, so this harness gives it
// an authenticator that accepts any client into one tenant and a permission
// loader that grants "#" for publish and subscribe. It is for interop runs
// only. Never use either in production.
//
// Run it with -mode single or -mode cell. It prints the MQTT address of every
// node, one per line as "node n1 mqtt 127.0.0.1:18831", then logs to stderr
// until it gets SIGINT or SIGTERM.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

const (
	modeSingle = "single"
	modeCell   = "cell"

	// connectWait bounds joining the embedded cell. A fresh three-server
	// JetStream cluster needs a few seconds to elect its meta leader.
	connectWait = 60 * time.Second

	// stopWait bounds each node's shutdown.
	stopWait = 10 * time.Second
)

// options are the command line flags.
type options struct {
	mode     string
	host     string
	port     int
	nodes    int
	natsBase int
	dataDir  string
	logLevel string
	grants   []string
}

// node is one running broker and what it needs closed after it.
type node struct {
	id      string
	broker  *broker.Broker
	conn    *cluster.Conn
	members *cluster.Members
	control *cluster.Control
}

// allowAll grants every principal the same filters for publish and subscribe,
// "#" unless the -grant flag adds more. A "#" grant never covers a topic that
// starts with "$"; those need a grant of their own.
type allowAll struct {
	grants []string
}

// Load implements broker.PermLoader.
func (a allowAll) Load(context.Context, *auth.Identity) (acl.Rules, []string, error) {
	return acl.Rules{
		Publish:   slices.Clone(a.grants),
		Subscribe: slices.Clone(a.grants),
	}, []string{"role_interop"}, nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "interop:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	opts, err := parseFlags(args)
	if err != nil {
		return err
	}

	logger, err := newLogger(opts.logLevel)
	if err != nil {
		return err
	}

	dir := opts.dataDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "tether-interop-")
		if err != nil {
			return fmt.Errorf("make data dir: %w", err)
		}

		defer os.RemoveAll(dir)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ids := map[string]*auth.Identity{
		// The suite sends no username, so the empty username is the one key.
		"": {
			Tenant:        &tenant.Tenant{ID: id.NewTenantID(), Name: "interop", Enabled: true},
			PrincipalKind: auth.PrincipalKindAPIKey,
			PrincipalID:   "akey_interop",
		},
	}

	var nodes []*node

	switch opts.mode {
	case modeSingle:
		nodes, err = startSingle(ctx, opts, ids, logger)
	default:
		nodes, err = startCell(ctx, opts, dir, ids, logger)
	}

	if err != nil {
		stopAll(nodes)

		return err
	}

	for _, n := range nodes {
		fmt.Printf("node %s mqtt %s\n", n.id, n.broker.Addr())
	}

	fmt.Println("ready")

	<-ctx.Done()
	stopAll(nodes)

	return nil
}

func parseFlags(args []string) (options, error) {
	var o options

	fs := flag.NewFlagSet("interop", flag.ContinueOnError)
	fs.StringVar(&o.mode, "mode", modeSingle, "single (one broker) or cell (three embedded nodes)")
	fs.StringVar(&o.host, "host", "127.0.0.1", "address to listen on")
	fs.IntVar(&o.port, "port", 18831, "MQTT port of the first node; the next nodes take the following ports")
	fs.IntVar(&o.nodes, "nodes", 3, "nodes in cell mode (1 to 3)")
	fs.IntVar(&o.natsBase, "nats-port", 14222, "NATS client port of the first cell node; cluster routes listen 100 above")
	fs.StringVar(&o.dataDir, "data-dir", "", "directory for the embedded JetStream stores (default: a temp directory removed on exit)")
	fs.StringVar(&o.logLevel, "log-level", "info", "debug, info, warn or error")

	var grant string

	fs.StringVar(&grant, "grant", "#", "comma-separated filters granted for publish and subscribe")

	if err := fs.Parse(args); err != nil {
		return o, err
	}

	o.grants = strings.Split(grant, ",")

	if o.mode != modeSingle && o.mode != modeCell {
		return o, fmt.Errorf("mode %q must be %q or %q", o.mode, modeSingle, modeCell)
	}

	if o.nodes < 1 || o.nodes > 3 {
		return o, fmt.Errorf("nodes %d must be 1 to 3", o.nodes)
	}

	return o, nil
}

func newLogger(level string) (*slog.Logger, error) {
	var lv slog.Level

	if err := lv.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}

	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lv})), nil
}

// brokerConfig is the default config listening on host:port.
func brokerConfig(host string, port int) broker.Config {
	cfg := broker.DefaultConfig()
	cfg.TCPAddress = fmt.Sprintf("%s:%d", host, port)

	return cfg
}

func startSingle(ctx context.Context, o options, ids map[string]*auth.Identity, logger *slog.Logger) ([]*node, error) {
	b, err := broker.New(brokerConfig(o.host, o.port), broker.Deps{
		Auth:   auth.NewStatic(ids),
		Perms:  allowAll{grants: o.grants},
		Logger: logger.With("node", "n1"),
	})
	if err != nil {
		return nil, fmt.Errorf("new broker: %w", err)
	}

	if err := b.Start(ctx); err != nil {
		return nil, fmt.Errorf("start broker: %w", err)
	}

	return []*node{{id: "n1", broker: b}}, nil
}

// clusterConfig is node i's cluster config: an embedded NATS server in one
// three-server cluster, with the other servers as routes.
func clusterConfig(o options, dir string, i int) cluster.Config {
	cfg := cluster.DefaultConfig()
	cfg.Enabled = true
	cfg.NodeID = fmt.Sprintf("n%d", i+1)
	cfg.Cell = "c1"

	routes := make([]string, 0, o.nodes-1)

	for j := range o.nodes {
		if j != i {
			routes = append(routes, fmt.Sprintf("nats://%s:%d", o.host, o.natsBase+100+j))
		}
	}

	cfg.Embedded = cluster.EmbeddedConfig{
		ClusterName:   "interop",
		Listen:        fmt.Sprintf("%s:%d", o.host, o.natsBase+i),
		ClusterListen: fmt.Sprintf("%s:%d", o.host, o.natsBase+100+i),
		Routes:        routes,
		StoreDir:      filepath.Join(dir, "nats", cfg.NodeID),
	}

	return cfg
}

// startCell wires each node the way the extension does: the control bus
// before the broker starts, Members after.
func startCell(ctx context.Context, o options, dir string, ids map[string]*auth.Identity, logger *slog.Logger) ([]*node, error) {
	nodes := make([]*node, o.nodes)
	cfgs := make([]cluster.Config, o.nodes)
	errs := make(chan error, o.nodes)

	// A node of an embedded cluster gets JetStream only once enough peers
	// are up, so the nodes connect together.
	for i := range o.nodes {
		cfgs[i] = clusterConfig(o, dir, i)
		nodes[i] = &node{id: cfgs[i].NodeID}

		go func() {
			cctx, cancel := context.WithTimeout(ctx, connectWait)
			defer cancel()

			conn, err := cluster.Connect(cctx, cfgs[i], logger.With("node", cfgs[i].NodeID))
			if err != nil {
				errs <- fmt.Errorf("%s: connect: %w", cfgs[i].NodeID, err)

				return
			}

			nodes[i].conn = conn
			errs <- nil
		}()
	}

	var joined error

	for range o.nodes {
		joined = errors.Join(joined, <-errs)
	}

	if joined != nil {
		return nodes, joined
	}

	for i, n := range nodes {
		pctx, cancel := context.WithTimeout(ctx, connectWait)
		err := cluster.Provision(pctx, n.conn, cfgs[i], cfgs[i].LogAge())

		cancel()

		if err != nil {
			return nodes, fmt.Errorf("%s: provision: %w", n.id, err)
		}
	}

	for i, n := range nodes {
		if err := startNode(ctx, n, cfgs[i], brokerConfig(o.host, o.port+i), allowAll{grants: o.grants}, ids, logger); err != nil {
			return nodes, fmt.Errorf("%s: %w", n.id, err)
		}
	}

	return nodes, nil
}

func startNode(ctx context.Context, n *node, cfg cluster.Config, bcfg broker.Config, perms broker.PermLoader,
	ids map[string]*auth.Identity, logger *slog.Logger,
) error {
	n.members = cluster.NewMembers(n.conn, cfg)
	n.control = cluster.NewControl(n.conn, cfg)

	b, err := broker.New(bcfg, broker.Deps{
		Auth:   auth.NewStatic(ids),
		Perms:  perms,
		Logger: logger.With("node", n.id),
		Cluster: &broker.Cluster{
			Conn:     n.conn,
			Config:   cfg,
			Sessions: cluster.NewSessions(n.conn, cfg),
			Log:      cluster.NewLog(n.conn, cfg),
			Retained: cluster.NewRetained(n.conn, cfg),
			Members:  n.members,
			Control:  n.control,
		},
	})
	if err != nil {
		return fmt.Errorf("new broker: %w", err)
	}

	n.broker = b

	if err := n.control.Start(ctx); err != nil {
		return fmt.Errorf("start control bus: %w", err)
	}

	if err := b.Start(ctx); err != nil {
		return fmt.Errorf("start broker: %w", err)
	}

	if err := n.members.Start(ctx); err != nil {
		return fmt.Errorf("start members: %w", err)
	}

	return nil
}

// stopAll stops every node in the order the extension does: the broker, then
// Members, the control bus and the connection (which also stops the embedded
// server).
func stopAll(nodes []*node) {
	for _, n := range nodes {
		if n == nil {
			continue
		}

		if n.broker != nil {
			ctx, cancel := context.WithTimeout(context.Background(), stopWait)
			err := n.broker.Stop(ctx)

			cancel()

			if err != nil {
				fmt.Fprintf(os.Stderr, "interop: stop %s: %v\n", n.id, err)
			}
		}

		if n.members != nil {
			n.members.Stop()
		}

		if n.control != nil {
			n.control.Stop()
		}

		if n.conn != nil {
			n.conn.Close()
		}
	}
}
