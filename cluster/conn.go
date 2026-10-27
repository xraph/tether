package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// connectWait bounds Connect when the context carries no deadline.
	connectWait = 30 * time.Second
	// probeEvery is how often Connect asks JetStream whether it is up yet.
	probeEvery = 100 * time.Millisecond
	// drainWait bounds the drain in Close.
	drainWait = 5 * time.Second
)

// Conn is a node's connection to the cell's NATS. It is safe for concurrent
// use.
//
// The connection is opened with nats.NoEcho: a node never receives the core
// NATS messages it publishes itself, even when it also subscribes to a
// matching subject. The router (a later task) relies on this to tell its own
// publishes from a peer's, so do not hand a Conn to code that needs the echo.
//
// A second connection, without NoEcho, carries the QoS 0 queue groups of
// shared subscriptions (ruling R5): a group member on the publishing node
// must be able to win the queue pick of that node's own publish. It is opened
// on first use (queueConn) and closed with the Conn.
type Conn struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	srv      *server.Server // nil in external mode
	logger   *slog.Logger
	closed   chan struct{} // closed by nats when the connection is closed
	replicas int
	once     sync.Once

	// addr and qopts open the queue connection: the main connection's
	// address and options, without NoEcho.
	addr  string
	qopts []nats.Option
	// qmu guards qnc and qdone; it is held while the queue connection is
	// dialled, which happens once.
	qmu   sync.Mutex
	qnc   *nats.Conn
	qdone chan struct{} // closed by nats when the queue connection is closed
	qshut bool          // Close ran: no queue connection opens after it
}

// Connect opens the node's connection. In embedded mode it first starts the
// in-process nats-server. It returns once JetStream answers, or fails with an
// error wrapping ErrUnavailable when ctx ends (or 30 s pass, when ctx has no
// deadline) before that. A node of an embedded cluster only gets JetStream
// once enough of its peers are up, so start the nodes together.
func Connect(ctx context.Context, cfg Config, logger *slog.Logger) (*Conn, error) {
	if !cfg.Enabled {
		return nil, errors.New("cluster: connect: clustering is not enabled")
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	logger = logger.With("component", "cluster", "node", cfg.NodeID, "cell", cfg.Cell)

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, connectWait)
		defer cancel()
	}

	c := &Conn{logger: logger, closed: make(chan struct{})}

	var (
		addr  string
		opts  = c.natsOptions(cfg)
		qopts = c.queueOptions(cfg)
	)

	if cfg.Mode == ModeEmbedded {
		srv, err := startEmbedded(cfg, remaining(ctx))
		if err != nil {
			return nil, fmt.Errorf("cluster: %w: %w", ErrUnavailable, err)
		}

		c.srv = srv
		addr = srv.ClientURL()

		opts = append(opts, nats.InProcessServer(srv))
		qopts = append(qopts, nats.InProcessServer(srv))

		if cfg.Embedded.User != "" {
			opts = append(opts, nats.UserInfo(cfg.Embedded.User, cfg.Embedded.Password))
			qopts = append(qopts, nats.UserInfo(cfg.Embedded.User, cfg.Embedded.Password))
		}
	} else {
		addr = strings.Join(cfg.URLs, ",")

		if cfg.Credentials != "" {
			opts = append(opts, nats.UserCredentials(cfg.Credentials))
			qopts = append(qopts, nats.UserCredentials(cfg.Credentials))
		}
	}

	c.addr, c.qopts = addr, qopts

	nc, err := nats.Connect(addr, opts...)
	if err != nil {
		c.shutdownServer()

		return nil, fmt.Errorf("cluster: %w: connect: %w", ErrUnavailable, err)
	}

	c.nc = nc

	js, err := jetstream.New(nc)
	if err != nil {
		c.Close()

		return nil, fmt.Errorf("cluster: jetstream: %w", err)
	}

	c.js = js

	if err := c.waitJetStream(ctx); err != nil {
		c.Close()

		return nil, err
	}

	c.replicas = replicasFor(cfg, nc)

	logger.Info("cluster connected", "mode", cfg.Mode, "replicas", c.replicas, "url", nc.ConnectedUrlRedacted())

	if cfg.Replicas == 0 && c.replicas < maxReplicas && clustered(cfg, nc) {
		// A NATS cluster that advertises nothing (no_advertise, or one URL
		// in front of a load balancer), or a seed-only route list, counts
		// fewer servers than it has. Provision never lowers what a stream
		// already has, but a fresh cell would get this count.
		logger.Warn("cluster: automatic replica count is below 3 on a clustered nats; set cluster.replicas if the cluster has more servers",
			"replicas", c.replicas, "discovered", len(nc.DiscoveredServers()))
	}

	return c, nil
}

func (c *Conn) natsOptions(cfg Config) []nats.Option {
	opts := []nats.Option{
		nats.Name("tether-" + cfg.NodeID),
		nats.NoEcho(),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			c.logger.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.logger.Info("nats reconnected", "url", nc.ConnectedUrlRedacted())
		}),
		nats.ClosedHandler(func(*nats.Conn) { close(c.closed) }),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			c.logger.Warn("nats error", "error", err)
		}),
	}

	if cfg.NoDiscovery {
		opts = append(opts, nats.IgnoreDiscoveredServers(), nats.DontRandomize())
	}

	return opts
}

// queueOptions are the options of the queue connection: the main
// connection's, without NoEcho and with handlers of its own.
func (c *Conn) queueOptions(cfg Config) []nats.Option {
	opts := []nats.Option{
		nats.Name("tether-" + cfg.NodeID + "-shared"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			c.logger.Warn("nats queue connection disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.logger.Info("nats queue connection reconnected", "url", nc.ConnectedUrlRedacted())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			c.logger.Warn("nats queue connection error", "error", err)
		}),
	}

	if cfg.NoDiscovery {
		opts = append(opts, nats.IgnoreDiscoveredServers(), nats.DontRandomize())
	}

	return opts
}

// queueConn returns the queue connection, opening it on first use. It fails
// with an error wrapping ErrUnavailable when it cannot be opened, or after
// Close.
func (c *Conn) queueConn() (*nats.Conn, error) {
	c.qmu.Lock()
	defer c.qmu.Unlock()

	if c.qnc != nil {
		return c.qnc, nil
	}

	if c.qshut || c.nc.IsClosed() {
		return nil, fmt.Errorf("cluster: queue connection: %w: connection closed", ErrUnavailable)
	}

	done := make(chan struct{})
	opts := append(append([]nats.Option{}, c.qopts...), nats.ClosedHandler(func(*nats.Conn) { close(done) }))

	nc, err := nats.Connect(c.addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("cluster: %w: queue connection: %w", ErrUnavailable, err)
	}

	c.qnc, c.qdone = nc, done

	return nc, nil
}

// waitJetStream polls until JetStream answers an account info request.
func (c *Conn) waitJetStream(ctx context.Context) error {
	t := time.NewTicker(probeEvery)
	defer t.Stop()

	var last error

	for {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := c.js.AccountInfo(pctx)

		cancel()

		if err == nil {
			return nil
		}

		last = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("cluster: %w: jetstream not ready: %w", ErrUnavailable, last)
		case <-t.C:
		}
	}
}

// replicasFor decides how many replicas the cell's streams and buckets get.
//
// An explicit Config.Replicas wins. Otherwise the answer is min(3, servers):
// in embedded mode the servers are this one plus its routes, and in external
// mode they are the servers the client knows about (the URLs it was given,
// or the one it connected to plus the ones the cluster advertised), or one
// when the server reports no cluster name. Both can count too few: a server
// that advertises nothing (no_advertise, or one URL in front of a load
// balancer) makes a cluster of any size count as one, and an embedded node
// given only a seed route counts two. Connect warns when a clustered NATS
// counts under three; set Replicas then. Provision never lowers the count of
// a stream that already has more.
func replicasFor(cfg Config, nc *nats.Conn) int {
	if cfg.Replicas > 0 {
		return cfg.Replicas
	}

	n := 1

	switch {
	case cfg.Mode == ModeEmbedded:
		n = 1 + len(cfg.Embedded.Routes)
	case nc.ConnectedClusterName() != "":
		n = max(len(cfg.URLs), 1+len(nc.DiscoveredServers()))
	}

	return max(1, min(maxReplicas, n))
}

// clustered reports whether the node's NATS is a cluster: an embedded
// server with a cluster name, or a server that reports one.
func clustered(cfg Config, nc *nats.Conn) bool {
	if cfg.Mode == ModeEmbedded {
		return cfg.Embedded.ClusterName != ""
	}

	return nc.ConnectedClusterName() != ""
}

// remaining is the time left on ctx, or connectWait without a deadline.
func remaining(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		return max(time.Until(dl), time.Second)
	}

	return connectWait
}

// JS returns the JetStream handle.
func (c *Conn) JS() jetstream.JetStream { return c.js }

// NC returns the NATS connection, for core publish and subscribe.
func (c *Conn) NC() *nats.Conn { return c.nc }

// Replicas is the replica count Provision gives the cell's streams and
// buckets.
func (c *Conn) Replicas() int { return c.replicas }

// MaxPayload is the largest message, headers included, the NATS server the
// connection is on takes (its max_payload); 0 before the connection has
// heard from a server. A write over it fails with an error wrapping
// ErrTooLarge. Embedded mode raises the server's default 1 MiB to
// EmbeddedMaxPayload.
func (c *Conn) MaxPayload() int64 { return c.nc.MaxPayload() }

// Healthy reports whether the connection is up and JetStream answers. The
// error wraps ErrUnavailable.
func (c *Conn) Healthy(ctx context.Context) error {
	if !c.nc.IsConnected() {
		return fmt.Errorf("cluster: %w: nats connection is %s", ErrUnavailable, c.nc.Status())
	}

	if _, err := c.js.AccountInfo(ctx); err != nil {
		return fmt.Errorf("cluster: %w: %w", ErrUnavailable, err)
	}

	return nil
}

// Close drains the connection and, in embedded mode, shuts the server down. It
// is safe to call more than once.
func (c *Conn) Close() {
	c.once.Do(func() {
		c.closeQueue()

		if c.nc != nil {
			c.drain()
		}

		c.shutdownServer()
	})
}

// closeQueue drains the queue connection, if one was opened, and keeps a
// later queueConn from opening one.
func (c *Conn) closeQueue() {
	c.qmu.Lock()
	nc, done := c.qnc, c.qdone
	c.qshut = true
	c.qmu.Unlock()

	if nc == nil {
		return
	}

	if err := nc.Drain(); err != nil {
		nc.Close()

		return
	}

	select {
	case <-done:
	case <-time.After(drainWait):
		nc.Close()
	}
}

func (c *Conn) drain() {
	if err := c.nc.Drain(); err != nil {
		c.nc.Close()

		return
	}

	select {
	case <-c.closed:
	case <-time.After(drainWait):
		c.nc.Close()
	}
}

func (c *Conn) shutdownServer() {
	if c.srv == nil {
		return
	}

	c.srv.Shutdown()
	c.srv.WaitForShutdown()
}
