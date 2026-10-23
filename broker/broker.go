// Package broker runs Tether's multi-tenant MQTT broker on mochi-mqtt.
//
// One mochi server carries every tenant. Tether's hook authenticates each
// CONNECT, compiles the client's topic grants, and namespaces its client ID
// and every topic it sends with the tenant ID, stripping the prefix again on
// the way out. Two tenants can therefore pick the same client IDs and topic
// names and never see each other.
package broker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage/badger"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/limits"
)

// Deps are the broker's collaborators.
type Deps struct {
	// Auth turns CONNECT credentials into an identity. Required.
	Auth auth.Authenticator

	// Perms loads the identity's topic grants. Required.
	Perms PermLoader

	// Limits tracks per-tenant connection slots and message rates. Nil gets
	// a fresh limiter.
	Limits *limits.Limiter

	// Logger receives the broker's and mochi's logs. Nil discards them.
	Logger *slog.Logger

	// Metrics receives the node-wide counters. Nil reports nothing.
	Metrics Metrics

	// Cluster makes the broker one node of a cell. Nil runs it alone,
	// exactly as without clustering. With a cluster mochi's storage hook is
	// never added and Config.DataDir is ignored.
	Cluster *Cluster
}

// Broker is a tenant-isolating MQTT broker. Create it with New, then Start
// and Stop it once.
type Broker struct {
	cfg   Config
	deps  Deps
	log   *slog.Logger
	state *registry

	// aclObserver, when set before Start, sees every ACL check. Tests only.
	aclObserver func(clientID, topic string, write bool)

	// beforePurge, when set, runs in DeleteSession after the connection
	// has ended and before the session is purged. Tests only.
	beforePurge func()

	// endWait bounds how long DeleteSession waits for mochi to finish with
	// a connection (sessionEndWait). Tests shorten it.
	endWait time.Duration

	// sysInterval is how often tenant $SYS stats are published. Tests
	// shorten it before Start.
	sysInterval time.Duration

	// receiveMaximum, when set before Start, replaces mochi's default
	// Receive Maximum (1024) in the server's capabilities. Tests only.
	receiveMaximum uint16

	countersMu sync.Mutex
	counters   map[string]*tenantCounters

	// changes counts the key, role and tenant changes announced to the
	// broker (admin.go). A CONNECT compares it before and after it
	// registers its state to learn whether a change may have missed it.
	changes atomic.Uint64

	// Reload retries. A reload that fails on an
	// outage is retried after retryBase, doubling up to retryMax, for at
	// most retryWindow; then the client is disconnected with 0x88. Tests
	// shorten them.
	retryBase, retryMax, retryWindow time.Duration

	// retryMu guards retryClosed. Stop sets it and closes retryQuit, which
	// ends every retry loop, and waits for them on retries.
	retryMu        sync.Mutex
	retryClosed    bool
	retryQuit      chan struct{}
	retries        sync.WaitGroup
	pendingRetries atomic.Int64

	// storage is mochi's badger hook when DataDir is set. Pruning a session
	// tells it about the subscriptions and inflight messages it removes.
	storage *badger.Hook

	// cluster is the broker's side of its cell, nil when it runs alone.
	cluster *clusterNode

	mu      sync.Mutex
	started bool
	server  *mqtt.Server
	tcpAddr net.Addr
	tlsAddr net.Addr

	// sysStop stops the $SYS publisher, which closes sysDone when it returns.
	sysStop chan struct{}
	sysDone chan struct{}

	// listenerIDs are the mochi listeners setup added, for shutdown.
	listenerIDs []string
}

// New checks the config and dependencies and returns a broker that is not
// listening yet. Zero limits in cfg take their defaults.
func New(cfg Config, deps Deps) (*Broker, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}

	if deps.Auth == nil {
		return nil, invalid("deps.Auth is required")
	}

	if deps.Perms == nil {
		return nil, invalid("deps.Perms is required")
	}

	if deps.Limits == nil {
		deps.Limits = limits.New()
	}

	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}

	if deps.Metrics == nil {
		deps.Metrics = noMetrics{}
	}

	b := &Broker{
		cfg:         cfg,
		deps:        deps,
		log:         deps.Logger,
		state:       newRegistry(),
		sysInterval: defaultSysInterval,
		counters:    make(map[string]*tenantCounters),
		retryBase:   defaultRetryBase,
		retryMax:    defaultRetryMax,
		retryWindow: defaultRetryWindow,
		retryQuit:   make(chan struct{}),
		endWait:     sessionEndWait,
	}

	if deps.Cluster != nil {
		n, err := newClusterNode(deps.Cluster, deps.Logger, b.DeliverRouted)
		if err != nil {
			return nil, err
		}

		b.cluster = n
	}

	return b, nil
}

// Start opens the listeners and starts serving. A broker starts once; a
// second call fails.
func (b *Broker) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.started {
		return fmt.Errorf("tether/broker: %w: already started", tether.ErrConflict)
	}

	b.started = true

	caps := mqtt.NewDefaultServerCapabilities()
	caps.TopicAliasMaximum = 0
	caps.MaximumSessionExpiryInterval = b.cfg.sessionExpirySeconds()
	caps.MaximumPacketSize = b.cfg.MaxPacketSize
	caps.MaximumInflight = b.cfg.MaxInflight

	if b.receiveMaximum > 0 {
		caps.ReceiveMaximum = b.receiveMaximum
	}

	srv := mqtt.New(&mqtt.Options{
		InlineClient: true,
		Capabilities: caps,
		Logger:       b.log.With(slog.String("component", "mochi")),
	})

	if err := b.setup(ctx, srv); err != nil {
		_ = srv.Close()

		return err
	}

	b.server = srv

	// The injector exists before the first client can connect, so a log
	// reader started for that client has somewhere to deliver.
	if b.cluster != nil {
		b.cluster.attach(srv)
	}

	if err := srv.Serve(); err != nil {
		if b.cluster != nil {
			b.cluster.detach()
		}

		_ = srv.Close()
		b.server = nil

		return fmt.Errorf("tether/broker: serve: %w", err)
	}

	if b.cluster != nil {
		b.startReaderSweep()
	}

	interval := b.sysInterval
	if interval <= 0 {
		interval = defaultSysInterval
	}

	b.sysStop, b.sysDone = make(chan struct{}), make(chan struct{})
	go b.runSys(srv, interval, b.sysStop, b.sysDone)

	return nil
}

// setup adds the hooks and listeners to srv. The Tether hook goes first so
// every later hook, storage included, sees namespaced client IDs, and
// lastHook goes last so it sees a connection's end after storage did.
func (b *Broker) setup(ctx context.Context, srv *mqtt.Server) error {
	if err := srv.AddHook(&hook{b: b, server: srv}, nil); err != nil {
		return fmt.Errorf("tether/broker: add hook: %w", err)
	}

	switch {
	case b.cfg.DataDir != "" && b.cluster != nil:
		b.log.Info("tether: data_dir is ignored in cluster mode, sessions live in the cell", slog.String("data_dir", b.cfg.DataDir))
	case b.cfg.DataDir != "":
		storage := new(badger.Hook)
		if err := srv.AddHook(storage, &badger.Options{Path: b.cfg.DataDir}); err != nil {
			return fmt.Errorf("tether/broker: open storage: %w", err)
		}

		b.storage = storage
	}

	if err := srv.AddHook(&lastHook{b: b, server: srv}, nil); err != nil {
		return fmt.Errorf("tether/broker: add last hook: %w", err)
	}

	lc := &net.ListenConfig{}

	if b.cfg.TCPAddress != "" {
		ln, err := lc.Listen(ctx, "tcp", b.cfg.TCPAddress)
		if err != nil {
			return fmt.Errorf("tether/broker: listen tcp: %w", err)
		}

		if err := b.addListener(srv, listeners.NewNet("tcp", ln), ln); err != nil {
			return err
		}

		b.tcpAddr = ln.Addr()
	}

	if b.cfg.TLS != nil {
		cert, err := tls.LoadX509KeyPair(b.cfg.TLS.CertFile, b.cfg.TLS.KeyFile)
		if err != nil {
			return fmt.Errorf("tether/broker: load tls key pair: %w", err)
		}

		raw, err := lc.Listen(ctx, "tcp", b.cfg.TLS.Address)
		if err != nil {
			return fmt.Errorf("tether/broker: listen tls: %w", err)
		}

		ln := tls.NewListener(raw, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err := b.addListener(srv, listeners.NewNet("tls", ln), ln); err != nil {
			return err
		}

		b.tlsAddr = ln.Addr()
	}

	if b.cfg.WebSocketAddress != "" {
		ws := listeners.NewWebsocket(listeners.Config{Type: listeners.TypeWS, ID: "ws", Address: b.cfg.WebSocketAddress})
		if err := srv.AddListener(ws); err != nil {
			return fmt.Errorf("tether/broker: add websocket listener: %w", err)
		}

		b.listenerIDs = append(b.listenerIDs, ws.ID())
	}

	return nil
}

// addListener adds l to srv and records its ID for shutdown. When adding
// fails it closes ln, since mochi only closes listeners it holds.
func (b *Broker) addListener(srv *mqtt.Server, l listeners.Listener, ln net.Listener) error {
	if err := srv.AddListener(l); err != nil {
		_ = ln.Close()

		return fmt.Errorf("tether/broker: add %s listener: %w", l.ID(), err)
	}

	b.listenerIDs = append(b.listenerIDs, l.ID())

	return nil
}

// Stop closes the listeners, disconnects every client and closes storage. It
// returns ctx's error if ctx ends first; the shutdown then finishes in the
// background. Stopping a broker that never started, or twice, is a no-op.
func (b *Broker) Stop(ctx context.Context) error {
	b.mu.Lock()
	srv := b.server
	ids := b.listenerIDs
	sysStop, sysDone := b.sysStop, b.sysDone
	b.server = nil
	b.mu.Unlock()

	if srv == nil {
		return nil
	}

	done := make(chan error, 1)

	go func() {
		// The $SYS publisher goes first, so it never publishes into a
		// server that is closing.
		close(sysStop)
		<-sysDone

		// Pending reload retries end before the clients go.
		b.stopRetries()

		// No message from another node is injected into a server that is
		// closing. The Router closes last: the clean sessions that end
		// during the shutdown still release their routes.
		// The log readers stop with it; pending QoS 1 and 2 acks are
		// abandoned as their clients disconnect.
		if b.cluster != nil {
			b.cluster.detach()
			b.closeReaders()
		}

		err := shutdown(srv, ids)

		if b.cluster != nil {
			b.cluster.close()
		}

		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("tether/broker: stop: %w", err)
		}

		return nil
	case <-ctx.Done():
		return fmt.Errorf("tether/broker: stop: %w", ctx.Err())
	}
}

// shutdown drains srv before closing it.
//
// mochi v2.7.9's Server.Close can deadlock: Clients.GetByListener holds the
// clients read lock and takes it again through Len, so a client added or
// deleted in between (a client connecting or disconnecting during shutdown)
// queues a writer that blocks both forever. So the listeners stop accepting
// first, with a no-op closer, every connected client is disconnected, and
// Close runs only once all client goroutines have returned and nothing writes
// to the clients map any more.
func shutdown(srv *mqtt.Server, listenerIDs []string) error {
	for _, id := range listenerIDs {
		srv.Listeners.Close(id, func(string) {})
	}

	// In parallel: each disconnect may wait up to disconnectWait for a
	// client that does not read.
	var wg sync.WaitGroup

	for _, cl := range srv.Clients.GetAll() {
		if !cl.Net.Inline && !cl.Closed() {
			wg.Go(func() { disconnect(srv, nil, cl, packets.ErrServerShuttingDown) })
		}
	}

	wg.Wait()
	srv.Listeners.ClientsWg.Wait()

	return srv.Close()
}

// disconnectWait bounds how long disconnect waits for the DISCONNECT packet
// to be written before it closes the connection anyway.
const disconnectWait = time.Second

// disconnect sends cl a DISCONNECT with code and closes it, and returns
// within about disconnectWait whatever the client does.
//
// Mochi's DisconnectClient writes the DISCONNECT before it stops the client,
// and a write has no deadline of its own: a client that stops reading until
// its socket buffer fills parks the write (and mochi's outbound writer, which
// holds the client's lock) for as long as it keeps sending, since every
// packet it sends extends its keepalive deadline. A revoked client could
// then hold up its own disconnect and the admin call that ordered it. So the
// write runs in its own goroutine, and after disconnectWait the client is
// stopped regardless: Stop closes the connection, which fails the stuck
// write and lets that goroutine finish.
//
// Mochi returns the code itself as the error for every code from 0x80 up,
// so only a different error, a failed write, is logged (when log is not nil).
func disconnect(srv *mqtt.Server, log *slog.Logger, cl *mqtt.Client, code packets.Code) {
	done := make(chan error, 1)

	go func() { done <- srv.DisconnectClient(cl, code) }()

	timer := time.NewTimer(disconnectWait)
	defer timer.Stop()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, code) && log != nil {
			log.Debug("tether: disconnect write failed", slog.String("client", cl.ID), slog.Any("error", err))
		}
	case <-timer.C:
		if log != nil {
			log.Info("tether: client did not take its DISCONNECT, closing the connection", slog.String("client", cl.ID))
		}
	}

	cl.Stop(code)
}

// Running reports whether the broker is started and not yet stopped. The
// admin operations that need mochi (Clients, Kick, DeleteSession, Retained,
// DeleteRetained, Publish) fail with tether.ErrNotFound or tether.ErrConflict
// when it is false; callers that must tell "stopped" from "no such thing"
// check Running first.
func (b *Broker) Running() bool {
	return b.running() != nil
}

// Addr returns the TCP listener's address, or nil when there is none or the
// broker has not started.
func (b *Broker) Addr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.tcpAddr
}
