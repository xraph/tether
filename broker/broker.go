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

	// sysInterval is how often tenant $SYS stats are published. Tests
	// shorten it before Start.
	sysInterval time.Duration

	countersMu sync.Mutex
	counters   map[string]*tenantCounters

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

	return &Broker{
		cfg:         cfg,
		deps:        deps,
		log:         deps.Logger,
		state:       newRegistry(),
		sysInterval: defaultSysInterval,
		counters:    make(map[string]*tenantCounters),
	}, nil
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

	if err := srv.Serve(); err != nil {
		_ = srv.Close()
		b.server = nil

		return fmt.Errorf("tether/broker: serve: %w", err)
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
// every later hook, storage included, sees namespaced client IDs.
func (b *Broker) setup(ctx context.Context, srv *mqtt.Server) error {
	if err := srv.AddHook(&hook{b: b, server: srv}, nil); err != nil {
		return fmt.Errorf("tether/broker: add hook: %w", err)
	}

	if b.cfg.DataDir != "" {
		if err := srv.AddHook(new(badger.Hook), &badger.Options{Path: b.cfg.DataDir}); err != nil {
			return fmt.Errorf("tether/broker: open storage: %w", err)
		}
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

		done <- shutdown(srv, ids)
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

	for _, cl := range srv.Clients.GetAll() {
		if !cl.Net.Inline && !cl.Closed() {
			disconnect(srv, nil, cl, packets.ErrServerShuttingDown)
		}
	}

	srv.Listeners.ClientsWg.Wait()

	return srv.Close()
}

// disconnect sends cl a DISCONNECT with code and closes it. Mochi returns the
// code itself as the error for every code from 0x80 up, so only a different
// error, a failed write, is logged (when log is not nil).
func disconnect(srv *mqtt.Server, log *slog.Logger, cl *mqtt.Client, code packets.Code) {
	err := srv.DisconnectClient(cl, code)
	if err != nil && !errors.Is(err, code) && log != nil {
		log.Debug("tether: disconnect write failed", slog.String("client", cl.ID), slog.Any("error", err))
	}
}

// Addr returns the TCP listener's address, or nil when there is none or the
// broker has not started.
func (b *Broker) Addr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.tcpAddr
}
