package cluster

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

const defaultEmbeddedListen = "127.0.0.1:-1"

// startEmbedded starts an in-process nats-server with JetStream and waits
// until it takes connections. In a cluster the JetStream meta group may not
// have a leader yet; Connect waits for that.
func startEmbedded(cfg Config, wait time.Duration) (*server.Server, error) {
	e := cfg.Embedded

	name := e.ServerName
	if name == "" {
		name = cfg.NodeID
	}

	host, port, err := splitListen(e.Listen, defaultEmbeddedListen)
	if err != nil {
		return nil, fmt.Errorf("embedded.listen: %w", err)
	}

	opts := &server.Options{
		ServerName: name,
		Host:       host,
		Port:       port,
		NoLog:      true,
		NoSigs:     true,
		JetStream:  true,
		StoreDir:   e.StoreDir,
	}

	if e.ClusterName != "" {
		chost, cport, cerr := splitListen(e.ClusterListen, "")
		if cerr != nil {
			return nil, fmt.Errorf("embedded.cluster_listen: %w", cerr)
		}

		opts.Cluster = server.ClusterOpts{Name: e.ClusterName, Host: chost, Port: cport}

		for _, r := range e.Routes {
			u, perr := url.Parse(r)
			if perr != nil || u.Host == "" {
				return nil, fmt.Errorf("embedded.routes: bad route %q", r)
			}

			opts.Routes = append(opts.Routes, u)
		}
	}

	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("new server: %w", err)
	}

	go srv.Start()

	if !srv.ReadyForConnections(wait) {
		srv.Shutdown()
		srv.WaitForShutdown()

		return nil, errors.New("embedded server not ready for connections")
	}

	return srv, nil
}

// splitListen parses "host:port" where the port may be -1 (random). An empty
// addr falls back to def.
func splitListen(addr, def string) (host string, port int, err error) {
	if addr == "" {
		addr = def
	}

	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}

	port, err = strconv.Atoi(p)
	if err != nil {
		return "", 0, fmt.Errorf("port %q: %w", p, err)
	}

	return host, port, nil
}
