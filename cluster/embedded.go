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

// EmbeddedMaxPayload is the max_payload of an embedded server: room for a
// 1 MiB MQTT packet once its retained copy is base64 in JSON, or its log
// entry carries Tether's headers (nats-server's own default is 1 MiB).
const EmbeddedMaxPayload = 4 << 20

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
		Username:   e.User,
		Password:   e.Password,
		MaxPayload: EmbeddedMaxPayload,
	}

	if e.ClusterName != "" {
		chost, cport, cerr := splitListen(e.ClusterListen, "")
		if cerr != nil {
			return nil, fmt.Errorf("embedded.cluster_listen: %w", cerr)
		}

		opts.Cluster = server.ClusterOpts{
			Name: e.ClusterName, Host: chost, Port: cport,
			Username: e.User, Password: e.Password,
		}

		for _, r := range e.Routes {
			u, perr := url.Parse(r)
			if perr != nil || u.Host == "" {
				return nil, fmt.Errorf("embedded.routes: bad route %q", r)
			}

			// The other servers want the same pair this one does.
			if u.User == nil && e.User != "" {
				u.User = url.UserPassword(e.User, e.Password)
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
