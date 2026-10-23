// Package clustertest starts in-process NATS servers with JetStream for tests
// of the cluster package and of the code built on it.
package clustertest

import (
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// ClusterName is the NATS cluster name of a cluster with more than one server.
const ClusterName = "tethertest"

// readyWait bounds how long Start waits for the servers and the JetStream meta
// group.
const readyWait = 30 * time.Second

// Cluster is a set of in-process servers.
type Cluster struct {
	// Servers are the running servers, in start order.
	Servers []*server.Server
}

// URLs returns each server's client URL.
func (c *Cluster) URLs() []string {
	out := make([]string, len(c.Servers))
	for i, s := range c.Servers {
		out[i] = s.ClientURL()
	}

	return out
}

// Stop shuts server i down and waits for it. Stopping a stopped server does
// nothing. The cleanup Start registered stops whatever is left.
func (c *Cluster) Stop(i int) {
	c.Servers[i].Shutdown()
	c.Servers[i].WaitForShutdown()
}

// Start runs an n-server JetStream cluster (a single plain server when n is 1)
// on random loopback ports, each with its own t.TempDir store, and returns the
// client URLs. It returns once every server is up and the JetStream meta group
// has a leader that knows all n peers. The servers are shut down when the test
// ends.
func Start(t testing.TB, n int) []string {
	t.Helper()

	return StartCluster(t, n).URLs()
}

// StartCluster is Start returning the servers too, for tests that stop one.
func StartCluster(t testing.TB, n int) *Cluster {
	t.Helper()

	if n < 1 {
		t.Fatalf("clustertest: need at least one server, got %d", n)
	}

	var routePorts []int
	if n > 1 {
		routePorts = FreePorts(t, n)
	}

	c := &Cluster{}

	for i := range n {
		opts := &server.Options{
			ServerName: fmt.Sprintf("s%d", i),
			Host:       "127.0.0.1",
			Port:       -1,
			NoLog:      true,
			NoSigs:     true,
			JetStream:  true,
			StoreDir:   t.TempDir(),
		}

		if n > 1 {
			opts.Cluster = server.ClusterOpts{Name: ClusterName, Host: "127.0.0.1", Port: routePorts[i]}

			for j, p := range routePorts {
				if j != i {
					opts.Routes = append(opts.Routes, mustRoute(t, p))
				}
			}
		}

		s, err := server.NewServer(opts)
		if err != nil {
			t.Fatalf("clustertest: new server %d: %v", i, err)
		}

		c.Servers = append(c.Servers, s)

		go s.Start()
	}

	// Registered after the temp dirs, so it runs before they are removed.
	t.Cleanup(func() {
		for i := range c.Servers {
			c.Stop(i)
		}
	})

	for i, s := range c.Servers {
		if !s.ReadyForConnections(readyWait) {
			t.Fatalf("clustertest: server %d not ready for connections", i)
		}
	}

	waitMeta(t, c)

	return c
}

// waitMeta waits for a JetStream meta leader that lists every server as a peer.
func waitMeta(t testing.TB, c *Cluster) {
	t.Helper()

	deadline := time.Now().Add(readyWait)

	for time.Now().Before(deadline) {
		for _, s := range c.Servers {
			if s.JetStreamEnabled() && (len(c.Servers) == 1 || s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == len(c.Servers)) {
				return
			}
		}

		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("clustertest: no JetStream meta leader with %d peers after %s", len(c.Servers), readyWait)
}

// FreePorts returns n distinct free loopback TCP ports. They are free when the
// call returns; another process could take one before you bind it, which is
// rare enough for tests.
func FreePorts(t testing.TB, n int) []int {
	t.Helper()

	listeners := make([]net.Listener, 0, n)

	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()

	ports := make([]int, 0, n)

	for range n {
		l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("clustertest: free port: %v", err)
		}

		listeners = append(listeners, l)
		addr, ok := l.Addr().(*net.TCPAddr)
		if !ok {
			t.Fatalf("clustertest: listener address is %T", l.Addr())
		}

		ports = append(ports, addr.Port)
	}

	return ports
}

// RouteURL is the route URL of a server whose cluster listener is on port.
func RouteURL(port int) string { return fmt.Sprintf("nats://127.0.0.1:%d", port) }

func mustRoute(t testing.TB, port int) *url.URL {
	t.Helper()

	u, err := url.Parse(RouteURL(port))
	if err != nil {
		t.Fatalf("clustertest: route url: %v", err)
	}

	return u
}
