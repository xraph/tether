package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Each NATS server runs in its own child process (this binary, or the test
// binary, re-executed with TJS_CHILD set), the same shape as spike 3. A child
// exits when its stdin closes, so the servers die with the parent however it
// ends. Child config travels in environment variables so the same entry point
// works under `go test`, whose flag parsing would reject unknown flags.

const (
	envChild  = "TJS_CHILD"  // server name; set means "run one server and nothing else"
	envRoutes = "TJS_ROUTES" // comma separated route URLs
	envStore  = "TJS_STORE"  // JetStream store directory
	envPorts  = "TJS_PORTS"  // "client,cluster"
)

// MaybeRunChild runs a child server and exits if this process is one.
func MaybeRunChild() {
	name := os.Getenv(envChild)
	if name == "" {
		return
	}

	if err := runChild(name, os.Getenv(envRoutes), os.Getenv(envStore), os.Getenv(envPorts)); err != nil {
		fmt.Fprintln(os.Stderr, "child:", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func runChild(name, routes, store, ports string) error {
	var clientPort, clusterPort int
	if _, err := fmt.Sscanf(ports, "%d,%d", &clientPort, &clusterPort); err != nil {
		return fmt.Errorf("ports %q: %w", ports, err)
	}

	opts := &server.Options{
		ServerName: name,
		Host:       "127.0.0.1",
		Port:       clientPort,
		NoLog:      true,
		NoSigs:     true,
		JetStream:  true,
		StoreDir:   store,
		Cluster: server.ClusterOpts{
			Name: "spike",
			Host: "127.0.0.1",
			Port: clusterPort,
		},
	}
	if routes != "" {
		opts.Routes = server.RoutesFromStr(routes)
	}

	s, err := server.NewServer(opts)
	if err != nil {
		return err
	}

	go s.Start()

	if !s.ReadyForConnections(60 * time.Second) {
		return errors.New("server not ready")
	}

	fmt.Printf("READY %s\n", s.ClientURL())

	_, _ = io.Copy(io.Discard, os.Stdin)

	s.Shutdown()
	s.WaitForShutdown()

	return nil
}

type child struct {
	name      string
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	clientURL string
}

// Cluster is a 3-server JetStream cluster of child processes.
type Cluster struct {
	children []*child
	root     string
}

// URLs returns each server's client URL.
func (c *Cluster) URLs() []string {
	out := make([]string, len(c.children))
	for i, ch := range c.children {
		out[i] = ch.clientURL
	}

	return out
}

// freePorts asks the kernel for n free ports. JetStream clustering wants full
// routes between all servers, so every port is fixed before any child starts.
func freePorts(n int) ([]int, error) {
	out := make([]int, 0, n)

	var closers []io.Closer

	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()

	for range n {
		ln, err := listenLocal()
		if err != nil {
			return nil, err
		}

		closers = append(closers, ln)
		out = append(out, ln.Addr().(*net.TCPAddr).Port)
	}

	return out, nil
}

func listenLocal() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }

func startChild(name, routes, store string, clientPort, clusterPort int) (*child, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(self, "-test.run=^$")
	cmd.Env = append(os.Environ(),
		envChild+"="+name,
		envRoutes+"="+routes,
		envStore+"="+store,
		fmt.Sprintf("%s=%d,%d", envPorts, clientPort, clusterPort),
	)
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	c := &child{name: name, cmd: cmd, stdin: stdin}
	line := make(chan string, 1)

	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "READY ") {
				line <- sc.Text()
			}
		}
	}()

	select {
	case l := <-line:
		c.clientURL = strings.Fields(l)[1]
	case <-time.After(90 * time.Second):
		c.stop()

		return nil, fmt.Errorf("%s did not start", name)
	}

	return c, nil
}

func (c *child) stop() {
	_ = c.stdin.Close()

	done := make(chan struct{})

	go func() {
		_ = c.cmd.Wait()

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		_ = c.cmd.Process.Kill()

		<-done
	}
}

// StartCluster starts n JetStream servers with full routes, each with its own
// store directory under a fresh temp dir, and waits for a meta leader.
func StartCluster(n int) (*Cluster, error) {
	root, err := os.MkdirTemp("", "tether-js-spike-")
	if err != nil {
		return nil, err
	}

	cl := &Cluster{root: root}

	ports, err := freePorts(2 * n)
	if err != nil {
		cl.Stop()

		return nil, err
	}

	routes := make([]string, n)
	for i := range n {
		routes[i] = fmt.Sprintf("nats-route://127.0.0.1:%d", ports[n+i])
	}

	errs := make([]error, n)
	kids := make([]*child, n)

	var wg sync.WaitGroup

	for i := range n {
		wg.Go(func() {
			var others []string

			for j, r := range routes {
				if j != i {
					others = append(others, r)
				}
			}

			kids[i], errs[i] = startChild(fmt.Sprintf("n%d", i), strings.Join(others, ","),
				filepath.Join(root, fmt.Sprintf("n%d", i)), ports[i], ports[n+i])
		})
	}

	wg.Wait()

	for _, k := range kids {
		if k != nil {
			cl.children = append(cl.children, k)
		}
	}

	if err := errors.Join(errs...); err != nil {
		cl.Stop()

		return nil, err
	}

	if err := cl.waitMeta(120 * time.Second); err != nil {
		cl.Stop()

		return nil, err
	}

	return cl, nil
}

// waitMeta waits until every server answers a JetStream account info request,
// which needs an elected meta leader that knows about all three peers.
func (c *Cluster) waitMeta(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for _, u := range c.URLs() {
		nc, err := nats.Connect(u)
		if err != nil {
			return err
		}

		js, _ := jetstream.New(nc)

		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := js.AccountInfo(ctx)

			cancel()

			if err == nil {
				break
			}

			if time.Now().After(deadline) {
				nc.Close()

				return fmt.Errorf("jetstream not ready on %s: %w", u, err)
			}

			time.Sleep(200 * time.Millisecond)
		}

		nc.Close()
	}

	// A probe R3 stream proves the meta group can place replicas on 3 peers.
	nc, err := nats.Connect(c.URLs()[0])
	if err != nil {
		return err
	}
	defer nc.Close()

	js, _ := jetstream.New(nc)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "PROBE", Subjects: []string{"probe.>"}, Replicas: 3, Storage: jetstream.MemoryStorage})

		cancel()

		if err == nil {
			break
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("R3 probe stream: %w", err)
		}

		time.Sleep(500 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return js.DeleteStream(ctx, "PROBE")
}

// Stop stops every child and removes the JetStream store directories.
func (c *Cluster) Stop() {
	var wg sync.WaitGroup

	for _, ch := range c.children {
		wg.Go(ch.stop)
	}

	wg.Wait()

	if c.root != "" {
		_ = os.RemoveAll(c.root)
	}
}
