package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// natsCommand is the hidden subcommand this binary runs as a NATS server, so
	// a cell needs nothing installed but Go and the tether binary.
	natsCommand = "nats"
	cellName    = "bench"
	startWait   = 90 * time.Second
)

// runNATS runs one nats-server with JetStream until stdin closes. The cell
// command holds stdin, so a server goes away with it however it ends, SIGKILL
// included. The broker processes have no such tie and outlive a killed cell.
func runNATS(args []string, stdout, stderr io.Writer) error {
	fs := newFlags(natsCommand, stderr)
	name := fs.String("name", "s1", "server name")
	port := fs.Int("port", 4222, "client port")
	cport := fs.Int("cluster-port", 0, "route port; 0 runs a single server")
	hport := fs.Int("http-port", 0, "monitoring port; 0 turns it off")
	routes := fs.String("routes", "", "routes to the other servers, comma separated")
	store := fs.String("store-dir", "", "JetStream store directory")

	if err := fs.Parse(args); err != nil {
		return err
	}

	opts := &server.Options{
		ServerName: *name, Host: "127.0.0.1", Port: *port, NoSigs: true, JetStream: true, StoreDir: *store,
		Logtime: true, HTTPHost: "127.0.0.1", HTTPPort: *hport,
	}

	if *cport != 0 {
		opts.Cluster = server.ClusterOpts{Name: cellName, Host: "127.0.0.1", Port: *cport}
		opts.Routes = server.RoutesFromStr(*routes)
	}

	s, err := server.NewServer(opts)
	if err != nil {
		return err
	}

	s.ConfigureLogger()

	go s.Start()

	if !s.ReadyForConnections(30 * time.Second) {
		return errors.New("nats server not ready")
	}

	fmt.Fprintln(stdout, "READY")

	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		fmt.Fprintln(stderr, "tether-bench: nats: stdin:", err)
	}

	s.Shutdown()
	s.WaitForShutdown()

	return nil
}

// child is one process of the cell.
type child struct {
	name  string
	cmd   *exec.Cmd
	stdin io.WriteCloser
	log   string
	done  chan struct{}
}

func (c *child) pid() int { return c.cmd.Process.Pid }

// stop ends the process: a server by closing its stdin, a broker with SIGTERM
// (it shuts down on it), and either with SIGKILL when that takes too long.
func (c *child) stop() {
	if c.stdin != nil {
		c.stdin.Close()
	} else {
		discard(c.cmd.Process.Signal(syscall.SIGTERM))
	}

	select {
	case <-c.done:
	case <-time.After(20 * time.Second):
		discard(c.cmd.Process.Kill())
		<-c.done
	}
}

// cellProcs are the processes of a running cell.
type cellProcs struct {
	dir     string
	servers []*child
	brokers []*child
	nats    []int // client ports
	mqtt    []string
	admin   []string // host:port of each node's HTTP listener
	closeMu sync.Once
}

func (cp *cellProcs) close() {
	cp.closeMu.Do(func() {
		var wg sync.WaitGroup

		for _, c := range cp.brokers {
			wg.Go(c.stop)
		}

		wg.Wait()

		for _, c := range cp.servers {
			wg.Go(c.stop)
		}

		wg.Wait()
	})
}

// freePorts reserves n loopback ports and releases them.
func freePorts(n int) ([]int, error) {
	var lc net.ListenConfig

	lns := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)

	defer func() {
		for _, ln := range lns {
			ln.Close()
		}
	}()

	for range n {
		ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}

		lns = append(lns, ln)

		addr, ok := ln.Addr().(*net.TCPAddr)
		if !ok {
			return nil, fmt.Errorf("listener address %v is not TCP", ln.Addr())
		}

		ports = append(ports, addr.Port)
	}

	return ports, nil
}

// launch starts a process, sends its output to a log file, and returns once it
// has started.
func launch(name, dir string, env []string, exe string, args ...string) (*child, io.ReadCloser, error) {
	logPath := filepath.Join(dir, name+".log")

	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}

	//nolint:gosec // G204: the cell command chose this executable and these arguments.
	cmd := exec.CommandContext(context.Background(), exe, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stderr = logf

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logf.Close()

		return nil, nil, err
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		logf.Close()

		return nil, nil, err
	}

	if err := cmd.Start(); err != nil {
		logf.Close()

		return nil, nil, err
	}

	c := &child{name: name, cmd: cmd, stdin: stdin, log: logPath, done: make(chan struct{})}

	go func() {
		discard(cmd.Wait())

		logf.Close()
		close(c.done)
	}()

	return c, stdout, nil
}

// startCell starts nNATS NATS servers (JetStream, one cluster) and nNodes brokers
// on loopback ports, and waits until every broker answers.
func startCell(ctx context.Context, dir, bin, token string, nNATS, nNodes int, logw io.Writer) (*cellProcs, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}

	cp := &cellProcs{dir: dir}

	ports, err := freePorts(3*nNATS + 2*nNodes)
	if err != nil {
		return nil, err
	}

	clientPorts, routePorts, httpPorts := ports[:nNATS], ports[nNATS:2*nNATS], ports[2*nNATS:3*nNATS]
	mqttPorts, adminPorts := ports[3*nNATS:3*nNATS+nNodes], ports[3*nNATS+nNodes:]
	cp.nats = clientPorts

	ok := false

	defer func() {
		if !ok {
			cp.close()
		}
	}()

	for i := range nNATS {
		var routes []string

		for j := range nNATS {
			if j != i && nNATS > 1 {
				routes = append(routes, "nats-route://127.0.0.1:"+strconv.Itoa(routePorts[j]))
			}
		}

		name := fmt.Sprintf("s%d", i+1)
		args := []string{
			natsCommand, "--name", name, "--port", strconv.Itoa(clientPorts[i]),
			"--http-port", strconv.Itoa(httpPorts[i]), "--store-dir", filepath.Join(dir, name),
		}

		if nNATS > 1 {
			args = append(args, "--cluster-port", strconv.Itoa(routePorts[i]), "--routes", strings.Join(routes, ","))
		}

		c, stdout, err := launch(name, dir, nil, self, args...)
		if err != nil {
			return nil, err
		}

		cp.servers = append(cp.servers, c)

		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err != nil || strings.TrimSpace(line) != "READY" {
			return nil, fmt.Errorf("%s did not start (see %s)", name, c.log)
		}
	}

	if err := waitJetStream(ctx, clientPorts[0]); err != nil {
		return nil, err
	}

	fmt.Fprintf(logw, "NATS: %d server(s) up, JetStream ready\n", nNATS)

	replicas := min(nNATS, 3)

	for i := range nNodes {
		id := fmt.Sprintf("n%d", i+1)
		mqtt := fmt.Sprintf("127.0.0.1:%d", mqttPorts[i])
		admin := fmt.Sprintf("127.0.0.1:%d", adminPorts[i])

		var urls []string

		for k := range nNATS {
			urls = append(urls, strconv.Quote("nats://127.0.0.1:"+strconv.Itoa(clientPorts[(i+k)%nNATS])))
		}

		cfg := fmt.Sprintf(`server:
  http_address: %q
  sqlite_path: %q
tether:
  broker:
    tcp_address: %q
    data_dir: %q
  cluster:
    enabled: true
    node_id: %q
    cell: %q
    mode: external
    urls: [%s]
    replicas: %d
`, admin, filepath.Join(dir, "tether.db"), mqtt, filepath.Join(dir, id, "sessions"), id, cellName,
			strings.Join(urls, ", "), replicas)

		path := filepath.Join(dir, id+".yaml")
		if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
			return nil, err
		}

		// The brokers share one SQLite file, and three processes switching a new
		// file to WAL mode at once fail with "database is locked". So one at a time.
		c, stdout, err := launch(id, dir, []string{tokenEnv + "=" + token}, bin, "--config", path)
		if err != nil {
			return nil, err
		}

		go drain(stdout)

		c.stdin.Close()
		c.stdin = nil

		cp.brokers = append(cp.brokers, c)
		cp.mqtt = append(cp.mqtt, mqtt)
		cp.admin = append(cp.admin, admin)

		if err := waitBroker(ctx, c, mqtt, admin); err != nil {
			return nil, err
		}

		fmt.Fprintf(logw, "node %s up: mqtt %s, admin %s, pid %d\n", id, mqtt, admin, c.pid())
	}

	ok = true

	return cp, nil
}

// waitJetStream waits until the servers have a JetStream meta leader.
func waitJetStream(ctx context.Context, port int) error {
	nc, err := nats.Connect("nats://127.0.0.1:"+strconv.Itoa(port), nats.Timeout(5*time.Second))
	if err != nil {
		return err
	}

	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	deadline := time.Now().Add(startWait)

	for time.Now().Before(deadline) {
		actx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = js.AccountInfo(actx)

		cancel()

		if err == nil {
			return nil
		}

		sleepUntil(ctx, time.Now().Add(200*time.Millisecond))
	}

	return fmt.Errorf("no JetStream meta leader after %s: %w", startWait, err)
}

// waitBroker waits until a broker answers /_/health and accepts MQTT connections.
func waitBroker(ctx context.Context, c *child, mqtt, admin string) error {
	deadline := time.Now().Add(startWait)

	for time.Now().Before(deadline) {
		select {
		case <-c.done:
			return fmt.Errorf("%s exited during start (see %s)", c.name, c.log)
		default:
		}

		if healthy(ctx, admin) && accepts(ctx, mqtt) {
			return nil
		}

		sleepUntil(ctx, time.Now().Add(250*time.Millisecond))
	}

	return fmt.Errorf("%s not ready after %s (see %s)", c.name, startWait, c.log)
}

// drain reads r to its end and throws it away.
func drain(r io.Reader) {
	_, err := io.Copy(io.Discard, r)
	discard(err)
}

func healthy(ctx context.Context, admin string) bool {
	hctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(hctx, http.MethodGet, "http://"+admin+"/_/health", http.NoBody)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}

	defer resp.Body.Close()

	drain(resp.Body)

	return resp.StatusCode == http.StatusOK
}

func accepts(ctx context.Context, addr string) bool {
	d := net.Dialer{Timeout: time.Second}

	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}

	conn.Close()

	return true
}

// cellDir makes the cell's directory: dir, or a new temp dir when dir is empty.
func cellDir(dir string) (string, error) {
	if dir == "" {
		return os.MkdirTemp("", "tether-bench-cell")
	}

	return dir, os.MkdirAll(dir, 0o700)
}

func runCell(args []string, stdout, stderr io.Writer) error {
	fs := newFlags("cell", stderr)
	bin := fs.String("bin", "", "the tether binary the nodes run (go build -o bin/tether ./cmd/tether)")
	dir := fs.String("dir", "", "where the cell keeps its data and logs (default: a new temp dir)")
	nodes := fs.Int("nodes", 3, "broker nodes")
	nNATS := fs.Int("nats", 3, "NATS servers (1, or 3 and up for a JetStream cluster)")
	devices := fs.Int("devices", 16, "devices to provision; the load commands spread their clients over them")
	out := fs.String("out", "", "where to write cell.json (default: <dir>/cell.json)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *bin == "" || *nodes < 1 || *nNATS < 1 || *nNATS == 2 || *devices < 1 {
		return errors.New("cell: --bin is required, --nodes and --devices at least 1, --nats 1 or at least 3")
	}

	abs, err := filepath.Abs(*bin)
	if err != nil {
		return err
	}

	if *dir, err = cellDir(*dir); err != nil {
		return err
	}

	if *out == "" {
		*out = filepath.Join(*dir, "cell.json")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	token := fmt.Sprintf("bench-%d", time.Now().UnixNano())

	cp, err := startCell(ctx, *dir, abs, token, *nNATS, *nodes, stdout)
	if err != nil {
		return err
	}

	defer cp.close()

	admins := make([]string, len(cp.admin))
	for i, a := range cp.admin {
		admins[i] = "http://" + a + "/tether/v1"
	}

	p, err := provision(ctx, newAdminClient(admins[0], token), *devices)
	if err != nil {
		return fmt.Errorf("provision: %w", err)
	}

	credsPath := filepath.Join(*dir, "creds.csv")

	if werr := writeCreds(credsPath, p.Creds); werr != nil {
		return werr
	}

	m := describeMachine(ctx)
	ci := cellInfo{
		Targets: cp.mqtt, Admins: admins, Token: token, Tenant: p.Tenant, Creds: credsPath,
		Dir: *dir, Binary: abs, Replicas: min(*nNATS, 3), Started: time.Now().Format(time.RFC3339), Machine: &m,
	}

	for _, c := range cp.brokers {
		ci.Brokers = append(ci.Brokers, c.pid())
	}

	for _, c := range cp.servers {
		ci.Servers = append(ci.Servers, c.pid())
	}

	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	if err := writeJSON(f, ci); err != nil {
		f.Close()

		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "cell ready: %d nodes, %d NATS servers, %d devices; info in %s\nCtrl-C stops it.\n",
		*nodes, *nNATS, *devices, *out)

	<-ctx.Done()

	fmt.Fprintln(stdout, "stopping the cell")

	return nil
}
