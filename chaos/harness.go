//go:build chaos

// Package chaos is Tether's chaos suite. It runs a cell as real processes on
// one host (three nats-server processes and three tether broker processes),
// puts a fault proxy from internal/chaos in front of every broker's NATS
// connections and every NATS route, and checks the cell's guarantees while it
// kills, cuts and slows them. The scenarios live in chaos_test.go behind the
// chaos build tag, and README.md says how to run them.
//
// This file is the harness: the processes, the faults, an observer that reads
// the cell's session and member buckets and its log straight from NATS, and
// MQTT 5 publishers and subscribers that record what they sent, what was
// acknowledged and what arrived.
package chaos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether/cluster"
	faults "github.com/xraph/tether/internal/chaos"
	"github.com/xraph/tether/subject"
)

// The cell's timings. They are short so a run takes minutes; README.md
// records them next to the results. A scenario can ask for others
// (withTimings), as the NATS server kill does for the production defaults.
const (
	heartbeat = 500 * time.Millisecond
	lease     = 3 * time.Second
	// deadMargin is how long past the lease a peer must stay silent before the
	// cell calls it dead (cluster's peerDeadMargin).
	deadMargin = 2 * time.Second
	// electionSlack is how long after the cell's new log leader shows up
	// publishing may take to resume: a pull of stream info by the observer
	// and an append's round trip.
	electionSlack = time.Second
	// resumeBound is the spec's bound on a client resuming its session on
	// another node, counted from its reconnect.
	resumeBound = 10 * time.Second

	cellName   = "chaos"
	natsSize   = 3
	brokerSize = 3

	// sessionExpiry is the session expiry persistent test clients ask for.
	sessionExpiry = 600

	childNATSEnv   = "TETHER_CHAOS_NATS"
	childBrokerEnv = "TETHER_CHAOS_BROKER"
	// keepEnv, when set, keeps every cell's data and logs after the run.
	keepEnv = "TETHER_CHAOS_KEEP"

	startWait = 90 * time.Second
)

// cellConfig is the cluster section every broker shares, for the names of
// the cell's streams and buckets.
func cellConfig() cluster.Config {
	c := cluster.DefaultConfig()
	c.Cell = cellName

	return c
}

// ---------------------------------------------------------------------------
// Child processes
// ---------------------------------------------------------------------------

// natsSpec is what a nats-server child needs, passed as JSON in
// TETHER_CHAOS_NATS.
type natsSpec struct {
	Name        string   `json:"name"`
	Port        int      `json:"port"`
	ClusterPort int      `json:"cluster_port"`
	Advertise   string   `json:"advertise"`
	Routes      []string `json:"routes"`
	StoreDir    string   `json:"store_dir"`
	HTTPPort    int      `json:"http_port"`
}

// childMain runs this test binary as a child process when the environment
// asks for one: a nats-server, or the supervisor of one broker. It says
// whether it ran one, and the exit code.
func childMain() (ran bool, code int) {
	if raw := os.Getenv(childNATSEnv); raw != "" {
		if err := serveNATS(raw); err != nil {
			fmt.Fprintln(os.Stderr, "chaos: nats child:", err)

			return true, 1
		}

		return true, 0
	}

	if bin := os.Getenv(childBrokerEnv); bin != "" {
		return true, superviseBroker(bin, os.Args[1:])
	}

	return false, 0
}

// serveNATS runs one nats-server with JetStream until stdin closes. The parent
// holds stdin, so the server goes away with the test binary however that ends.
func serveNATS(raw string) error {
	var spec natsSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return err
	}

	opts := &server.Options{
		ServerName: spec.Name,
		Host:       "127.0.0.1",
		Port:       spec.Port,
		NoSigs:     true,
		JetStream:  true,
		StoreDir:   spec.StoreDir,
		Logtime:    true,
		// Short pings, so a route or a client behind a black hole is noticed
		// in seconds rather than minutes.
		PingInterval: time.Second,
		MaxPingsOut:  3,
		// The monitoring endpoints (/jsz, /connz), for looking into a
		// failed run.
		HTTPHost: "127.0.0.1",
		HTTPPort: spec.HTTPPort,
		Cluster: server.ClusterOpts{
			Name: cellName,
			Host: "127.0.0.1",
			Port: spec.ClusterPort,
			// Advertise an address nothing listens on, so no server routes
			// to another around the route proxies: every route is one the
			// harness configured, through a proxy it controls.
			Advertise: spec.Advertise,
		},
	}

	if len(spec.Routes) > 0 {
		opts.Routes = server.RoutesFromStr(strings.Join(spec.Routes, ","))
	}

	s, err := server.NewServer(opts)
	if err != nil {
		return err
	}

	s.ConfigureLogger()

	go s.Start()

	if !s.ReadyForConnections(30 * time.Second) {
		return errors.New("server not ready")
	}

	fmt.Println("READY")

	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		fmt.Fprintln(os.Stderr, "chaos: nats child: stdin:", err)
	}

	s.Shutdown()
	s.WaitForShutdown()

	return nil
}

// superviseBroker starts the tether binary with args, prints its process ID
// on stdout, and kills it when stdin closes, so a broker never outlives the
// test binary. It exits with the broker's exit code.
func superviseBroker(bin string, args []string) int {
	//nolint:gosec // G204: the harness built this binary and chose the arguments.
	cmd := exec.CommandContext(context.Background(), bin, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "chaos: supervisor:", err)

		return 1
	}

	fmt.Printf("PID %d\n", cmd.Process.Pid)

	go func() {
		if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
			fmt.Fprintln(os.Stderr, "chaos: supervisor: stdin:", err)
		}

		discard(cmd.Process.Kill())
	}()

	if err := cmd.Wait(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}

		return 1
	}

	return 0
}

// proc is one child process of the test binary.
type proc struct {
	name    string
	logPath string
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	// pid is the process a kill -9 hits: the nats-server child itself, or the
	// broker under its supervisor.
	pid  int
	done chan struct{}
}

// startProc re-runs the test binary with env added, sends its stderr to
// logPath, and waits for the first line on its stdout, which it returns.
func startProc(name, logPath string, env []string, args ...string) (*proc, string, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, "", err
	}

	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, "", err
	}

	//nolint:gosec // G204: this is the test binary itself, re-run as a child.
	cmd := exec.CommandContext(context.Background(), self, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stderr = logf

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logf.Close()

		return nil, "", err
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		logf.Close()

		return nil, "", err
	}

	if err := cmd.Start(); err != nil {
		logf.Close()

		return nil, "", err
	}

	p := &proc{name: name, logPath: logPath, cmd: cmd, stdin: stdin, pid: cmd.Process.Pid, done: make(chan struct{})}

	lines := make(chan string, 1)

	go func() {
		r := bufio.NewReader(stdout)

		line, rerr := r.ReadString('\n')
		if rerr == nil {
			lines <- strings.TrimSpace(line)
		}

		close(lines)

		// Keep draining, so a child that prints more never blocks.
		if _, err := io.Copy(logf, r); err != nil {
			discard(err)
		}
	}()

	go func() {
		discard(cmd.Wait())
		logf.Close()
		close(p.done)
	}()

	select {
	case line, ok := <-lines:
		if !ok {
			p.kill()

			return nil, "", fmt.Errorf("%s exited before it was ready (see %s)", name, logPath)
		}

		return p, line, nil
	case <-time.After(startWait):
		p.kill()

		return nil, "", fmt.Errorf("%s not ready after %s (see %s)", name, startWait, logPath)
	}
}

// kill sends SIGKILL to the process and waits for the child to go.
func (p *proc) kill() {
	if pr, err := os.FindProcess(p.pid); err == nil {
		discard(pr.Kill())
	}

	if p.pid != p.cmd.Process.Pid {
		// The supervisor exits once its broker is gone; make sure.
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			discard(p.cmd.Process.Kill())
		}
	}

	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
	}
}

// stop closes stdin, which ends a nats-server child cleanly and has a
// supervisor kill its broker, then waits.
func (p *proc) stop() {
	p.stdin.Close()

	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		p.kill()
	}
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// The cell
// ---------------------------------------------------------------------------

// natsServer is one nats-server child.
type natsServer struct {
	*proc

	name        string
	port        int
	clusterPort int
	httpPort    int // monitoring
}

// node is one tether broker child.
type node struct {
	*proc

	id   string
	mqtt string // MQTT listener, host:port
	http string // admin API and health, host:port
	// proxies[j] carries this node's NATS connections to server j. The
	// node's URLs list its own server (index = node index) first.
	proxies [natsSize]*faults.Proxy
}

// testCell is three NATS servers and three brokers.
type testCell struct {
	t      *testing.T
	dir    string
	bin    string
	token  string
	tenant string

	// heartbeat and lease are the brokers' cluster timings.
	heartbeat, lease time.Duration

	servers [natsSize]*natsServer
	// routes[i][j] carries server i's route to server j.
	routes [natsSize][natsSize]*faults.Proxy
	nodes  [brokerSize]*node
	obs    *observer
}

var (
	buildOnce sync.Once
	builtBin  string
	errBuild  error
)

// removeBinary deletes the directory tetherBinary built into, for TestMain.
func removeBinary() {
	if builtBin != "" {
		discard(os.RemoveAll(filepath.Dir(builtBin)))
	}
}

// tetherBinary builds cmd/tether once per test binary.
func tetherBinary(t *testing.T) string {
	t.Helper()

	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "tether-chaos-bin")
		if err != nil {
			errBuild = err

			return
		}

		builtBin = filepath.Join(dir, "tether-chaos")

		//nolint:gosec // G204: the output path is a temp dir this function made.
		cmd := exec.CommandContext(context.Background(), "go", "build", "-o", builtBin, "../cmd/tether")
		cmd.Env = append(os.Environ(), "GOWORK=off")

		out, err := cmd.CombinedOutput()
		if err != nil {
			errBuild = fmt.Errorf("build cmd/tether: %w\n%s", err, out)
		}
	})

	if errBuild != nil {
		t.Fatal(errBuild)
	}

	return builtBin
}

// freePorts reserves n loopback ports and releases them.
func freePorts(t *testing.T, n int) []int {
	t.Helper()

	var lc net.ListenConfig

	out := make([]int, 0, n)
	lns := make([]net.Listener, 0, n)

	for range n {
		ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		lns = append(lns, ln)

		addr, ok := ln.Addr().(*net.TCPAddr)
		if !ok {
			t.Fatalf("listener address %v is not TCP", ln.Addr())
		}

		out = append(out, addr.Port)
	}

	for _, ln := range lns {
		ln.Close()
	}

	return out
}

func newProxy(t *testing.T, target string) *faults.Proxy {
	t.Helper()

	p, err := faults.NewProxy(target)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(p.Close)

	return p
}

// cellOption changes a cell before it starts.
type cellOption func(*testCell)

// withTimings runs the brokers with the given heartbeat and lease instead of
// the chaos ones.
func withTimings(hb, l time.Duration) cellOption {
	return func(c *testCell) { c.heartbeat, c.lease = hb, l }
}

// startCell starts three NATS servers routed to each other through proxies,
// then three brokers one after another (they share one SQLite file), each
// connected to NATS through its own proxies, and makes a tenant with a role
// that may publish and subscribe under chaos/#.
func startCell(t *testing.T, opts ...cellOption) *testCell {
	t.Helper()

	c := &testCell{
		t:         t,
		dir:       cellDir(t),
		bin:       tetherBinary(t),
		token:     fmt.Sprintf("chaos-%d", time.Now().UnixNano()),
		heartbeat: heartbeat,
		lease:     lease,
	}

	for _, o := range opts {
		o(c)
	}

	// Cleanups run last in, first out. The log dump goes first so it runs
	// last, from the files, whatever stage a failure stopped at. The servers'
	// live state is dumped from a cleanup startServers registers, which runs
	// while they are still up. Brokers go before servers, and both before the
	// proxies (registered first, so they close last).
	t.Cleanup(func() {
		if t.Failed() {
			c.dumpLogs()
		}
	})

	c.startServers()
	c.obs = newObserver(t, c.serverURLs())
	c.waitJetStream()
	c.startBrokers()
	c.obs.watch(t)
	c.waitMembers()
	c.makeTenant()

	return c
}

// cellDir is the directory for the cell's data and logs: a test temp dir, or
// one that outlives the run when TETHER_CHAOS_KEEP is set.
func cellDir(t *testing.T) string {
	t.Helper()

	if os.Getenv(keepEnv) == "" {
		return t.TempDir()
	}

	dir, err := os.MkdirTemp("", "tether-chaos-"+t.Name()+"-")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("cell data and logs kept in %s", dir)

	return dir
}

func (c *testCell) serverURLs() []string {
	out := make([]string, 0, natsSize)
	for _, s := range c.servers {
		out = append(out, fmt.Sprintf("nats://127.0.0.1:%d", s.port))
	}

	return out
}

func (c *testCell) startServers() {
	t := c.t
	ports := freePorts(t, 3*natsSize+1)
	dead := fmt.Sprintf("127.0.0.1:%d", ports[3*natsSize])

	for i := range natsSize {
		for j := range natsSize {
			if i != j {
				c.routes[i][j] = newProxy(t, fmt.Sprintf("127.0.0.1:%d", ports[natsSize+j]))
			}
		}
	}

	for i := range natsSize {
		c.servers[i] = &natsServer{name: fmt.Sprintf("s%d", i+1), port: ports[i], clusterPort: ports[natsSize+i], httpPort: ports[2*natsSize+i]}
	}

	for i := range natsSize {
		c.launchServer(i, dead)
	}

	// Registered after every server's stop, so it runs before them.
	c.t.Cleanup(func() {
		if c.t.Failed() {
			c.dumpState()
		}
	})
}

// launchServer starts server i (again, after a kill) with its routes through
// the route proxies.
func (c *testCell) launchServer(i int, dead string) {
	t := c.t
	s := c.servers[i]

	var routes []string

	for j := range natsSize {
		if j != i {
			routes = append(routes, "nats-route://"+c.routes[i][j].Addr())
		}
	}

	spec, err := json.Marshal(natsSpec{
		Name:        s.name,
		Port:        s.port,
		ClusterPort: s.clusterPort,
		Advertise:   dead,
		Routes:      routes,
		StoreDir:    filepath.Join(c.dir, s.name),
		HTTPPort:    s.httpPort,
	})
	if err != nil {
		t.Fatal(err)
	}

	p, line, err := startProc(s.name, filepath.Join(c.dir, s.name+".log"), []string{childNATSEnv + "=" + string(spec)})
	if err != nil {
		t.Fatal(err)
	}

	if line != "READY" {
		p.kill()
		t.Fatalf("%s said %q, not READY", s.name, line)
	}

	s.proc = p

	t.Cleanup(p.stop)
}

// waitJetStream waits until the three servers have a JetStream meta leader.
func (c *testCell) waitJetStream() {
	c.t.Helper()

	eventually(c.t, startWait, "JetStream meta leader", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.obs.js.AccountInfo(ctx)

		return err == nil
	})
}

func (c *testCell) startBrokers() {
	t := c.t
	ports := freePorts(t, 2*brokerSize)

	for i := range brokerSize {
		n := &node{
			id:   fmt.Sprintf("n%d", i+1),
			mqtt: fmt.Sprintf("127.0.0.1:%d", ports[i]),
			http: fmt.Sprintf("127.0.0.1:%d", ports[brokerSize+i]),
		}

		for j := range natsSize {
			n.proxies[j] = newProxy(t, fmt.Sprintf("127.0.0.1:%d", c.servers[j].port))
		}

		c.nodes[i] = n
	}

	// One at a time: they share one SQLite file, and three processes that
	// switch a new file to WAL mode at once fail with "database is locked".
	for i := range brokerSize {
		c.launchBroker(i)
	}
}

// brokerConfig is the YAML config of node i.
func (c *testCell) brokerConfig(i int) string {
	n := c.nodes[i]

	// The node's own server first. With no_discovery the node only ever
	// uses these URLs, in this order, so it cannot reconnect around a proxy.
	urls := []string{"nats://" + n.proxies[i].Addr()}

	for j := range natsSize {
		if j != i {
			urls = append(urls, "nats://"+n.proxies[j].Addr())
		}
	}

	quoted := make([]string, len(urls))
	for k, u := range urls {
		quoted[k] = strconv.Quote(u)
	}

	return fmt.Sprintf(`server:
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
    heartbeat: %s
    lease: %s
    load_interval: %s
    replicas: 3
    no_discovery: true
`, n.http, filepath.Join(c.dir, "tether.db"), n.mqtt, filepath.Join(c.dir, n.id, "sessions"),
		n.id, cellName, strings.Join(quoted, ", "), c.heartbeat, c.lease, min(cluster.DefaultLoadInterval, c.lease/3))
}

func (c *testCell) launchBroker(i int) {
	t := c.t
	n := c.nodes[i]

	path := filepath.Join(c.dir, n.id+".yaml")
	if err := os.WriteFile(path, []byte(c.brokerConfig(i)), 0o600); err != nil {
		t.Fatal(err)
	}

	p, line, err := startProc(n.id, filepath.Join(c.dir, n.id+".log"),
		[]string{childBrokerEnv + "=" + c.bin, "TETHER_ADMIN_TOKEN=" + c.token},
		"--config", path)
	if err != nil {
		t.Fatal(err)
	}

	pid, ok := strings.CutPrefix(line, "PID ")
	if !ok {
		p.kill()
		t.Fatalf("%s supervisor said %q", n.id, line)
	}

	p.pid, err = strconv.Atoi(pid)
	if err != nil {
		p.kill()
		t.Fatal(err)
	}

	n.proc = p

	t.Cleanup(p.stop)

	eventually(t, startWait, n.id+" healthy", func() bool {
		if p.exited() {
			t.Fatalf("%s exited during start; log %s:\n%s", n.id, p.logPath, tail(p.logPath, 40))
		}

		return c.healthy(i) && c.mqttUp(i)
	})
}

// mqttUp reports whether node i accepts TCP connections on its MQTT port.
func (c *testCell) mqttUp(i int) bool {
	d := net.Dialer{Timeout: time.Second}

	conn, err := d.DialContext(context.Background(), "tcp", c.nodes[i].mqtt)
	if err != nil {
		return false
	}

	conn.Close()

	return true
}

// waitBeat waits until node i has written a heartbeat after t0, which means
// it can reach JetStream again and has unfenced, and returns when the
// observer saw it.
func (c *testCell) waitBeat(i int, t0 time.Time, d time.Duration) time.Time {
	c.t.Helper()

	id := c.nodes[i].id

	var seen time.Time

	eventually(c.t, d, id+" heartbeating again", func() bool {
		b, ok := c.obs.lastBeat(id)
		if ok && b.stamp.After(t0) {
			seen = b.seen

			return true
		}

		return false
	})

	return seen
}

// healthy reports whether node i answers /_/health with 200. Forge answers
// 200 through a startup grace period and from a cached report, so this says
// the process is up, not that it is unfenced: waitBeat says that.
func (c *testCell) healthy(i int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+c.nodes[i].http+"/_/health", http.NoBody)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}

	defer resp.Body.Close()

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return false
	}

	return resp.StatusCode == http.StatusOK
}

// waitMembers waits until the observer has seen every node's heartbeat.
func (c *testCell) waitMembers() {
	eventually(c.t, startWait, "every node heartbeating", func() bool {
		for _, n := range c.nodes {
			if _, ok := c.obs.lastBeat(n.id); !ok {
				return false
			}
		}

		return true
	})
}

// admin sends an admin API request to the first healthy node and decodes the
// reply into out.
func (c *testCell) admin(method, path string, body, out any) {
	t := c.t
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}

	for i := range brokerSize {
		if c.nodes[i].exited() {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		req, err := http.NewRequestWithContext(ctx, method, "http://"+c.nodes[i].http+"/tether/v1"+path, bytes.NewReader(raw))
		if err != nil {
			cancel()
			t.Fatal(err)
		}

		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()

			continue
		}

		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()

		if err != nil {
			t.Fatal(err)
		}

		if resp.StatusCode/100 != 2 {
			t.Fatalf("%s %s on %s: %d %s", method, path, c.nodes[i].id, resp.StatusCode, data)
		}

		if out != nil {
			if err := json.Unmarshal(data, out); err != nil {
				t.Fatalf("%s %s: decode %s: %v", method, path, data, err)
			}
		}

		return
	}

	t.Fatalf("%s %s: no node answered", method, path)
}

func (c *testCell) makeTenant() {
	var tn struct {
		ID string `json:"id"`
	}

	c.admin(http.MethodPost, "/tenants", map[string]any{
		"app_id": "aapp_chaos", "external_id": "chaos", "name": "Chaos",
	}, &tn)

	c.tenant = tn.ID

	c.admin(http.MethodPost, "/tenants/"+c.tenant+"/roles", map[string]any{
		"slug": "rw", "publish": []string{"chaos/#"}, "subscribe": []string{"chaos/#"},
	}, nil)
}

// creds are one device's MQTT username and password.
type creds struct{ user, pass string }

// device makes a device pinned to clientID with the rw role.
func (c *testCell) device(clientID string) creds {
	var d struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	c.admin(http.MethodPost, "/tenants/"+c.tenant+"/devices", map[string]any{
		"name": clientID, "client_id": clientID, "roles": []string{"rw"},
	}, &d)

	return creds{user: d.Username, pass: d.Password}
}

// sessionKey is clientID's key in the sessions bucket.
func (c *testCell) sessionKey(clientID string) string {
	return subject.SessionKey(c.tenant, clientID)
}

// killNode sends SIGKILL to broker i and returns when it is gone.
func (c *testCell) killNode(i int) time.Time {
	at := time.Now()
	c.nodes[i].kill()

	return at
}

// killServer sends SIGKILL to NATS server i and returns when it is gone.
func (c *testCell) killServer(i int) time.Time {
	at := time.Now()
	c.servers[i].kill()

	return at
}

// cutNode black-holes every NATS connection of broker i, both ways. Its MQTT
// clients stay connected.
func (c *testCell) cutNode(i int) time.Time {
	for _, p := range c.nodes[i].proxies {
		p.SetMode(faults.Both, faults.Blackhole)
	}

	return time.Now()
}

// healNode lets broker i's NATS traffic through again.
func (c *testCell) healNode(i int) {
	for _, p := range c.nodes[i].proxies {
		p.SetMode(faults.Both, faults.Pass)
	}
}

// delayNode adds d each way to every NATS connection of broker i.
func (c *testCell) delayNode(i int, d time.Duration) {
	for _, p := range c.nodes[i].proxies {
		p.SetDelay(faults.Both, d)
		p.SetMode(faults.Both, faults.Delay)
	}
}

// isolateServer cuts NATS server s off from the other servers, together with
// broker s, whose NATS connection it holds: the routes between s and the rest
// go dark, broker s can reach no other server, and no other broker can reach
// server s.
func (c *testCell) isolateServer(s int) time.Time {
	c.setIsolation(s, faults.Blackhole)

	return time.Now()
}

// healServer undoes isolateServer.
func (c *testCell) healServer(s int) {
	c.setIsolation(s, faults.Pass)
}

func (c *testCell) setIsolation(s int, m faults.Mode) {
	for j := range natsSize {
		if j == s {
			continue
		}

		c.routes[s][j].SetMode(faults.Both, m)
		c.routes[j][s].SetMode(faults.Both, m)
		c.nodes[s].proxies[j].SetMode(faults.Both, m)
		c.nodes[j].proxies[s].SetMode(faults.Both, m)
	}
}

// monitor fetches a monitoring endpoint (path and query, "/jsz?...") of NATS
// server i.
func (c *testCell) monitor(i int, path string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d%s", c.servers[i].httpPort, path), http.NoBody)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)

	return string(data), err
}

// serverIndex maps a server name ("s2") to its index.
func (c *testCell) serverIndex(name string) int {
	for i, s := range c.servers {
		if s.name == name {
			return i
		}
	}

	c.t.Fatalf("no server named %q", name)

	return -1
}

// logCount counts the lines of node i's log that contain s.
func (c *testCell) logCount(i int, s string) int {
	data, err := os.ReadFile(c.nodes[i].logPath)
	if err != nil {
		return 0
	}

	return bytes.Count(data, []byte(s))
}

// dumpLogs prints the warnings and errors of every process, for a failed
// run.
func (c *testCell) dumpLogs() {
	for _, n := range c.nodes {
		if n == nil || n.proc == nil {
			continue
		}

		c.t.Logf("---- %s (%s), warnings and errors:\n%s", n.id, n.logPath, grepTail(n.logPath, 60, "WARN", "ERROR", "level=warn", "level=error"))
	}

	for _, s := range c.servers {
		if s == nil || s.proc == nil {
			continue
		}

		c.t.Logf("---- %s (%s), tail:\n%s", s.name, s.logPath, tail(s.logPath, 20))
	}
}

// dumpState saves each live NATS server's /jsz and /connz in the cell's
// directory, for a failed run.
func (c *testCell) dumpState() {
	for i, s := range c.servers {
		if s == nil || s.proc == nil || s.exited() {
			continue
		}

		// The JetStream and connection views of the server, next to the
		// logs: which consumers exist, where, and how far they got.
		for name, path := range map[string]string{"jsz": "/jsz?consumers=true", "connz": "/connz?subs=detail&limit=1000"} {
			body, err := c.monitor(i, path)
			if err != nil {
				c.t.Logf("---- %s %s: %v", s.name, name, err)

				continue
			}

			out := filepath.Join(c.dir, s.name+"-"+name+".json")
			if err := os.WriteFile(out, []byte(body), 0o600); err != nil {
				c.t.Logf("---- %s %s: %v", s.name, name, err)
			}
		}
	}
}

func tail(path string, n int) string {
	return grepTail(path, n)
}

// grepTail returns the last n lines of the file that contain any of the
// needles (every line when there are none).
func grepTail(path string, n int, needles ...string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}

	var keep []string

	for line := range strings.SplitSeq(string(data), "\n") {
		if len(needles) == 0 || slices.ContainsFunc(needles, func(s string) bool { return strings.Contains(line, s) }) {
			keep = append(keep, line)
		}
	}

	if len(keep) > n {
		keep = keep[len(keep)-n:]
	}

	return strings.Join(keep, "\n")
}

// ---------------------------------------------------------------------------
// The observer
// ---------------------------------------------------------------------------

// kvEvent is one update of a key, as the observer saw it.
type kvEvent struct {
	seen    time.Time // when the observer received it
	stamp   time.Time // when JetStream stored it
	rev     uint64
	deleted bool
	rec     *cluster.SessionRecord // sessions bucket only
}

// observer reads the cell from NATS directly, past every proxy: session
// records and heartbeats as they change, the log, and stream leaders.
type observer struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg cluster.Config

	mu       sync.Mutex
	sessions map[string][]kvEvent
	members  map[string][]kvEvent
}

func newObserver(t *testing.T, urls []string) *observer {
	t.Helper()

	nc, err := nats.Connect(strings.Join(urls, ","),
		nats.Name("chaos-observer"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(250*time.Millisecond),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	return &observer{
		nc: nc, js: js, cfg: cellConfig(),
		sessions: make(map[string][]kvEvent),
		members:  make(map[string][]kvEvent),
	}
}

// watch follows the sessions and members buckets until the test ends.
func (o *observer) watch(t *testing.T) {
	t.Helper()

	o.follow(t, o.cfg.SessionsBucket(), o.sessions, true)
	o.follow(t, o.cfg.MembersBucket(), o.members, false)
}

func (o *observer) follow(t *testing.T, bucket string, into map[string][]kvEvent, records bool) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	var (
		kv  jetstream.KeyValue
		err error
	)

	eventually(t, startWait, "bucket "+bucket, func() bool {
		octx, ocancel := context.WithTimeout(ctx, 2*time.Second)
		defer ocancel()

		kv, err = o.js.KeyValue(octx, bucket)

		return err == nil
	})

	w, err := kv.WatchAll(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		for e := range w.Updates() {
			if e == nil {
				continue
			}

			ev := kvEvent{seen: time.Now(), stamp: e.Created(), rev: e.Revision(), deleted: e.Operation() != jetstream.KeyValuePut}

			if records && !ev.deleted {
				var rec cluster.SessionRecord
				if json.Unmarshal(e.Value(), &rec) == nil {
					ev.rec = &rec
				}
			}

			o.mu.Lock()
			into[e.Key()] = append(into[e.Key()], ev)
			o.mu.Unlock()
		}
	}()

	t.Cleanup(func() {
		discard(w.Stop())
		cancel()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})
}

// events returns a copy of the updates seen for a session key.
func (o *observer) events(key string) []kvEvent {
	o.mu.Lock()
	defer o.mu.Unlock()

	return slices.Clone(o.sessions[key])
}

// record returns the latest record of a session key, nil when there is none.
func (o *observer) record(key string) *cluster.SessionRecord {
	ev := o.events(key)
	if len(ev) == 0 || ev[len(ev)-1].deleted {
		return nil
	}

	return ev[len(ev)-1].rec
}

// lastBeat returns the newest heartbeat of a node seen so far.
func (o *observer) lastBeat(nodeID string) (kvEvent, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	ev := o.members[nodeID]
	if len(ev) == 0 {
		return kvEvent{}, false
	}

	return ev[len(ev)-1], true
}

// beats returns a copy of a node's heartbeats.
func (o *observer) beats(nodeID string) []kvEvent {
	o.mu.Lock()
	defer o.mu.Unlock()

	return slices.Clone(o.members[nodeID])
}

// streamInfo reads the log stream's info, retrying through elections.
func (o *observer) streamInfo(t *testing.T) *jetstream.StreamInfo {
	t.Helper()

	var info *jetstream.StreamInfo

	eventually(t, 30*time.Second, "log stream info", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		s, err := o.js.Stream(ctx, o.cfg.LogStream())
		if err != nil {
			return false
		}

		info, err = s.Info(ctx)

		return err == nil && info.Cluster != nil && info.Cluster.Leader != ""
	})

	return info
}

// waitNewLeader polls the log stream every 100 ms until a leader other than
// old is in place and returns when it saw it.
func (o *observer) waitNewLeader(t *testing.T, old string) time.Time {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		s, err := o.js.Stream(ctx, o.cfg.LogStream())
		if err == nil {
			var info *jetstream.StreamInfo

			info, err = s.Info(ctx)
			if err == nil && info.Cluster != nil && info.Cluster.Leader != "" && info.Cluster.Leader != old {
				cancel()

				return time.Now()
			}
		}

		cancel()
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("no new log stream leader within 60 s of losing %s", old)

	return time.Time{}
}

// logEntry is one message of the cell's log.
type logEntry struct {
	seq     uint64
	topic   string
	payload string
}

// readLog returns the tenant's log messages with sequence above from.
func (o *observer) readLog(t *testing.T, tenant string, from uint64) []logEntry {
	t.Helper()

	last := o.streamInfo(t).State.LastSeq
	if last <= from {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cons, err := o.js.OrderedConsumer(ctx, o.cfg.LogStream(), jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject.LogTenantAll(tenant)},
		DeliverPolicy:  jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:    from + 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	var out []logEntry

	for ctx.Err() == nil {
		batch, err := cons.Fetch(1000, jetstream.FetchMaxWait(time.Second))
		if err != nil {
			t.Fatal(err)
		}

		n := 0

		for m := range batch.Messages() {
			n++

			md, err := m.Metadata()
			if err != nil {
				t.Fatal(err)
			}

			_, topic, err := subject.DecodeLog(m.Subject())
			if err != nil {
				t.Fatal(err)
			}

			out = append(out, logEntry{seq: md.Sequence.Stream, topic: topic, payload: string(m.Data())})

			if md.Sequence.Stream >= last {
				return out
			}
		}

		if n == 0 && batch.Error() == nil {
			// Nothing left of this tenant's below last.
			return out
		}
	}

	t.Fatal("reading the log timed out")

	return nil
}

// ---------------------------------------------------------------------------
// MQTT clients
// ---------------------------------------------------------------------------

// conn is one MQTT connection of a test client, as the client saw it.
type conn struct {
	node           string
	at             time.Time // CONNACK received
	end            time.Time // zero while connected
	reason         byte      // DISCONNECT reason from the server, 0 when none came
	gotDisconnect  bool
	sessionPresent bool
	err            string // the client error that ended it, if any

	pc  *paho.Client
	raw net.Conn
}

// ended records one report of the connection's end under mu: the first
// report's time and error, and a DISCONNECT's reason whenever it comes. It
// says whether this was the first report.
func (c *conn) ended(reason byte, gotDisc bool, err error, mu *sync.Mutex) bool {
	mu.Lock()
	defer mu.Unlock()

	if gotDisc {
		c.reason, c.gotDisconnect = reason, true
	}

	if err != nil && c.err == "" {
		c.err = err.Error()
	}

	if !c.end.IsZero() {
		return false
	}

	c.end = time.Now()

	return true
}

// dialMQTT opens a 5.0 connection to addr. The handlers receive messages and
// learn how the connection ended; onEnd may be called more than once for one
// connection.
func dialMQTT(addr, clientID string, cr creds, clean bool, timeout time.Duration,
	onMsg func(*paho.Publish), onEnd func(reason byte, gotDisc bool, err error),
) (*paho.Client, net.Conn, *paho.Connack, error) {
	d := net.Dialer{Timeout: 2 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, nil, err
	}

	// paho calls OnServerDisconnect on its own goroutine after it has closed
	// the client, so the end can be reported more than once and in either
	// order. onEnd keeps the first time and any DISCONNECT reason.
	end := onEnd

	pc := paho.NewClient(paho.ClientConfig{
		Conn:          raw,
		PacketTimeout: timeout,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				if onMsg != nil {
					onMsg(pr.Packet)
				}

				return true, nil
			},
		},
		OnServerDisconnect: func(d *paho.Disconnect) { end(d.ReasonCode, true, nil) },
		OnClientError:      func(err error) { end(0, false, err) },
	})

	expiry := uint32(sessionExpiry)

	ca, err := pc.Connect(ctx, &paho.Connect{
		ClientID:     clientID,
		Username:     cr.user,
		UsernameFlag: true,
		Password:     []byte(cr.pass),
		PasswordFlag: true,
		CleanStart:   clean,
		KeepAlive:    30,
		Properties:   &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	})
	if err != nil {
		raw.Close()

		if ca != nil {
			return nil, nil, ca, fmt.Errorf("connack 0x%02x: %w", ca.ReasonCode, err)
		}

		return nil, nil, nil, err
	}

	go func() {
		<-pc.Done()
		end(0, false, errors.New("client done"))
	}()

	return pc, raw, ca, nil
}

// subscriber is a persistent MQTT 5 session subscribed at QoS 1. It records
// every message it receives by payload, every connection, and every
// DISCONNECT the server sends it.
type subscriber struct {
	id     string
	cr     creds
	filter string
	// slow is how long the handler sleeps per message: a slow consumer.
	slow time.Duration

	mu    sync.Mutex
	got   map[string]time.Time // first receipt of each payload
	dups  int
	conns []*conn
	cur   *conn
	drop  chan struct{} // gets a value when a connection ends
}

func newSubscriber(c *testCell, clientID, filter string) *subscriber {
	return &subscriber{
		id: clientID, cr: c.device(clientID), filter: filter,
		got:  make(map[string]time.Time),
		drop: make(chan struct{}, 1),
	}
}

// connect opens a connection to node n. The first connection starts a clean
// session and subscribes; later ones resume it.
func (s *subscriber) connect(n *node) (*conn, error) {
	s.mu.Lock()
	first := len(s.conns) == 0
	s.mu.Unlock()

	cn := &conn{node: n.id}

	// A drop signal left from an earlier connection says nothing about this
	// one.
	select {
	case <-s.drop:
	default:
	}

	pc, raw, ca, err := dialMQTT(n.mqtt, s.id, s.cr, first, 30*time.Second, s.onMsg, func(reason byte, gotDisc bool, err error) {
		if cn.ended(reason, gotDisc, err, &s.mu) {
			s.lost(cn)
		}
	})
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	cn.at, cn.sessionPresent, cn.pc, cn.raw = time.Now(), ca.SessionPresent, pc, raw
	s.conns = append(s.conns, cn)
	gone := !cn.end.IsZero()

	if !gone {
		s.cur = cn
	}
	s.mu.Unlock()

	if gone {
		// It ended before it was registered, so lost said nothing.
		s.signalDrop()
	}

	if first {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		sa, err := pc.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: s.filter, QoS: 1}}})
		if err != nil {
			return cn, fmt.Errorf("subscribe: %w", err)
		}

		if len(sa.Reasons) != 1 || sa.Reasons[0] != 1 {
			return cn, fmt.Errorf("suback %v", sa.Reasons)
		}
	}

	return cn, nil
}

// lost handles the end of cn: a registered connection stops being current and
// signals the drop. One that is not registered yet (still in connect, or a
// refused CONNECT) is left to connect.
func (s *subscriber) lost(cn *conn) {
	s.mu.Lock()
	registered := !cn.at.IsZero()

	if s.cur == cn {
		s.cur = nil
	}
	s.mu.Unlock()

	if registered {
		s.signalDrop()
	}
}

func (s *subscriber) signalDrop() {
	select {
	case s.drop <- struct{}{}:
	default:
	}
}

func (s *subscriber) onMsg(p *paho.Publish) {
	if s.slow > 0 {
		time.Sleep(s.slow)
	}

	key := string(p.Payload)
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.got[key]; ok {
		s.dups++

		return
	}

	s.got[key] = now
}

// waitDrop waits until the current connection ends.
func (s *subscriber) waitDrop(d time.Duration) bool {
	select {
	case <-s.drop:
		return true
	case <-time.After(d):
		return false
	}
}

// abort closes the TCP connection under the client, as a network failure on
// the client's side would, without a DISCONNECT.
func (s *subscriber) abort() {
	s.mu.Lock()
	cn := s.cur
	s.mu.Unlock()

	if cn != nil {
		cn.raw.Close()
	}
}

// close disconnects normally (the session stays) if connected.
func (s *subscriber) close() {
	s.mu.Lock()
	cn := s.cur
	s.mu.Unlock()

	if cn != nil {
		discard(cn.pc.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	}
}

// keepConnected reconnects whenever the connection ends, trying the nodes in
// order with a short pause, until ctx ends.
func (s *subscriber) keepConnected(ctx context.Context, nodes ...*node) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.drop:
			}

			for ctx.Err() == nil {
				if s.tryNodes(nodes) {
					break
				}

				sleepCtx(ctx, 200*time.Millisecond)
			}
		}
	}()
}

func (s *subscriber) tryNodes(nodes []*node) bool {
	for _, n := range nodes {
		if n.exited() {
			continue
		}

		if _, err := s.connect(n); err == nil {
			return true
		}
	}

	return false
}

// snapshot returns copies of what the subscriber recorded.
func (s *subscriber) snapshot() (got map[string]time.Time, conns []conn, dups int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	got = make(map[string]time.Time, len(s.got))
	for k, v := range s.got {
		got[k] = v
	}

	conns = make([]conn, len(s.conns))
	for i, c := range s.conns {
		conns[i] = *c
	}

	return got, conns, s.dups
}

// has reports whether the payload arrived.
func (s *subscriber) has(key string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	at, ok := s.got[key]

	return at, ok
}

// takeovers counts the DISCONNECTs with reason 0x8E (session taken over) the
// server sent this client.
func (s *subscriber) takeovers() int {
	_, conns, _ := s.snapshot()

	n := 0

	for _, c := range conns {
		if c.gotDisconnect && c.reason == 0x8E {
			n++
		}
	}

	return n
}

// publisher publishes QoS 1 messages at a fixed rate on one topic, with
// payloads "<name>:<n>", and records when each was sent and which got a
// successful PUBACK.
type publisher struct {
	name  string
	id    string
	cr    creds
	topic string
	rate  int
	nodes []*node // reconnect order

	mu       sync.Mutex
	sent     map[string]time.Time
	acked    map[string]time.Time
	nacked   map[string]byte
	failed   int // publishes that ended without any PUBACK
	next     int
	conns    []*conn
	cur      *conn
	drop     chan struct{}
	wg       sync.WaitGroup // in-flight publishes
	cancel   context.CancelFunc
	loops    sync.WaitGroup
	stopOnce sync.Once
}

func newPublisher(c *testCell, name, topic string, rate int, nodes ...*node) *publisher {
	clientID := "pub-" + name

	p := &publisher{
		name: name, id: clientID, cr: c.device(clientID), topic: topic, rate: rate, nodes: nodes,
		sent: make(map[string]time.Time), acked: make(map[string]time.Time), nacked: make(map[string]byte),
		drop: make(chan struct{}, 1),
	}

	// A scenario that fails part way must not leave the publisher running
	// into the next one: disconnect at once, without waiting for acks.
	c.t.Cleanup(func() { p.halt(false) })

	return p
}

// start connects and publishes until stop.
func (p *publisher) start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	p.loops.Add(2)

	go func() {
		defer p.loops.Done()

		p.connectLoop(ctx)
	}()

	go func() {
		defer p.loops.Done()

		p.publishLoop(ctx)
	}()
}

// stop ends publishing, waits for the publishes in flight to finish, and
// disconnects. Only the first stop or halt does anything.
func (p *publisher) stop() { p.halt(true) }

// halt ends publishing and disconnects. With drain it first waits for the
// publishes in flight to be answered; without, it disconnects at once, which
// ends them.
func (p *publisher) halt(drain bool) {
	p.stopOnce.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}

		p.loops.Wait()

		if drain {
			p.wg.Wait()
		}

		p.mu.Lock()
		cn := p.cur
		p.mu.Unlock()

		if cn != nil {
			discard(cn.pc.Disconnect(&paho.Disconnect{ReasonCode: 0}))
		}

		p.wg.Wait()
	})
}

func (p *publisher) connectLoop(ctx context.Context) {
	p.drop <- struct{}{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.drop:
		}

		for ctx.Err() == nil && !p.tryConnect() {
			sleepCtx(ctx, 200*time.Millisecond)
		}
	}
}

func (p *publisher) tryConnect() bool {
	for _, n := range p.nodes {
		if n.exited() {
			continue
		}

		cn := &conn{node: n.id}

		pc, raw, _, err := dialMQTT(n.mqtt, p.id, p.cr, true, 60*time.Second, nil, func(reason byte, gotDisc bool, err error) {
			if cn.ended(reason, gotDisc, err, &p.mu) {
				p.lost(cn)
			}
		})
		if err != nil {
			continue
		}

		p.mu.Lock()
		cn.at, cn.pc, cn.raw = time.Now(), pc, raw
		p.conns = append(p.conns, cn)
		gone := !cn.end.IsZero()

		if !gone {
			p.cur = cn
		}
		p.mu.Unlock()

		if gone {
			p.signalDrop()
		}

		return true
	}

	return false
}

// lost handles the end of a connection, as subscriber.lost does.
func (p *publisher) lost(cn *conn) {
	p.mu.Lock()
	registered := !cn.at.IsZero()

	if p.cur == cn {
		p.cur = nil
	}
	p.mu.Unlock()

	if registered {
		p.signalDrop()
	}
}

func (p *publisher) signalDrop() {
	select {
	case p.drop <- struct{}{}:
	default:
	}
}

func (p *publisher) publishLoop(ctx context.Context) {
	const tick = 10 * time.Millisecond

	perTick := max(p.rate/100, 1)

	t := time.NewTicker(tick)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		p.mu.Lock()
		cn := p.cur
		p.mu.Unlock()

		if cn == nil {
			continue
		}

		for range perTick {
			p.publishOne(cn)
		}
	}
}

func (p *publisher) publishOne(cn *conn) {
	p.mu.Lock()
	key := fmt.Sprintf("%s:%d", p.name, p.next)
	p.next++
	p.sent[key] = time.Now()
	p.mu.Unlock()

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		resp, err := cn.pc.Publish(ctx, &paho.Publish{Topic: p.topic, QoS: 1, Payload: []byte(key)})
		now := time.Now()

		p.mu.Lock()
		defer p.mu.Unlock()

		switch {
		case resp != nil && resp.ReasonCode < 0x80 && err == nil:
			p.acked[key] = now
		case resp != nil:
			p.nacked[key] = resp.ReasonCode
		default:
			p.failed++
		}
	}()
}

// pubStats is a copy of a publisher's records.
type pubStats struct {
	sent, acked map[string]time.Time
	nacked      map[string]byte
	failed      int
	conns       []conn
}

func (p *publisher) stats() pubStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	st := pubStats{
		sent: make(map[string]time.Time, len(p.sent)), acked: make(map[string]time.Time, len(p.acked)),
		nacked: make(map[string]byte, len(p.nacked)), failed: p.failed,
	}

	for k, v := range p.sent {
		st.sent[k] = v
	}

	for k, v := range p.acked {
		st.acked[k] = v
	}

	for k, v := range p.nacked {
		st.nacked[k] = v
	}

	for _, c := range p.conns {
		st.conns = append(st.conns, *c)
	}

	return st
}

// ackCount is the number of successful PUBACKs so far.
func (p *publisher) ackCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.acked)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// eventually polls cond every 50 ms until it holds, failing the test after d.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(d)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// discard drops an error nothing can act on.
func discard(error) {}
