package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/server/pse"
	"github.com/nats-io/nats.go"
)

// Each NATS server runs in its own child process (this binary with -serve), so
// RSS and CPU time are per server and are not mixed with the harness's own heap
// (1M client subscriptions plus 1M mochi filters).

// serverStats is what a child reports about itself.
type serverStats struct {
	Name      string `json:"name"`
	RSS       int64  `json:"rss"`        // resident set size, bytes, before any GC
	RSSAfter  int64  `json:"rss_after"`  // after runtime.GC and FreeOSMemory (only when asked)
	HeapInuse uint64 `json:"heap_inuse"` // after GC when asked
	GoSys     uint64 `json:"go_sys"`
	UserUS    int64  `json:"user_us"` // cumulative user CPU, microseconds
	SysUS     int64  `json:"sys_us"`  // cumulative system CPU, microseconds
	NumSubs   uint32 `json:"num_subs"`
	NumRoutes int    `json:"num_routes"`
}

func (s serverStats) cpuUS() int64 { return s.UserUS + s.SysUS }

func rusageSelf() (user, sys int64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}

	return ru.Utime.Nano() / 1e3, ru.Stime.Nano() / 1e3
}

// runChild starts one nats-server and serves /stats until stdin closes. The
// parent holds stdin, so the child exits when the parent exits for any reason.
func runChild(name, routes string) error {
	opts := &server.Options{
		ServerName: name,
		Host:       "127.0.0.1",
		Port:       -1,
		NoLog:      true,
		NoSigs:     true,
		Cluster: server.ClusterOpts{
			Name: "spike",
			Host: "127.0.0.1",
			Port: -1,
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

	if !s.ReadyForConnections(30 * time.Second) {
		return errors.New("server not ready")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		var pcpu float64

		var rss, vss int64

		_ = pse.ProcUsage(&pcpu, &rss, &vss)

		st := serverStats{Name: name, RSS: rss, NumSubs: s.NumSubscriptions(), NumRoutes: s.NumRoutes()}

		if r.URL.Query().Get("gc") == "1" {
			runtime.GC()
			debug.FreeOSMemory()

			_ = pse.ProcUsage(&pcpu, &st.RSSAfter, &vss)
		}

		var ms runtime.MemStats

		runtime.ReadMemStats(&ms)
		st.HeapInuse, st.GoSys = ms.HeapInuse, ms.Sys
		st.UserUS, st.SysUS = rusageSelf()

		_ = json.NewEncoder(w).Encode(st)
	})

	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() { _ = hs.Serve(ln) }()

	fmt.Printf("READY %s nats-route://%s http://%s\n", s.ClientURL(), s.ClusterAddr(), ln.Addr())

	_, _ = io.Copy(io.Discard, os.Stdin)

	_ = hs.Close()

	s.Shutdown()
	s.WaitForShutdown()

	return nil
}

type child struct {
	name      string
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	clientURL string
	routeURL  string
	statsURL  string
}

type cluster struct {
	children []*child
	http     *http.Client
}

func startChild(name, routes string) (*child, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(self, "-serve", "-name", name, "-routes", routes)
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
		f := strings.Fields(l)
		c.clientURL, c.routeURL, c.statsURL = f[1], f[2], f[3]
	case <-time.After(60 * time.Second):
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
	case <-time.After(20 * time.Second):
		_ = c.cmd.Process.Kill()

		<-done
	}
}

// startCluster starts n servers, the first as the seed every other one routes
// to, and waits until a message published on each server reaches each other one.
func startCluster(n int) (*cluster, error) {
	cl := &cluster{http: &http.Client{Timeout: 60 * time.Second}}

	for i := range n {
		routes := ""
		if i > 0 {
			routes = cl.children[0].routeURL
		}

		c, err := startChild(fmt.Sprintf("n%d", i), routes)
		if err != nil {
			cl.stop()

			return nil, err
		}

		cl.children = append(cl.children, c)
	}

	if err := cl.waitMesh(60 * time.Second); err != nil {
		cl.stop()

		return nil, err
	}

	return cl, nil
}

func (cl *cluster) waitMesh(timeout time.Duration) error {
	conns := make([]*nats.Conn, len(cl.children))

	defer func() {
		for _, nc := range conns {
			if nc != nil {
				nc.Close()
			}
		}
	}()

	chans := make([]chan *nats.Msg, len(cl.children))

	for i, c := range cl.children {
		nc, err := nats.Connect(c.clientURL)
		if err != nil {
			return err
		}

		conns[i] = nc
		chans[i] = make(chan *nats.Msg, 64)

		if _, err := nc.ChanSubscribe(fmt.Sprintf("mesh.%d", i), chans[i]); err != nil {
			return err
		}

		if err := nc.Flush(); err != nil {
			return err
		}
	}

	deadline := time.Now().Add(timeout)

	for to := range cl.children {
		for from := range cl.children {
			if from == to {
				continue
			}

			for {
				if time.Now().After(deadline) {
					return fmt.Errorf("mesh: n%d never reached n%d", from, to)
				}

				_ = conns[from].Publish(fmt.Sprintf("mesh.%d", to), []byte(fmt.Sprint(from)))

				select {
				case m := <-chans[to]:
					if string(m.Data) == fmt.Sprint(from) {
						goto next
					}
				case <-time.After(50 * time.Millisecond):
				}
			}
		next:
		}
	}

	return nil
}

func (cl *cluster) stats(gc bool) []serverStats {
	out := make([]serverStats, len(cl.children))

	var wg sync.WaitGroup

	for i, c := range cl.children {
		wg.Go(func() {
			u := c.statsURL + "/stats"
			if gc {
				u += "?gc=1"
			}

			resp, err := cl.http.Get(u) //nolint:noctx // spike
			if err != nil {
				out[i] = serverStats{Name: c.name}

				return
			}

			defer resp.Body.Close()

			_ = json.NewDecoder(resp.Body).Decode(&out[i])
		})
	}

	wg.Wait()

	return out
}

func (cl *cluster) stop() {
	var wg sync.WaitGroup

	for _, c := range cl.children {
		wg.Go(c.stop)
	}

	wg.Wait()
}

func sumCPU(st []serverStats) int64 {
	var t int64
	for _, s := range st {
		t += s.cpuUS()
	}

	return t
}
