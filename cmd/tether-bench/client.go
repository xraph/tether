package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// keepAlive is the MQTT keep alive the bench clients ask for, in seconds. It is
// long on purpose: with tens of thousands of clients in one process, a late
// PINGREQ should not be what ends a connection.
const keepAlive = 120

// epoch is the clock every latency is measured on. Publisher and subscriber live
// in this process, so a send time stamped on one reads correctly on the other,
// and time.Since uses the monotonic clock.
var epoch = time.Now()

// target is what the load commands connect to and how they judge it.
type target struct {
	addrs      []string // MQTT listeners, host:port, one per node
	creds      []cred
	prefix     string // client ID prefix, unique per run
	pids       []int  // broker processes, for memory
	serverPids []int  // NATS server processes, for CPU
	admins     []string
	token      string
	tenant     string
	timeout    time.Duration
}

// newPrefix is a short random client ID prefix, so a run never takes over the
// sessions of an earlier one.
func newPrefix() string {
	var b [3]byte

	if _, err := rand.Read(b[:]); err != nil {
		return "b"
	}

	return "b" + hex.EncodeToString(b[:])
}

func (t *target) cred(i int) cred { return t.creds[i%len(t.creds)] }

// client is one connected MQTT 5 client.
type client struct {
	id   string
	node int
	pc   *paho.Client
	raw  net.Conn

	ended atomic.Bool
	// lost is closed when the connection ends without the bench asking for it.
	lost     chan struct{}
	lostOnce sync.Once
	closing  atomic.Bool
}

// dial connects client number i under role (a name that goes in the client ID)
// to node n and returns it with the time CONNECT took. onMsg, when set, gets
// every PUBLISH the broker sends the client.
func (t *target) dial(ctx context.Context, role string, i, n int, onMsg func(*paho.Publish)) (*client, time.Duration, error) {
	addr := t.addrs[n]
	id := fmt.Sprintf("%s-%s-%d", t.prefix, role, i)
	cr := t.cred(i)
	start := time.Now()

	dctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	d := net.Dialer{}

	raw, err := d.DialContext(dctx, "tcp", addr)
	if err != nil {
		return nil, 0, fmt.Errorf("dial: %w", err)
	}

	c := &client{id: id, node: n, raw: raw, lost: make(chan struct{})}

	cfg := paho.ClientConfig{
		Conn:               raw,
		PacketTimeout:      t.timeout,
		OnServerDisconnect: func(*paho.Disconnect) { c.markLost() },
		OnClientError:      func(error) { c.markLost() },
	}

	if onMsg != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				onMsg(pr.Packet)

				return true, nil
			},
		}
	}

	c.pc = paho.NewClient(cfg)

	ca, err := c.pc.Connect(dctx, &paho.Connect{
		ClientID:     id,
		Username:     cr.User,
		UsernameFlag: true,
		Password:     []byte(cr.Pass),
		PasswordFlag: true,
		CleanStart:   true,
		KeepAlive:    keepAlive,
	})
	if err != nil {
		raw.Close()

		if ca != nil {
			return nil, 0, fmt.Errorf("connack 0x%02x: %w", ca.ReasonCode, err)
		}

		return nil, 0, fmt.Errorf("connect: %w", err)
	}

	return c, time.Since(start), nil
}

func (c *client) markLost() {
	if !c.closing.Load() {
		c.lostOnce.Do(func() { close(c.lost) })
	}
}

// close ends the connection with a DISCONNECT.
func (c *client) close() {
	if !c.ended.CompareAndSwap(false, true) {
		return
	}

	c.closing.Store(true)

	discard(c.pc.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	c.raw.Close()
}

// drop kills the connection the way a crash or a cut cable does: no DISCONNECT,
// and a reset instead of a FIN, so the closed port does not sit in TIME_WAIT.
func (c *client) drop() {
	if !c.ended.CompareAndSwap(false, true) {
		return
	}

	c.closing.Store(true)

	if tc, ok := c.raw.(*net.TCPConn); ok {
		discard(tc.SetLinger(0))
	}

	c.raw.Close()
}

// connStats is what a batch of connection attempts did.
type connStats struct {
	OK       int            `json:"ok"`
	Failed   int            `json:"failed"`
	Errors   map[string]int `json:"errors,omitempty"`
	Latency  summary        `json:"connect_latency"`
	Elapsed  float64        `json:"elapsed_s"`
	Achieved float64        `json:"achieved_per_s"`
	PerNode  []int          `json:"ok_per_node"`

	raw []time.Duration // every successful connect's latency, for merging batches
}

// connector collects the results of connection attempts from many goroutines.
type connector struct {
	mu      sync.Mutex
	clients []*client
	lat     []time.Duration
	errs    map[string]int
	failed  int
	perNode []int
}

func newConnector(nodes int) *connector {
	return &connector{errs: map[string]int{}, perNode: make([]int, nodes)}
}

func (cn *connector) add(c *client, lat time.Duration, err error) {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	if err != nil {
		cn.failed++
		cn.errs[errorKey(err)]++

		return
	}

	cn.clients = append(cn.clients, c)
	cn.lat = append(cn.lat, lat)
	cn.perNode[c.node]++
}

// stats is the connector's results so far, over elapsed.
func (cn *connector) stats(elapsed time.Duration) connStats {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	lat := append([]time.Duration(nil), cn.lat...)
	st := connStats{
		OK: len(cn.clients), Failed: cn.failed, Latency: summarize(lat),
		Elapsed: elapsed.Seconds(), PerNode: append([]int(nil), cn.perNode...),
		raw: append([]time.Duration(nil), cn.lat...),
	}

	if len(cn.errs) > 0 {
		st.Errors = make(map[string]int, len(cn.errs))
		for k, v := range cn.errs {
			st.Errors[k] = v
		}
	}

	if elapsed > 0 {
		st.Achieved = float64(st.OK+st.Failed) / elapsed.Seconds()
	}

	return st
}

// errorKey groups an error for counting: the CONNACK reason, or the kind of failure.
func errorKey(err error) string {
	var ne net.Error

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	}

	s := err.Error()
	if len(s) > 80 {
		s = s[:80]
	}

	return s
}

// connectMany opens n clients of the given role at about rate per second (zero
// means as fast as parallel dials allow), with at most parallel dials in flight.
// Client i goes to node nodeOf(i), or to node i modulo the node count when
// nodeOf is nil. onMsg, when set, builds the handler for client i. It returns
// the connected clients, in no particular order, and what the attempts did.
func (t *target) connectMany(ctx context.Context, role string, n int, rate float64, parallel int,
	nodeOf func(i int) int, onMsg func(i int) func(*paho.Publish),
) ([]*client, connStats) {
	cn := newConnector(len(t.addrs))
	sem := make(chan struct{}, parallel)
	start := time.Now()

	var wg sync.WaitGroup

	for i := range n {
		if ctx.Err() != nil {
			break
		}

		if rate > 0 {
			sleepUntil(ctx, start.Add(time.Duration(float64(i)/rate*float64(time.Second))))
		}

		sem <- struct{}{}

		node := i % len(t.addrs)
		if nodeOf != nil {
			node = nodeOf(i)
		}

		var h func(*paho.Publish)
		if onMsg != nil {
			h = onMsg(i)
		}

		wg.Go(func() {
			defer func() { <-sem }()

			c, lat, err := t.dial(ctx, role, i, node, h)
			cn.add(c, lat, err)
		})
	}

	wg.Wait()

	return cn.clients, cn.stats(time.Since(start))
}

// closeAll ends every client, a bounded number at a time.
func closeAll(cs []*client, abrupt bool) {
	sem := make(chan struct{}, 256)

	var wg sync.WaitGroup

	for _, c := range cs {
		sem <- struct{}{}

		wg.Go(func() {
			defer func() { <-sem }()

			if abrupt {
				c.drop()
			} else {
				c.close()
			}
		})
	}

	wg.Wait()
}

func sleepUntil(ctx context.Context, at time.Time) {
	d := time.Until(at)
	if d <= 0 {
		return
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
