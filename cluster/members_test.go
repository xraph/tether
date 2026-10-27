package cluster

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/internal/chaos"
)

const (
	testHeartbeat = 100 * time.Millisecond
	testLease     = time.Second
	// testDeadAfter is lease plus the fixed margin.
	testDeadAfter = testLease + peerDeadMargin
)

// memberEvents records what a Members called back.
type memberEvents struct {
	mu       sync.Mutex
	fences   []time.Time
	unfences []time.Time
	dead     []deadEvent
}

type deadEvent struct {
	id string
	at time.Time
}

func (e *memberEvents) attach(m *Members) {
	m.OnFence(func() {
		e.mu.Lock()
		e.fences = append(e.fences, time.Now())
		e.mu.Unlock()
	})
	m.OnUnfence(func() {
		e.mu.Lock()
		e.unfences = append(e.unfences, time.Now())
		e.mu.Unlock()
	})
	m.OnPeerDead(func(id string) {
		e.mu.Lock()
		e.dead = append(e.dead, deadEvent{id, time.Now()})
		e.mu.Unlock()
	})
}

func (e *memberEvents) fenceTimes() []time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()

	return slices.Clone(e.fences)
}

func (e *memberEvents) unfenceTimes() []time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()

	return slices.Clone(e.unfences)
}

func (e *memberEvents) deadEvents(id string) []deadEvent {
	e.mu.Lock()
	defer e.mu.Unlock()

	var out []deadEvent

	for _, d := range e.dead {
		if id == "" || d.id == id {
			out = append(out, d)
		}
	}

	return out
}

// memberCell is a three-server cell whose buckets exist.
type memberCell struct {
	urls []string
}

func startMemberCell(t *testing.T) *memberCell {
	t.Helper()

	urls := clustertest.Start(t, 3)
	cfg := memberConfig("setup", urls[0])

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	if err := Provision(tctx(t, 30*time.Second), c, cfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	return &memberCell{urls: urls}
}

func memberConfig(id, url string) Config {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.NodeID = id
	cfg.Cell = "c1"
	cfg.Mode = ModeExternal
	cfg.URLs = []string{url}
	cfg.Replicas = 3
	cfg.Heartbeat = testHeartbeat
	cfg.Lease = testLease

	return cfg
}

// memberNode is one started Members with its connection.
type memberNode struct {
	cfg  Config
	conn *Conn
	m    *Members
	ev   *memberEvents
}

// node connects a node with the given ID to url and starts its Members.
func (mc *memberCell) node(t *testing.T, id, url string) *memberNode {
	t.Helper()

	cfg := memberConfig(id, url)

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	n := &memberNode{cfg: cfg, conn: c, ev: &memberEvents{}}
	n.m = n.newMembers()

	if err := n.m.Start(tctx(t, 10*time.Second)); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { n.m.Stop() })

	return n
}

// newMembers makes a Members on the node's connection with events attached.
func (n *memberNode) newMembers() *Members {
	m := NewMembers(n.conn, n.cfg)
	n.ev.attach(m)

	return m
}

// nodeOn starts node i of the cell on server i%3.
func (mc *memberCell) nodeOn(t *testing.T, id string, server int) *memberNode {
	t.Helper()

	return mc.node(t, id, mc.urls[server%len(mc.urls)])
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(d)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func liveIs(m *Members, want ...string) func() bool {
	return func() bool { return slices.Equal(m.Live(), want) }
}

func TestPeersSeeEachOther(t *testing.T) {
	cell := startMemberCell(t)
	a := cell.nodeOn(t, "n1", 0)
	b := cell.nodeOn(t, "n2", 1)

	eventually(t, 5*time.Second, "A to see B", liveIs(a.m, "n1", "n2"))
	eventually(t, 5*time.Second, "B to see A", liveIs(b.m, "n1", "n2"))

	if a.m.Fenced() || b.m.Fenced() {
		t.Fatal("a healthy node is fenced")
	}

	// Heartbeats keep both live well past the point a missing one would show.
	time.Sleep(testLease + 500*time.Millisecond)

	if !liveIs(a.m, "n1", "n2")() || !liveIs(b.m, "n1", "n2")() {
		t.Fatalf("live sets drifted: A %v, B %v", a.m.Live(), b.m.Live())
	}

	if len(a.ev.fenceTimes()) != 0 || len(b.ev.fenceTimes()) != 0 {
		t.Fatal("a healthy node was fenced")
	}

	if len(a.ev.deadEvents("")) != 0 || len(b.ev.deadEvents("")) != 0 {
		t.Fatal("a live peer was declared dead")
	}
}

func TestPeerDeadAfterLease(t *testing.T) {
	cell := startMemberCell(t)
	a := cell.nodeOn(t, "n1", 0)
	b := cell.nodeOn(t, "n2", 1)

	eventually(t, 5*time.Second, "A to see B", liveIs(a.m, "n1", "n2"))

	stopped := time.Now()

	b.m.Stop()

	eventually(t, testDeadAfter+3*time.Second, "OnPeerDead(n2)", func() bool {
		return len(a.ev.deadEvents("n2")) > 0
	})

	got := a.ev.deadEvents("n2")[0].at.Sub(stopped)

	// Never before the lease; the margin is fixed at 2 s on top. B's last
	// heartbeat landed up to one heartbeat before Stop, so allow that and a
	// monitor tick on the early side.
	if got < testLease {
		t.Fatalf("declared dead after %v, before the lease %v", got, testLease)
	}

	if got < testDeadAfter-testHeartbeat-100*time.Millisecond {
		t.Fatalf("declared dead after %v, want about lease + 2 s = %v", got, testDeadAfter)
	}

	if got > testDeadAfter+1500*time.Millisecond {
		t.Fatalf("declared dead only after %v, want about %v", got, testDeadAfter)
	}

	if !liveIs(a.m, "n1")() {
		t.Fatalf("live after death = %v", a.m.Live())
	}

	if !a.m.IsLeaderFor("n2") {
		t.Fatal("the only live node is not the leader for the dead one")
	}

	if d := a.m.Dead(); len(d) != 1 || d[0] != "n2" {
		t.Fatalf("Dead() = %v, want [n2]", d)
	}

	// Once: nothing more arrives.
	time.Sleep(500 * time.Millisecond)

	if n := len(a.ev.deadEvents("n2")); n != 1 {
		t.Fatalf("OnPeerDead fired %d times, want 1", n)
	}

	if len(b.ev.fenceTimes()) != 0 || len(b.ev.deadEvents("")) != 0 {
		t.Fatal("a stopped node called back")
	}

	// The same node ID heartbeating again is live again, with no callback.
	b2 := b.newMembers()
	if err := b2.Start(tctx(t, 10*time.Second)); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(b2.Stop)

	eventually(t, 5*time.Second, "A to see B again", liveIs(a.m, "n1", "n2"))

	if d := a.m.Dead(); len(d) != 0 {
		t.Fatalf("Dead() after the peer came back = %v, want none", d)
	}

	if n := len(a.ev.deadEvents("n2")); n != 1 {
		t.Fatalf("OnPeerDead fired %d times after the peer came back, want 1", n)
	}
}

func TestFenceWhenNATSUnreachable(t *testing.T) {
	cell := startMemberCell(t)

	proxy, err := chaos.NewProxy(strings.TrimPrefix(cell.urls[0], "nats://"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(proxy.Close)

	a := cell.nodeOn(t, "n1", 1)
	p := cell.node(t, "n2", "nats://"+proxy.Addr())

	eventually(t, 5*time.Second, "both to see both", func() bool {
		return liveIs(a.m, "n1", "n2")() && liveIs(p.m, "n1", "n2")()
	})

	if p.m.Fenced() {
		t.Fatal("fenced before the fault")
	}

	// Silence, not a reset: the client has no way to tell, and nats.go's own
	// ping interval (minutes) cannot hide the fence.
	cut := time.Now()

	proxy.SetMode(chaos.Both, chaos.Blackhole)

	eventually(t, testLease, "OnFence", func() bool { return len(p.ev.fenceTimes()) > 0 })

	got := p.ev.fenceTimes()[0].Sub(cut)

	// Lease/2 after the last acknowledged write, which was up to one heartbeat
	// before the cut; the monitor checks every heartbeat/4.
	if got > testLease/2+300*time.Millisecond {
		t.Fatalf("fenced after %v, want within lease/2 = %v plus slack", got, testLease/2)
	}

	if got < testLease/2-testHeartbeat-100*time.Millisecond {
		t.Fatalf("fenced after only %v", got)
	}

	if !p.m.Fenced() {
		t.Fatal("Fenced() is false after OnFence")
	}

	if p.m.Live() != nil || p.m.IsLeaderFor("n1") {
		t.Fatalf("a fenced node claims a view: live %v, leader %v", p.m.Live(), p.m.IsLeaderFor("n1"))
	}

	// OnFence fires once, however long the fence lasts.
	time.Sleep(testLease)

	if n := len(p.ev.fenceTimes()); n != 1 {
		t.Fatalf("OnFence fired %d times, want 1", n)
	}

	if len(p.ev.unfenceTimes()) != 0 {
		t.Fatal("unfenced while still cut off")
	}

	// Heal: the held bytes flow, writes are acknowledged, the fence lifts.
	proxy.SetMode(chaos.Both, chaos.Pass)

	eventually(t, 5*time.Second, "OnUnfence", func() bool { return len(p.ev.unfenceTimes()) > 0 })

	if p.m.Fenced() {
		t.Fatal("still fenced after OnUnfence")
	}

	eventually(t, 5*time.Second, "P to see A again", liveIs(p.m, "n1", "n2"))

	if n := len(p.ev.fenceTimes()); n != 1 {
		t.Fatalf("OnFence fired %d times in all, want 1", n)
	}
}

func TestFencedNodeDeclaresNobodyDead(t *testing.T) {
	cell := startMemberCell(t)

	proxy, err := chaos.NewProxy(strings.TrimPrefix(cell.urls[0], "nats://"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(proxy.Close)

	a := cell.nodeOn(t, "n1", 1)
	p := cell.node(t, "n2", "nats://"+proxy.Addr())

	eventually(t, 5*time.Second, "both to see both", func() bool {
		return liveIs(a.m, "n1", "n2")() && liveIs(p.m, "n1", "n2")()
	})

	proxy.SetMode(chaos.Both, chaos.Blackhole)

	// Long enough for each side to call the other dead, if it could.
	eventually(t, testDeadAfter+3*time.Second, "A to declare P dead", func() bool {
		return len(a.ev.deadEvents("n2")) > 0
	})

	time.Sleep(500 * time.Millisecond)

	if n := len(p.ev.deadEvents("")); n != 0 {
		t.Fatalf("a node that is cut off declared %d peers dead", n)
	}

	if !liveIs(a.m, "n1")() {
		t.Fatalf("A's live set while P is cut off = %v", a.m.Live())
	}

	proxy.SetMode(chaos.Both, chaos.Pass)

	eventually(t, 5*time.Second, "P unfenced", func() bool { return !p.m.Fenced() })
	eventually(t, 5*time.Second, "A to see P again", liveIs(a.m, "n1", "n2"))
	eventually(t, 5*time.Second, "P to see A", liveIs(p.m, "n1", "n2"))

	// P gave A a full window after the heal instead of calling it dead from
	// the silence it could not interpret.
	time.Sleep(testLease)

	if n := len(p.ev.deadEvents("")); n != 0 {
		t.Fatalf("P declared %d peers dead after the heal", n)
	}
}

func TestLowestIDDoesTheWork(t *testing.T) {
	cell := startMemberCell(t)
	a := cell.nodeOn(t, "n1", 0)
	b := cell.nodeOn(t, "n2", 1)
	c := cell.nodeOn(t, "n3", 2)

	for _, n := range []*memberNode{a, b, c} {
		eventually(t, 5*time.Second, n.cfg.NodeID+" to see all", liveIs(n.m, "n1", "n2", "n3"))
	}

	c.m.Stop()

	for _, n := range []*memberNode{a, b} {
		eventually(t, testDeadAfter+3*time.Second, n.cfg.NodeID+" to see n3 dead", func() bool {
			return len(n.ev.deadEvents("n3")) > 0
		})
	}

	if !a.m.IsLeaderFor("n3") {
		t.Fatal("n1 is not the leader for n3")
	}

	if b.m.IsLeaderFor("n3") {
		t.Fatal("n2 is also the leader for n3")
	}

	// The dead node is excluded, and a node is never the leader for itself.
	if a.m.IsLeaderFor("n1") {
		t.Fatal("n1 is the leader for itself")
	}

	if !b.m.IsLeaderFor("n1") {
		t.Fatal("n2 is not the leader for a hypothetically dead n1")
	}

	// Each dead event fired once on each survivor.
	for _, n := range []*memberNode{a, b} {
		if got := len(n.ev.deadEvents("n3")); got != 1 {
			t.Fatalf("%s saw n3 die %d times", n.cfg.NodeID, got)
		}
	}
}

func TestStopIsFinal(t *testing.T) {
	cell := startMemberCell(t)
	a := cell.nodeOn(t, "n1", 0)

	a.m.Stop()
	a.m.Stop() // idempotent

	if err := a.m.Start(tctx(t, 5*time.Second)); err == nil {
		t.Fatal("Start after Stop succeeded")
	}

	// A stopped node must not fence itself when its connection goes: nothing
	// runs any more.
	a.conn.Close()

	time.Sleep(testLease)

	if n := len(a.ev.fenceTimes()); n != 0 {
		t.Fatalf("OnFence fired %d times after Stop", n)
	}

	m := NewMembers(a.conn, a.cfg)
	if m.Fenced() || m.IsLeaderFor("x") {
		t.Fatal("a Members that never started claims a role")
	}

	m.Stop()
}

func TestStartTwiceAndMissingBucket(t *testing.T) {
	cell := startMemberCell(t)
	a := cell.nodeOn(t, "n1", 0)

	if err := a.m.Start(tctx(t, 5*time.Second)); err == nil {
		t.Fatal("second Start succeeded")
	}

	// A cell nobody provisioned has no members bucket.
	cfg := memberConfig("n1", cell.urls[0])
	cfg.Cell = "other"

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	m := NewMembers(c, cfg)

	err = m.Start(tctx(t, 5*time.Second))
	if err == nil {
		m.Stop()
		t.Fatal("Start without a bucket succeeded")
	}

	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("a missing bucket reads as unavailable: %v", err)
	}
}

func TestFenceOnClosedConnection(t *testing.T) {
	cell := startMemberCell(t)
	a := cell.nodeOn(t, "n1", 0)

	cut := time.Now()

	a.conn.Close()

	eventually(t, testLease, "OnFence after the connection closed", func() bool {
		return len(a.ev.fenceTimes()) > 0
	})

	if got := a.ev.fenceTimes()[0].Sub(cut); got > testLease/2+300*time.Millisecond {
		t.Fatalf("fenced after %v", got)
	}
}

// A members watch whose subscription closes under it (nats.go ends a KV
// watcher whose consumer it cannot recreate) is started again. Before, the
// loop returned silently: the node never heard a peer join and, with its own
// heartbeat unseen, never declared one dead.
func TestMembersWatchRestarts(t *testing.T) {
	cell := startMemberCell(t)
	a := cell.nodeOn(t, "n1", 0)
	b := cell.nodeOn(t, "n2", 1)

	eventually(t, 5*time.Second, "A to see B", liveIs(a.m, "n1", "n2"))

	a.m.mu.Lock()
	w := a.m.watcher
	a.m.mu.Unlock()

	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}

	cell.nodeOn(t, "n3", 2)

	eventually(t, 10*time.Second, "A to see C join after its watch ended", liveIs(a.m, "n1", "n2", "n3"))

	b.m.Stop()

	eventually(t, testDeadAfter+5*time.Second, "OnPeerDead(n2) on A", func() bool {
		return len(a.ev.deadEvents("n2")) > 0
	})

	if len(a.ev.fenceTimes()) != 0 {
		t.Fatal("A was fenced")
	}
}

// proxiedCluster is three in-process JetStream servers whose routes all run
// through fault proxies (routes[i][j] carries server i's route to server j),
// and that advertise a cluster address nothing listens on, so no route goes
// around them. Isolating a server leaves it running, the way kill -9 or a
// partition does: no shutdown event, no leadership handoff, so its peers sit
// out a real raft election (4 to 9 s in nats-server 2.15).
type proxiedCluster struct {
	servers []*server.Server
	routes  [3][3]*chaos.Proxy
}

func startProxiedCluster(t *testing.T) *proxiedCluster {
	t.Helper()

	ports := clustertest.FreePorts(t, 4)
	cluster, dead := ports[:3], ports[3]
	pc := &proxiedCluster{}

	for i := range 3 {
		for j := range 3 {
			if i == j {
				continue
			}

			p, err := chaos.NewProxy(fmt.Sprintf("127.0.0.1:%d", cluster[j]))
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(p.Close)

			pc.routes[i][j] = p
		}
	}

	for i := range 3 {
		var routes []string

		for j := range 3 {
			if j != i {
				routes = append(routes, "nats-route://"+pc.routes[i][j].Addr())
			}
		}

		opts := &server.Options{
			ServerName:   fmt.Sprintf("s%d", i),
			Host:         "127.0.0.1",
			Port:         -1,
			NoLog:        true,
			NoSigs:       true,
			JetStream:    true,
			StoreDir:     t.TempDir(),
			PingInterval: time.Second,
			MaxPingsOut:  3,
			Cluster: server.ClusterOpts{
				Name: clustertest.ClusterName, Host: "127.0.0.1", Port: cluster[i],
				Advertise: fmt.Sprintf("127.0.0.1:%d", dead),
			},
			Routes: server.RoutesFromStr(strings.Join(routes, ",")),
		}

		s, err := server.NewServer(opts)
		if err != nil {
			t.Fatal(err)
		}

		pc.servers = append(pc.servers, s)

		go s.Start()
	}

	t.Cleanup(func() {
		for _, s := range pc.servers {
			s.Shutdown()
			s.WaitForShutdown()
		}
	})

	for _, s := range pc.servers {
		if !s.ReadyForConnections(30 * time.Second) {
			t.Fatal("server not ready")
		}
	}

	eventually(t, 30*time.Second, "a meta leader with three peers", func() bool {
		for _, s := range pc.servers {
			if s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == 3 {
				return true
			}
		}

		return false
	})

	return pc
}

// isolate cuts server i's routes both ways.
func (pc *proxiedCluster) isolate(i int) {
	for j := range 3 {
		if j != i {
			pc.routes[i][j].SetMode(chaos.Both, chaos.Blackhole)
			pc.routes[j][i].SetMode(chaos.Both, chaos.Blackhole)
		}
	}
}

// Ruling R47 with the R48 default lease: losing the server that leads the
// members bucket, with quorum left, costs a raft election of 4 to 9 s, and
// the node must ride it out. It is not fenced, and it renews within 1.5 s of
// the bucket having a leader again, not at its next 2 s heartbeat or after
// a write that hung for Lease/4, because a failed write is retried every
// 250 ms with each attempt bounded by 500 ms. (A write that went out just
// before the leader appeared can still take its 500 ms, or wait out the
// stream's delayed 503, before the next one: 1.5 s covers both.)
func TestMembersRideOutBucketLeaderLoss(t *testing.T) {
	pc := startProxiedCluster(t)

	connect := func(id string, i int) *Conn {
		cfg := memberConfig(id, pc.servers[i].ClientURL())
		cfg.Heartbeat = DefaultHeartbeat
		cfg.Lease = DefaultLease

		c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(c.Close)

		return c
	}

	setup := connect("setup", 0)
	cfg := memberConfig("setup", pc.servers[0].ClientURL())

	if err := Provision(tctx(t, 30*time.Second), setup, cfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	bucket := "KV_" + cfg.MembersBucket()

	leaderOf := func(c *Conn) string {
		st, err := c.js.Stream(tctx(t, 2*time.Second), bucket)
		if err != nil {
			return ""
		}

		info, err := st.Info(tctx(t, 2*time.Second))
		if err != nil || info.Cluster == nil {
			return ""
		}

		return info.Cluster.Leader
	}

	var leader string

	eventually(t, 20*time.Second, "a members bucket leader", func() bool {
		leader = leaderOf(setup)

		return leader != ""
	})

	victim, survivor := -1, -1

	for i, s := range pc.servers {
		switch {
		case s.Name() == leader:
			victim = i
		case survivor < 0:
			survivor = i
		}
	}

	if victim < 0 {
		t.Fatalf("no server named %s", leader)
	}

	// The node and the observer talk to a server that stays in the majority,
	// so only the bucket's election is in the way.
	conn := connect("n1", survivor)
	obs := connect("obs", survivor)

	ncfg := memberConfig("n1", pc.servers[survivor].ClientURL())
	ncfg.Heartbeat = DefaultHeartbeat
	ncfg.Lease = DefaultLease

	n := &memberNode{cfg: ncfg, conn: conn, ev: &memberEvents{}}
	n.m = n.newMembers()

	if err := n.m.Start(tctx(t, 10*time.Second)); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(n.m.Stop)

	time.Sleep(2 * ncfg.Heartbeat)

	cut := time.Now()

	pc.isolate(victim)

	var elected time.Time

	eventually(t, 30*time.Second, "a new members bucket leader", func() bool {
		if l := leaderOf(obs); l != "" && l != leader {
			elected = time.Now()

			return true
		}

		return false
	})

	renewedAfter := func() bool {
		n.m.mu.Lock()
		defer n.m.mu.Unlock()

		return n.m.lastRenew.After(elected)
	}

	eventually(t, 5*time.Second, "a renew after the election", renewedAfter)

	n.m.mu.Lock()
	gap := n.m.lastRenew.Sub(elected)
	n.m.mu.Unlock()

	t.Logf("leader %s cut off; new leader seen after %s; renewed %s after that", leader,
		elected.Sub(cut).Round(time.Millisecond), gap.Round(time.Millisecond))

	if elected.Sub(cut) < 2*time.Second {
		t.Fatalf("new leader after %s: no real election happened", elected.Sub(cut))
	}

	if gap > 1500*time.Millisecond {
		t.Fatalf("renewed %s after the bucket had a leader again, want within 1.5 s", gap)
	}

	if n.m.Fenced() || len(n.ev.fenceTimes()) != 0 {
		t.Fatal("the node fenced over one server's loss")
	}
}
