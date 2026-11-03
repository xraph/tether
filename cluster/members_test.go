package cluster

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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

	nc := clustertest.StartCluster(t, 3)
	urls := nc.URLs()
	cfg := memberConfig("setup", urls[0])

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	if err := Provision(tctx(t, 30*time.Second), c, cfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	nc.WaitInterest(t, cellSubjects(cfg)...)

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
	// Validate keeps LoadInterval at or under Lease/2.
	cfg.LoadInterval = testLease / 2

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

// A first heartbeat that gets no responders is sent again. The members
// bucket has none for a moment while its leader is being elected, or right
// after Provision made it, before its leader's interest reaches the node's
// server; on a loaded runner that outlasted nats.go's own two retries and
// Start failed with "no response from stream", so the node never started.
func TestStartRetriesUnansweredFirstHeartbeat(t *testing.T) {
	cell := startMemberCell(t)
	cfg := memberConfig("n1", cell.urls[0])

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	flaky := &flakyJS{JetStream: c.js, putMisses: 3}
	c.js = flaky

	m := NewMembers(c, cfg)
	t.Cleanup(m.Stop)

	if err := m.Start(tctx(t, 10*time.Second)); err != nil {
		t.Fatalf("Start through three unanswered heartbeats: %v", err)
	}

	if puts := flaky.putCount(); puts < 4 {
		t.Fatalf("%d heartbeat writes, want the three unanswered ones and one more", puts)
	}

	eventually(t, 5*time.Second, "the node to see itself", liveIs(m, "n1"))
}

// Every JetStream request of Members.Start goes again while nobody answers
// it: opening the bucket, the first heartbeat and the watch. On a loaded
// runner the watch's consumer create went unanswered for its whole 5 s and
// Start failed with "watch members: context deadline exceeded". An error
// that is not a lack of an answer (here a refused open) fails Start at once.
func TestStartRetriesUnansweredRequests(t *testing.T) {
	cell := startMemberCell(t)
	cfg := memberConfig("n1", cell.urls[0])

	c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(c.Close)

	plain := c.js
	flaky := &flakyJS{JetStream: plain, openMisses: map[string]int{cfg.MembersBucket(): 2}, putMisses: 2, watchMisses: 2}
	c.js = flaky

	m := NewMembers(c, cfg)
	t.Cleanup(m.Stop)

	if err := m.Start(tctx(t, 15*time.Second)); err != nil {
		t.Fatalf("Start through unanswered opens, heartbeats and watches: %v", err)
	}

	if left := flaky.left(); left != 0 {
		t.Fatalf("%d unanswered requests left, so Start did not send them again", left)
	}

	eventually(t, 5*time.Second, "the node to see itself", liveIs(m, "n1"))

	refused := errors.New("permission denied")
	c.js = &flakyJS{JetStream: plain, openErr: refused}

	m2 := NewMembers(c, memberConfig("n2", cell.urls[0]))
	t.Cleanup(m2.Stop)

	began := time.Now()

	if err := m2.Start(tctx(t, 15*time.Second)); !errors.Is(err, refused) {
		t.Fatalf("Start with a refused open: want the refusal, got %v", err)
	}

	if took := time.Since(began); took > retryGap {
		t.Fatalf("a refused open took %s to fail, so it was retried", took)
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

// Ruling R47 with the R48 default lease: losing the server that leads the
// members bucket, with quorum left, costs a raft election of 4 to 9 s, and
// the node must ride it out without fencing. The cut-off leader swallows the
// writes sent to it without a reply (a stream member drops a publish it
// cannot lead), so the node's two write slots fill with writes that end only
// at their Lease/4 bound (6 s); after that a write goes every 250 ms. So the
// first renew after the cut comes once the bucket has a leader again and
// those writes have timed out, give or take a retry, and well before the
// 12 s fence.
func TestMembersRideOutBucketLeaderLoss(t *testing.T) {
	pc := clustertest.StartProxied(t)

	connect := func(id string, i int) *Conn {
		cfg := memberConfig(id, pc.Servers[i].ClientURL())
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
	cfg := memberConfig("setup", pc.Servers[0].ClientURL())

	if err := Provision(tctx(t, 30*time.Second), setup, cfg, time.Hour); err != nil {
		t.Fatal(err)
	}

	bucket := "KV_" + cfg.MembersBucket()

	// Short asks, so a leaderless stream (no answer) does not hold the
	// polling back for long and "leader seen" is close to the election.
	leaderOf := func(c *Conn) string {
		st, err := c.js.Stream(tctx(t, 300*time.Millisecond), bucket)
		if err != nil {
			return ""
		}

		info, err := st.Info(tctx(t, 300*time.Millisecond))
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

	for i, s := range pc.Servers {
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

	ncfg := memberConfig("n1", pc.Servers[survivor].ClientURL())
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

	pc.Isolate(victim)

	var elected time.Time

	eventually(t, 30*time.Second, "a new members bucket leader", func() bool {
		if l := leaderOf(obs); l != "" && l != leader {
			elected = time.Now()

			return true
		}

		return false
	})

	// A write in flight at the cut may still be acknowledged just after it;
	// the renew that counts comes later.
	renewedAfterCut := func() bool {
		n.m.mu.Lock()
		defer n.m.mu.Unlock()

		return n.m.lastRenew.After(cut.Add(200 * time.Millisecond))
	}

	eventually(t, 15*time.Second, "a renew after the cut", renewedAfterCut)

	n.m.mu.Lock()
	renewed := n.m.lastRenew
	n.m.mu.Unlock()

	// The swallowed writes went out at most a retry after the cut and end
	// at Lease/4; after that, and once there is a leader, a write lands
	// within a retry or two.
	due := cut.Add(n.m.putTimeout() + renewRetry)
	if elected.After(due) {
		due = elected
	}

	t.Logf("leader %s cut off; new leader seen after %s; first renew after the cut at %s (due by %s)", leader,
		elected.Sub(cut).Round(time.Millisecond), renewed.Sub(cut).Round(time.Millisecond), due.Sub(cut).Round(time.Millisecond))

	if elected.Sub(cut) < 2*time.Second {
		t.Fatalf("new leader after %s: no real election happened", elected.Sub(cut))
	}

	if late := renewed.Sub(due); late > time.Second {
		t.Fatalf("renewed %s after the cut, %s past when it was due", renewed.Sub(cut), late)
	}

	if n.m.Fenced() || len(n.ev.fenceTimes()) != 0 {
		t.Fatal("the node fenced over one server's loss")
	}
}

// A members bucket that is slow but working never fences: a heartbeat write
// may take up to Lease/4, and the retries that go out every 250 ms while it
// is outstanding run alongside it rather than cutting it short. Here every
// write takes over 600 ms (the node's link delayed 300 ms each way) for 5 s,
// more than twice Lease/2, with a 4 s lease (Lease/4 = 1 s). A fixed 500 ms
// bound on a write failed every one of them and fenced the node at 2 s.
func TestMembersSlowWritesDoNotFence(t *testing.T) {
	cell := startMemberCell(t)

	proxy, err := chaos.NewProxy(strings.TrimPrefix(cell.urls[0], "nats://"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(proxy.Close)

	cfg := memberConfig("n1", "nats://"+proxy.Addr())
	cfg.Heartbeat = 200 * time.Millisecond
	cfg.Lease = 4 * time.Second

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

	t.Cleanup(n.m.Stop)

	proxy.SetDelay(chaos.Both, 300*time.Millisecond)
	proxy.SetMode(chaos.Both, chaos.Delay)

	slowFrom := time.Now()
	renews := 0
	last := time.Time{}

	for time.Since(slowFrom) < 5*time.Second {
		time.Sleep(50 * time.Millisecond)

		n.m.mu.Lock()
		lr := n.m.lastRenew
		n.m.mu.Unlock()

		if lr.After(slowFrom) && lr.After(last) {
			renews++
			last = lr
		}
	}

	proxy.SetMode(chaos.Both, chaos.Pass)

	t.Logf("%d renews in 5 s of 600 ms writes", renews)

	if n.m.Fenced() || len(n.ev.fenceTimes()) != 0 {
		t.Fatal("fenced while every heartbeat write was landing within Lease/4")
	}

	if renews < 3 {
		t.Fatalf("%d renews in 5 s, want them to keep landing", renews)
	}
}

// A bucket that is slow but working gets one heartbeat write per Heartbeat,
// not two in flight for as long as it stays slow. With the production timings
// (2 s heartbeat, 24 s lease) the node is behind a link delayed 200 ms each
// way, so every write takes over 400 ms, longer than the 250 ms retry. Over
// 10 s it must send about five writes, as on a fast link, keep renewing and
// never fence. (Going back to the Heartbeat only once nothing was in flight
// sent about 40.)
func TestMembersHeartbeatCadenceOnSlowBucket(t *testing.T) {
	cell := startMemberCell(t)

	proxy, err := chaos.NewProxy(strings.TrimPrefix(cell.urls[0], "nats://"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(proxy.Close)

	cfg := memberConfig("n1", "nats://"+proxy.Addr())
	cfg.Heartbeat = DefaultHeartbeat
	cfg.Lease = DefaultLease

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

	t.Cleanup(n.m.Stop)

	const window = 10 * time.Second

	count := func() (writes int64, renews int) {
		from := time.Now()
		before := n.m.writes.Load()

		var last time.Time

		for time.Since(from) < window {
			time.Sleep(50 * time.Millisecond)

			n.m.mu.Lock()
			lr := n.m.lastRenew
			n.m.mu.Unlock()

			if lr.After(from) && lr.After(last) {
				renews++
				last = lr
			}
		}

		return n.m.writes.Load() - before, renews
	}

	fastWrites, fastRenews := count()

	proxy.SetDelay(chaos.Both, 200*time.Millisecond)
	proxy.SetMode(chaos.Both, chaos.Delay)

	slowWrites, slowRenews := count()

	t.Logf("in %s: fast link %d writes, %d renews; slow link %d writes, %d renews", window, fastWrites, fastRenews, slowWrites, slowRenews)

	want := int64(window / cfg.Heartbeat)

	for _, got := range []int64{fastWrites, slowWrites} {
		if got < want-1 || got > want+2 {
			t.Fatalf("%d writes in %s, want about %d (one per heartbeat)", got, window, want)
		}
	}

	if slowRenews < int(want)-2 {
		t.Fatalf("%d renews in %s on the slow link, want about %d", slowRenews, window, want)
	}

	if n.m.Fenced() || len(n.ev.fenceTimes()) != 0 {
		t.Fatal("fenced on a slow but working bucket")
	}
}
