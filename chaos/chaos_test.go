//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xraph/tether/cluster"
)

// TestMain lets the test binary serve as its own child processes: a
// nats-server, or the supervisor of a broker.
func TestMain(m *testing.M) {
	if ran, code := childMain(); ran {
		os.Exit(code)
	}

	code := m.Run()

	removeBinary()
	os.Exit(code)
}

// result logs one line the README's results table is built from.
func result(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("RESULT %s: %s", t.Name(), fmt.Sprintf(format, args...))
}

// subscribe connects n persistent subscribers to node nd, subscribed to
// filter, and waits until the cell has stored their sessions with the
// subscription. Each setup runs on a subscriber before it connects.
func subscribe(t *testing.T, c *testCell, prefix, filter string, count int, nd *node, setup ...func(*subscriber)) []*subscriber {
	t.Helper()

	subs := make([]*subscriber, 0, count)

	for i := range count {
		s := newSubscriber(c, fmt.Sprintf("%s-%d", prefix, i), filter)

		for _, f := range setup {
			f(s)
		}

		cn, err := s.connect(nd)
		if err != nil {
			t.Fatalf("%s: connect to %s: %v", s.id, nd.id, err)
		}

		if cn.sessionPresent {
			t.Fatalf("%s: a new session came back present", s.id)
		}

		t.Cleanup(s.close)

		subs = append(subs, s)
	}

	waitOwned(t, c, subs, nd.id)

	return subs
}

// waitOwned waits until every subscriber's session record names owner and
// holds its subscription.
func waitOwned(t *testing.T, c *testCell, subs []*subscriber, owner string) {
	t.Helper()

	for _, s := range subs {
		key := c.sessionKey(s.id)

		eventually(t, 15*time.Second, s.id+" owned by "+owner, func() bool {
			rec := c.obs.record(key)

			return rec != nil && rec.Owner == owner && len(rec.Subscriptions) > 0
		})
	}
}

// baseline is the log's last sequence before a scenario publishes anything:
// every later message of the tenant matches every subscriber.
func baseline(t *testing.T, c *testCell) uint64 {
	t.Helper()

	return c.obs.streamInfo(t).State.LastSeq
}

// assertNoLoss waits up to d for every subscriber to have every payload a
// publisher got a successful PUBACK for, and fails naming what is missing.
func assertNoLoss(t *testing.T, d time.Duration, pubs []*publisher, subs []*subscriber) {
	t.Helper()

	acked := make(map[string]time.Time)

	for _, p := range pubs {
		for k, v := range p.stats().acked {
			acked[k] = v
		}
	}

	if len(acked) == 0 {
		t.Fatal("no publish was acknowledged at all")
	}

	missing := func(s *subscriber) []string {
		var out []string

		for k := range acked {
			if _, ok := s.has(k); !ok {
				out = append(out, k)
			}
		}

		sort.Strings(out)

		return out
	}

	deadline := time.Now().Add(d)

	for {
		total := 0
		for _, s := range subs {
			total += len(missing(s))
		}

		if total == 0 {
			break
		}

		if time.Now().After(deadline) {
			for _, s := range subs {
				if m := missing(s); len(m) > 0 {
					t.Errorf("%s lost %d of %d acknowledged messages, first: %v", s.id, len(m), len(acked), m[:min(len(m), 10)])
				}
			}

			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	dups := 0

	for _, s := range subs {
		_, _, d := s.snapshot()
		dups += d
	}

	result(t, "no loss: %d acknowledged messages reached all %d subscribers (%d duplicates in all)", len(acked), len(subs), dups)
}

// assertNoCursorSkips checks every session record the observer saw for the
// subscribers against the log: when a record says the session is done with a
// log sequence (at or below Cursor, or at or below DoneThrough and not
// Pending), the subscriber must already have received that message. A record
// that runs ahead of the client is a cursor skip: a node resuming from it
// would never send what was skipped.
func assertNoCursorSkips(t *testing.T, c *testCell, from uint64, subs []*subscriber) {
	t.Helper()

	entries := c.obs.readLog(t, c.tenant, from)

	checked, violations := 0, 0

	for _, s := range subs {
		prefix := strings.TrimSuffix(s.filter, "#")
		got, _, _ := s.snapshot()

		var first string

		for _, ev := range c.obs.events(c.sessionKey(s.id)) {
			rec := ev.rec
			if rec == nil || rec.DoneThrough <= from && rec.Cursor <= from {
				continue
			}

			checked++

			for _, e := range entries {
				if e.seq > rec.DoneThrough && e.seq > rec.Cursor {
					break
				}

				if !strings.HasPrefix(e.topic, prefix) {
					continue
				}

				done := e.seq <= rec.Cursor || e.seq <= rec.DoneThrough && !slices.Contains(rec.Pending, e.seq)
				if !done {
					continue
				}

				at, ok := got[e.payload]
				if ok && at.Before(ev.seen) {
					continue
				}

				violations++

				if first == "" {
					first = fmt.Sprintf("record rev %d (owner %s, cursor %d, done through %d, pending %v) seen at %s covers seq %d (%s), which the client had not received then (received %t)",
						ev.rev, rec.Owner, rec.Cursor, rec.DoneThrough, rec.Pending, ev.seen.Format("15:04:05.000"), e.seq, e.payload, ok)
				}
			}
		}

		if first != "" {
			t.Errorf("%s: cursor skip: %s", s.id, first)
		}
	}

	if checked == 0 {
		t.Error("no session record with a cursor past the baseline was seen: nothing was checked")
	}

	if violations == 0 {
		result(t, "no cursor skips: %d record updates checked against %d log messages", checked, len(entries))
	}
}

// fences counts how often each broker fenced itself, from its log.
func fences(c *testCell) []int {
	out := make([]int, brokerSize)
	for i := range brokerSize {
		out[i] = c.logCount(i, "tether: fenced from the cell,")
	}

	return out
}

// TestKill9BrokerDuringQoS1 is the spec's success criterion: persistent
// subscribers on node 2, a publisher on node 1 at 500 msg/s QoS 1, and a
// kill -9 of node 2. The subscribers reconnect to node 3. Every message the
// publisher got a PUBACK for reaches every subscriber at least once, and each
// subscriber has caught up within 10 s of reconnecting.
func TestKill9BrokerDuringQoS1(t *testing.T) {
	c := startCell(t)
	n1, n3 := c.nodes[0], c.nodes[2]

	subs := subscribe(t, c, "k9", "chaos/k9/#", 5, c.nodes[1])
	from := baseline(t, c)

	pub := newPublisher(c, "p1", "chaos/k9/p1", 500, n1)
	pub.start()

	// Somewhere inside a cursor flush interval, so runs do not all kill just
	// after a flush.
	time.Sleep(5*time.Second + rand.N(time.Second))

	if pub.ackCount() == 0 {
		t.Fatal("nothing acknowledged before the kill")
	}

	killed := c.killNode(1)
	reconnected := make(map[string]time.Time, len(subs))

	for _, s := range subs {
		if !s.waitDrop(5 * time.Second) {
			t.Fatalf("%s still connected 5 s after the kill", s.id)
		}

		cn, err := s.connect(n3)
		if err != nil {
			t.Fatalf("%s: reconnect to n3: %v", s.id, err)
		}

		if !cn.sessionPresent {
			t.Errorf("%s: session not present on n3", s.id)
		}

		reconnected[s.id] = cn.at
	}

	time.Sleep(5 * time.Second)
	pub.stop()

	st := pub.stats()

	assertNoLoss(t, 30*time.Second, []*publisher{pub}, subs)

	var worst time.Duration

	for _, s := range subs {
		at := reconnected[s.id]

		// Caught up: every message acknowledged before the reconnect has
		// arrived.
		var caught time.Time

		for k, ackAt := range st.acked {
			if !ackAt.Before(at) {
				continue
			}

			if r, ok := s.has(k); ok && r.After(caught) {
				caught = r
			}
		}

		resume := max(caught.Sub(at), 0)
		worst = max(worst, resume)

		if resume > resumeBound {
			t.Errorf("%s caught up %s after reconnecting, over %s", s.id, resume, resumeBound)
		}

		if rec := c.obs.record(c.sessionKey(s.id)); rec == nil || rec.Owner != "n3" {
			t.Errorf("%s: record after the move: %+v", s.id, rec)
		}
	}

	assertNoCursorSkips(t, c, from, subs)

	result(t, "published %d, acknowledged %d, %d without PUBACK; reconnected %s after the kill; slowest catch-up %s (bound %s); fences %v",
		len(st.sent), len(st.acked), len(st.sent)-len(st.acked), firstAfter(reconnected, killed), worst.Round(time.Millisecond), resumeBound, fences(c))
}

// firstAfter is the longest gap from t0 to the times in m.
func firstAfter(m map[string]time.Time, t0 time.Time) time.Duration {
	var d time.Duration
	for _, at := range m {
		d = max(d, at.Sub(t0))
	}

	return d.Round(time.Millisecond)
}

// TestKill9NATSServer kills the NATS server that leads the log stream while a
// publisher runs at 500 msg/s QoS 1 and persistent subscribers on two nodes
// listen. The brokers run with the production heartbeat and lease
// (cluster.DefaultHeartbeat and cluster.DefaultLease): two servers still make
// a quorum, so no broker may fence. Nothing
// acknowledged is lost, and publishing resumes within the JetStream leader
// election time.
func TestKill9NATSServer(t *testing.T) {
	c := startCell(t, withTimings(cluster.DefaultHeartbeat, cluster.DefaultLease))
	n1, n2, n3 := c.nodes[0], c.nodes[1], c.nodes[2]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subs := subscribe(t, c, "kn2", "chaos/kn/#", 3, n2)
	for _, s := range subs {
		s.keepConnected(ctx, n2, n3)
	}

	subs3 := subscribe(t, c, "kn3", "chaos/kn/#", 3, n3)
	for _, s := range subs3 {
		s.keepConnected(ctx, n3, n2)
	}

	subs = append(subs, subs3...)
	from := baseline(t, c)

	pub := newPublisher(c, "p1", "chaos/kn/p1", 500, n1, n2, n3)
	pub.start()

	time.Sleep(5 * time.Second)

	if pub.ackCount() == 0 {
		t.Fatal("nothing acknowledged before the kill")
	}

	leaderName := c.obs.streamInfo(t).Cluster.Leader
	leader := c.serverIndex(leaderName)
	killed := c.killServer(leader)
	elected := c.obs.waitNewLeader(t, leaderName).Sub(killed)

	// Publishing has resumed once PUBACKs flow at half the offered rate or
	// more again. Keep going without it, so the loss checks still run.
	var (
		resumed time.Duration
		ok      bool
	)

	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if resumed, ok = resumedAfter(pub.stats(), killed, pub.rate); ok {
			break
		}
	}

	switch {
	case !ok:
		t.Errorf("publishing had not resumed 90 s after the kill; the new log leader was elected after %s", elected.Round(time.Millisecond))
	case resumed > elected+electionSlack:
		t.Errorf("publishing resumed %s after the kill; the new log leader was elected after %s (bound: election + %s)",
			resumed.Round(time.Millisecond), elected.Round(time.Millisecond), electionSlack)
	}

	time.Sleep(5 * time.Second)
	pub.stop()

	st := pub.stats()

	// The subscribers keep reconnecting until the loss check is done.
	assertNoLoss(t, 60*time.Second, []*publisher{pub}, subs)
	cancel()
	assertNoCursorSkips(t, c, from, subs)

	// No broker fenced: none logged it, and no client was let go with 0x88
	// (server unavailable), which is how a fence drops a 5.0 client.
	fenced := fences(c)
	if slices.ContainsFunc(fenced, func(n int) bool { return n > 0 }) {
		t.Errorf("brokers fenced %v times with a NATS quorum intact", fenced)
	}

	reconnects, unavailable := 0, 0

	for _, s := range subs {
		_, conns, _ := s.snapshot()
		reconnects += len(conns) - 1

		unavailable += count88(conns)
	}

	unavailable += count88(st.conns)

	if unavailable > 0 {
		t.Errorf("%d client connections were dropped with 0x88 (server unavailable)", unavailable)
	}

	result(t, "heartbeat %s, lease %s; killed %s (log stream leader); new leader after %s; publishing resumed %t after %s (bound election + %s); published %d, acknowledged %d; publisher connections %d, subscriber reconnects %d; 0x88 drops %d; fences %v",
		c.heartbeat, c.lease, leaderName, elected.Round(time.Millisecond), ok, resumed.Round(time.Millisecond), electionSlack,
		len(st.sent), len(st.acked), len(st.conns), reconnects, unavailable, fenced)
}

// count88 counts the connections the server ended with DISCONNECT 0x88.
func count88(conns []conn) int {
	n := 0

	for _, cn := range conns {
		if cn.gotDisconnect && cn.reason == 0x88 {
			n++
		}
	}

	return n
}

// resumedAfter finds when, after t0, PUBACKs for messages sent after t0 first
// came at half the publisher's rate or more over a whole second: the start of
// the first such window, counted from t0. It reports false while there is
// none yet.
func resumedAfter(st pubStats, t0 time.Time, rate int) (time.Duration, bool) {
	var acks []time.Time

	for k, at := range st.acked {
		if st.sent[k].After(t0) {
			acks = append(acks, at)
		}
	}

	sort.Slice(acks, func(i, j int) bool { return acks[i].Before(acks[j]) })

	need := max(rate/2, 1)

	for i := range acks {
		end := acks[i].Add(time.Second)
		j := sort.Search(len(acks), func(k int) bool { return !acks[k].Before(end) })

		if j-i >= need {
			return acks[i].Sub(t0), true
		}
	}

	return 0, false
}

// TestPartitionBrokerFromNATS cuts node 1 off from NATS while its persistent
// subscribers stay connected to it. Node 1 drops them within lease/2 + 1 s;
// no other node takes their sessions until lease + 2 s after node 1's last
// heartbeat; the sessions never have two owners (from the records and from
// the clients, who never see 0x8E); and after the subscribers move to node 2
// and node 1 heals, nothing acknowledged is lost.
func TestPartitionBrokerFromNATS(t *testing.T) {
	c := startCell(t)
	n2, n3 := c.nodes[1], c.nodes[2]

	subs := subscribe(t, c, "part", "chaos/part/#", 4, c.nodes[0])
	from := baseline(t, c)

	pub := newPublisher(c, "p1", "chaos/part/p1", 200, n3)
	pub.start()

	time.Sleep(3 * time.Second)

	if pub.ackCount() == 0 {
		t.Fatal("nothing acknowledged before the cut")
	}

	cut := c.cutNode(0)

	// Node 1 fences and drops its clients. The subscribers stay away, so
	// only the survivors' reclaim can move the sessions.
	var lastDrop time.Time

	for _, s := range subs {
		if !s.waitDrop(lease/2 + 5*time.Second) {
			t.Fatalf("%s still connected to n1 %s after the cut", s.id, lease/2+5*time.Second)
		}

		_, conns, _ := s.snapshot()
		cn := conns[len(conns)-1]

		took := cn.end.Sub(cut)
		if took > lease/2+time.Second {
			t.Errorf("%s dropped %s after the cut, over lease/2 + 1 s (%s)", s.id, took.Round(time.Millisecond), lease/2+time.Second)
		}

		lastDrop = maxTime(lastDrop, cn.end)
	}

	// Wait for the survivors to take every session away from n1.
	moved := make(map[string]kvEvent, len(subs))

	eventually(t, lease+deadMargin+20*time.Second, "the survivors reclaiming n1's sessions", func() bool {
		for _, s := range subs {
			key := c.sessionKey(s.id)
			if _, ok := moved[key]; ok {
				continue
			}

			for _, ev := range c.obs.events(key) {
				if ev.stamp.After(cut) && (ev.rec == nil || ev.rec.Owner != "n1") {
					moved[key] = ev

					break
				}
			}
		}

		return len(moved) == len(subs)
	})

	var firstMove kvEvent
	for _, ev := range moved {
		if firstMove.stamp.IsZero() || ev.stamp.Before(firstMove.stamp) {
			firstMove = ev
		}
	}

	// n1's last heartbeat before the survivors acted is the last moment the
	// cell knew it alive.
	var lastBeat time.Time

	for _, b := range c.obs.beats("n1") {
		if b.stamp.Before(firstMove.stamp) {
			lastBeat = maxTime(lastBeat, b.stamp)
		}
	}

	if gap := firstMove.stamp.Sub(lastBeat); gap < lease+deadMargin {
		t.Errorf("a session left n1 %s after its last heartbeat, before lease + 2 s (%s)", gap.Round(time.Millisecond), lease+deadMargin)
	}

	if !lastDrop.Before(firstMove.stamp) {
		t.Errorf("n1 dropped its last client at %s, not before the first takeover at %s",
			lastDrop.Format("15:04:05.000"), firstMove.stamp.Format("15:04:05.000"))
	}

	// The subscribers come back on node 2 and find their sessions.
	for _, s := range subs {
		cn, err := s.connect(n2)
		if err != nil {
			t.Fatalf("%s: connect to n2: %v", s.id, err)
		}

		if !cn.sessionPresent {
			t.Errorf("%s: session not present on n2", s.id)
		}
	}

	waitOwned(t, c, subs, "n2")
	time.Sleep(2 * time.Second)

	healed := time.Now()
	c.healNode(0)

	unfenced := c.waitBeat(0, healed, 30*time.Second).Sub(healed)

	// n1 reclaims its own keys as it unfences. Give it a few heartbeats
	// past that, then look at every record update since the heal: none may
	// hand a session back to n1, which n2 now runs.
	beatsAfter(t, c, "n1", healed, 4)
	pub.stop()

	for _, s := range subs {
		for _, ev := range c.obs.events(c.sessionKey(s.id)) {
			if ev.stamp.After(healed) && ev.rec != nil && ev.rec.Owner == "n1" {
				t.Errorf("%s: record rev %d went back to n1 after the heal (epoch %d)", s.id, ev.rev, ev.rec.Epoch)
			}
		}

		if rec := c.obs.record(c.sessionKey(s.id)); rec == nil || rec.Owner != "n2" {
			t.Errorf("%s: record after the heal: %+v", s.id, rec)
		}
	}

	assertNoLoss(t, 30*time.Second, []*publisher{pub}, subs)

	takeovers := 0

	for _, s := range subs {
		takeovers += s.takeovers()

		var epoch uint64

		for _, ev := range c.obs.events(c.sessionKey(s.id)) {
			if ev.rec == nil {
				continue
			}

			if ev.rec.Epoch < epoch {
				t.Errorf("%s: epoch went back from %d to %d (rev %d, owner %s)", s.id, epoch, ev.rec.Epoch, ev.rev, ev.rec.Owner)
			}

			epoch = ev.rec.Epoch
		}
	}

	if takeovers != 0 {
		t.Errorf("clients saw %d DISCONNECTs with 0x8E (session taken over): two nodes served a session", takeovers)
	}

	// How n1 let each client go: 0x88 (server unavailable) is the fence.
	reasons := map[string]int{}

	for _, s := range subs {
		_, conns, _ := s.snapshot()
		for _, cn := range conns {
			if cn.node == "n1" && !cn.end.IsZero() {
				reasons[fmt.Sprintf("0x%02x/%t", cn.reason, cn.gotDisconnect)]++
			}
		}
	}

	assertNoCursorSkips(t, c, from, subs)

	st := pub.stats()

	result(t, "drops at most %s after the cut (bound %s); first takeover %s after n1's last heartbeat (floor %s), %s after the last drop; 0x8E seen %d; n1 drop reasons (reason/DISCONNECT sent) %v; n1 heartbeating %s after the heal; published %d, acknowledged %d; fences %v",
		lastDrop.Sub(cut).Round(time.Millisecond), lease/2+time.Second,
		firstMove.stamp.Sub(lastBeat).Round(time.Millisecond), lease+deadMargin,
		firstMove.stamp.Sub(lastDrop).Round(time.Millisecond), takeovers, reasons, unfenced.Round(time.Millisecond),
		len(st.sent), len(st.acked), fences(c))
}

// beatsAfter waits until the node has written n heartbeats stamped after t0.
func beatsAfter(t *testing.T, c *testCell, nodeID string, t0 time.Time, n int) {
	t.Helper()

	eventually(t, 30*time.Second, fmt.Sprintf("%d heartbeats from %s", n, nodeID), func() bool {
		got := 0

		for _, b := range c.obs.beats(nodeID) {
			if b.stamp.After(t0) {
				got++
			}
		}

		return got >= n
	})
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}

	return a
}

// TestNATSMinorityNoAcks isolates NATS server 1 together with broker 1, which
// is connected to it. QoS 1 publishes through broker 1 get no PUBACK while the
// partition lasts, broker 1 fences and accepts no CONNECT, the majority side
// keeps publishing after at most a leader election, and once healed nothing
// acknowledged on either side is lost.
func TestNATSMinorityNoAcks(t *testing.T) {
	c := startCell(t)
	n1, n2, n3 := c.nodes[0], c.nodes[1], c.nodes[2]

	// Each broker sits on its own server, so isolating server 1 leaves broker
	// 1 in the minority and the others in the majority.
	eventually(t, 15*time.Second, "each broker connected to its own server only", func() bool {
		for i, n := range c.nodes {
			for j, p := range n.proxies {
				if (i == j) != (p.Conns() > 0) {
					return false
				}
			}
		}

		return true
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subs := subscribe(t, c, "min", "chaos/min/#", 3, n3)
	for _, s := range subs {
		s.keepConnected(ctx, n3, n2)
	}

	from := baseline(t, c)

	minority := newPublisher(c, "minority", "chaos/min/minority", 200, n1)
	majority := newPublisher(c, "majority", "chaos/min/majority", 200, n2, n3)

	minority.start()
	majority.start()

	time.Sleep(3 * time.Second)

	if minority.ackCount() == 0 || majority.ackCount() == 0 {
		t.Fatal("nothing acknowledged before the partition")
	}

	cut := c.isolateServer(0)

	time.Sleep(15 * time.Second)

	healed := time.Now()
	c.healServer(0)

	c.waitBeat(0, healed, 60*time.Second)

	// Wait for the minority publisher to be acknowledged through n1 again.
	eventually(t, 30*time.Second, "a PUBACK through n1 after the heal", func() bool {
		st := minority.stats()
		for k := range st.acked {
			if st.sent[k].After(healed) {
				return true
			}
		}

		return false
	})

	time.Sleep(3 * time.Second)
	minority.stop()
	majority.stop()

	minSt, majSt := minority.stats(), majority.stats()

	attempts, acked := 0, 0

	for k, at := range minSt.sent {
		if at.After(cut) && at.Before(healed) {
			attempts++

			if _, ok := minSt.acked[k]; ok {
				acked++
			}
		}
	}

	if attempts == 0 {
		t.Error("the minority publisher sent nothing during the partition: nothing was checked")
	}

	if acked != 0 {
		t.Errorf("%d of %d QoS 1 publishes sent through the minority side during the partition got a PUBACK", acked, attempts)
	}

	// Broker 1 fenced and dropped the publisher, and refused it until the
	// heal. Exactly one connection was open at the cut, and it has to have
	// ended within lease/2 + 1 s of it.
	var (
		dropped time.Duration
		open    int
	)

	for _, cn := range minSt.conns {
		switch {
		case cn.at.Before(cut) && (cn.end.IsZero() || cn.end.After(cut)):
			open++

			if cn.end.IsZero() {
				t.Error("n1 never dropped the minority publisher's connection from before the cut")

				continue
			}

			dropped = cn.end.Sub(cut)
			if dropped > lease/2+time.Second {
				t.Errorf("n1 dropped the minority publisher %s after the cut, over lease/2 + 1 s", dropped.Round(time.Millisecond))
			}
		case cn.at.After(cut) && cn.at.Before(healed):
			t.Errorf("n1 accepted a CONNECT at %s, during the partition", cn.at.Format("15:04:05.000"))
		}
	}

	if open != 1 {
		t.Errorf("the minority publisher had %d connections open at the cut, want 1", open)
	}

	// The majority side keeps the cell going: past any fence and election the
	// partition sets off (so counting from a lease after the cut), its
	// PUBACKs flow at half the offered rate or more again before the heal.
	settled := cut.Add(lease)
	resumed, ok := resumedAfter(majSt, settled, majority.rate)

	if !ok || settled.Add(resumed+time.Second).After(healed) {
		t.Errorf("the majority side did not get back to publishing during the %s partition (resumed %t, %s after cut + lease)",
			healed.Sub(cut).Round(time.Second), ok, resumed.Round(time.Millisecond))
	}

	resumed += lease

	// The subscribers keep reconnecting until the loss check is done.
	assertNoLoss(t, 60*time.Second, []*publisher{minority, majority}, subs)
	cancel()
	assertNoCursorSkips(t, c, from, subs)

	result(t, "minority publishes during the partition %d, acknowledged %d; minority publisher dropped %s after the cut; majority resumed after %s; acknowledged minority %d, majority %d; fences %v",
		attempts, acked, dropped.Round(time.Millisecond), resumed.Round(time.Millisecond),
		len(minSt.acked), len(majSt.acked), fences(c))
}

// TestSlowConsumerNoSkips delays every NATS connection of node 2 by 200 ms
// each way while two persistent subscribers on it take a 500 msg/s QoS 1
// stream, one of them slow to handle each message. The subscribers drop their
// connections and resume on node 3, then drop again and resume on the slow
// node 2. No session record ever runs ahead of what its client received, and
// nothing acknowledged is lost.
func TestSlowConsumerNoSkips(t *testing.T) {
	c := startCell(t)
	n1, n2, n3 := c.nodes[0], c.nodes[1], c.nodes[2]

	fast := subscribe(t, c, "slow-a", "chaos/slow/#", 1, n2)
	slow := subscribe(t, c, "slow-b", "chaos/slow/#", 1, n2, func(s *subscriber) { s.slow = 3 * time.Millisecond })
	subs := append(fast, slow...)

	from := baseline(t, c)

	const delay = 200 * time.Millisecond

	c.delayNode(1, delay)

	pub := newPublisher(c, "p1", "chaos/slow/p1", 500, n1)
	pub.start()

	time.Sleep(8 * time.Second)

	var backlogs []string

	move := func(to *node, wantBacklog int) {
		acked := pub.stats().acked

		for _, s := range subs {
			behind := 0

			for k := range acked {
				if _, ok := s.has(k); !ok {
					behind++
				}
			}

			backlogs = append(backlogs, fmt.Sprintf("%s %d behind", s.id, behind))

			if s.slow > 0 && behind < wantBacklog {
				t.Fatalf("%s is only %d messages behind at the move, under %d: the scenario has no backlog to test", s.id, behind, wantBacklog)
			}
		}

		for _, s := range subs {
			s.abort()

			if !s.waitDrop(5 * time.Second) {
				t.Fatalf("%s: no drop after closing its connection", s.id)
			}

			cn, err := s.connect(to)
			if err != nil {
				t.Fatalf("%s: connect to %s: %v", s.id, to.id, err)
			}

			if !cn.sessionPresent {
				t.Errorf("%s: session not present on %s", s.id, to.id)
			}
		}
	}

	// 8 s at 500 msg/s against about 330 msg/s leaves well over a thousand
	// behind; a few hundred is the floor for a backlog worth the name.
	move(n3, 300)
	time.Sleep(5 * time.Second)
	move(n2, 0)
	time.Sleep(5 * time.Second)
	pub.stop()

	assertNoLoss(t, 120*time.Second, []*publisher{pub}, subs)
	assertNoCursorSkips(t, c, from, subs)

	st := pub.stats()

	result(t, "node 2 delayed %s each way; at the moves: %s; published %d, acknowledged %d; fences %v",
		delay, strings.Join(backlogs, ", "), len(st.sent), len(st.acked), fences(c))
}
