package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster/clustertest"
)

const testTenant = "ttnt_test"

func tctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)

	return ctx
}

// startCell starts a three-server JetStream cluster, provisions a cell on it,
// waits until every server can route a write to the cell's streams, and
// returns one Sessions per node, node i on server i%3. Node IDs are n1, n2
// and so on.
func startCell(t *testing.T, nodes int) []*Sessions {
	t.Helper()

	nc := clustertest.StartCluster(t, 3)
	urls := nc.URLs()
	out := make([]*Sessions, 0, nodes)

	for i := range nodes {
		cfg := DefaultConfig()
		cfg.Enabled = true
		cfg.NodeID = fmt.Sprintf("n%d", i+1)
		cfg.Cell = "c1"
		cfg.Mode = ModeExternal
		cfg.URLs = []string{urls[i%len(urls)]}
		cfg.Replicas = 3

		c, err := Connect(tctx(t, 20*time.Second), cfg, nil)
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(c.Close)

		if i == 0 {
			if err := Provision(tctx(t, 30*time.Second), c, cfg, time.Hour); err != nil {
				t.Fatal(err)
			}

			nc.WaitInterest(t, cellSubjects(cfg)...)
		}

		out = append(out, NewSessions(c, cfg))
	}

	return out
}

func apiCode(err error) jetstream.ErrorCode {
	var api *jetstream.APIError
	if errors.As(err, &api) {
		return api.ErrorCode
	}

	return 0
}

func isCASCode(c jetstream.ErrorCode) bool {
	return c == jetstream.JSErrCodeStreamWrongLastSequence || c == codeWrongLastSeqConstant
}

func TestAcquireOneWinner(t *testing.T) {
	cell := startCell(t, 2)
	a, b := cell[0], cell[1]

	// Both Acquires finish their read before either writes, so every round is
	// two writers CASing from the same revision.
	var round atomic.Pointer[sync.WaitGroup]

	hook := func() {
		wg := round.Load()
		wg.Done()
		wg.Wait()
	}
	a.afterRead, b.afterRead = hook, hook

	type result struct {
		rec *SessionRecord
		rev uint64
		err error
	}

	codes := map[jetstream.ErrorCode]int{}
	wins := map[string]int{}

	const rounds = 200

	for i := range rounds {
		wg := new(sync.WaitGroup)
		wg.Add(2)
		round.Store(wg)

		var ra, rb result

		var done sync.WaitGroup

		done.Go(func() {
			ra.rec, ra.rev, _, ra.err = a.Acquire(tctx(t, 20*time.Second), testTenant, "dev-1", "n1")
		})
		done.Go(func() {
			rb.rec, rb.rev, _, rb.err = b.Acquire(tctx(t, 20*time.Second), testTenant, "dev-1", "n2")
		})
		done.Wait()

		var win, lose result

		var winner string

		switch {
		case ra.err == nil && rb.err == nil:
			t.Fatalf("round %d: both acquires won", i)
		case ra.err != nil && rb.err != nil:
			t.Fatalf("round %d: both failed: %v / %v", i, ra.err, rb.err)
		case ra.err == nil:
			win, lose, winner = ra, rb, "n1"
		default:
			win, lose, winner = rb, ra, "n2"
		}

		wins[winner]++

		if !errors.Is(lose.err, ErrLostCAS) {
			t.Fatalf("round %d: loser's error %v does not match ErrLostCAS", i, lose.err)
		}

		code := apiCode(lose.err)
		if !isCASCode(code) {
			t.Fatalf("round %d: loser's JetStream code is %d, want 10071 or 10164", i, code)
		}

		codes[code]++

		if win.rec.Owner != winner || win.rec.Epoch != uint64(i+1) {
			t.Fatalf("round %d: winner's record is owner %q epoch %d", i, win.rec.Owner, win.rec.Epoch)
		}

		got, rev, err := a.Get(tctx(t, 10*time.Second), testTenant, "dev-1")
		if err != nil {
			t.Fatal(err)
		}

		if got.Owner != winner || got.Epoch != uint64(i+1) || rev != win.rev {
			t.Fatalf("round %d: stored owner %q epoch %d rev %d, winner %q epoch %d rev %d",
				i, got.Owner, got.Epoch, rev, winner, i+1, win.rev)
		}
	}

	t.Logf("%d rounds, wins %v, loser JetStream codes %v", rounds, wins, codes)
}

func TestUpdateStaleRevision(t *testing.T) {
	cell := startCell(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 30*time.Second)

	rec, rev1, existed, err := a.Acquire(ctx, testTenant, "dev-2", "n1")
	if err != nil {
		t.Fatal(err)
	}

	if existed || rec.Epoch != 1 || rec.Owner != "n1" {
		t.Fatalf("first Acquire: existed %v, owner %q, epoch %d", existed, rec.Owner, rec.Epoch)
	}

	rec.Cursor = 7

	rev2, err := a.Update(ctx, testTenant, "dev-2", rev1, rec)
	if err != nil {
		t.Fatal(err)
	}

	if rev2 <= rev1 {
		t.Fatalf("revision went from %d to %d", rev1, rev2)
	}

	// The same revision again, from this node and from another one.
	for name, s := range map[string]*Sessions{"same node": a, "other node": b} {
		_, uerr := s.Update(ctx, testTenant, "dev-2", rev1, rec)
		if !errors.Is(uerr, ErrLostCAS) {
			t.Fatalf("%s: stale Update = %v, want ErrLostCAS", name, uerr)
		}

		if code := apiCode(uerr); !isCASCode(code) {
			t.Fatalf("%s: JetStream code %d", name, code)
		}
	}

	if err = b.Delete(ctx, testTenant, "dev-2", rev1); !errors.Is(err, ErrLostCAS) {
		t.Fatalf("stale Delete = %v, want ErrLostCAS", err)
	}

	if _, err = a.Update(ctx, testTenant, "dev-2", 0, rec); !errors.Is(err, tether.ErrInvalidInput) {
		t.Fatalf("Update at revision 0 = %v, want ErrInvalidInput", err)
	}

	got, rev, err := b.Get(ctx, testTenant, "dev-2")
	if err != nil || rev != rev2 || got.Cursor != 7 {
		t.Fatalf("Get = %+v rev %d err %v, want cursor 7 at rev %d", got, rev, err, rev2)
	}

	// A takeover keeps what the session held and raises the epoch.
	taken, rev3, existed, err := b.Acquire(ctx, testTenant, "dev-2", "n2")
	if err != nil {
		t.Fatal(err)
	}

	if !existed || taken.Owner != "n2" || taken.Epoch != 2 || taken.Cursor != 7 || rev3 <= rev2 {
		t.Fatalf("takeover: existed %v, owner %q, epoch %d, cursor %d, rev %d", existed, taken.Owner, taken.Epoch, taken.Cursor, rev3)
	}
}

func TestOneOutstandingCASPerKey(t *testing.T) {
	cell := startCell(t, 1)
	s := cell[0]
	ctx := tctx(t, 60*time.Second)

	rec, rev, _, err := s.Acquire(ctx, testTenant, "dev-3", "n1")
	if err != nil {
		t.Fatal(err)
	}

	// Two writers on one key at the same revision. With the keyed lock the
	// second one starts after the first is acknowledged, so the loser meets an
	// applied write (10071). Without it the loser would meet one still in
	// flight (10164) almost every time.
	var applied, inflight int

	const rounds = 100

	for i := range rounds {
		var (
			wg   sync.WaitGroup
			revs [2]uint64
			errs [2]error
		)

		start := make(chan struct{})

		for w := range 2 {
			wg.Go(func() {
				<-start

				rec := *rec
				rec.Cursor = uint64(i*2 + w)
				revs[w], errs[w] = s.Update(ctx, testTenant, "dev-3", rev, &rec)
			})
		}

		close(start)
		wg.Wait()

		var winner, loser int

		switch {
		case errs[0] == nil && errs[1] == nil:
			t.Fatalf("round %d: both updates won", i)
		case errs[0] != nil && errs[1] != nil:
			t.Fatalf("round %d: both failed: %v / %v", i, errs[0], errs[1])
		case errs[0] == nil:
			winner, loser = 0, 1
		default:
			winner, loser = 1, 0
		}

		if !errors.Is(errs[loser], ErrLostCAS) {
			t.Fatalf("round %d: loser got %v", i, errs[loser])
		}

		switch apiCode(errs[loser]) {
		case jetstream.JSErrCodeStreamWrongLastSequence:
			applied++
		case codeWrongLastSeqConstant:
			inflight++
		}

		rev = revs[winner]
	}

	if inflight != 0 {
		t.Fatalf("%d of %d losers met a write in flight (10164): the node had two writes outstanding on one key", inflight, rounds)
	}

	t.Logf("%d rounds, every loser got 10071 (%d)", rounds, applied)
}

func TestExpirePurgesAndWatchSees(t *testing.T) {
	cell := startCell(t, 2)
	a := cell[0]
	ctx := tctx(t, 60*time.Second)

	type lost struct {
		tenant, clientID string
		rec              *SessionRecord
		deleted          bool
		at               time.Time
	}

	events := make(chan lost, 16)

	stop, err := a.WatchOwned(ctx, "n1", func(tenant, clientID string, rec *SessionRecord, deleted bool) {
		events <- lost{tenant, clientID, rec, deleted, time.Now()}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	rec, rev, _, err := a.Acquire(ctx, testTenant, "dev-4", "n1")
	if err != nil {
		t.Fatal(err)
	}

	rec.DisconnectedAt = time.Now().UTC()

	if _, err := a.Expire(ctx, testTenant, "dev-4", rev, rec, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	expired := time.Now()

	select {
	case ev := <-events:
		if !ev.deleted || ev.rec != nil || ev.tenant != testTenant || ev.clientID != "dev-4" {
			t.Fatalf("event = %+v, want a deletion of %s/dev-4", ev, testTenant)
		}

		if d := ev.at.Sub(expired); d > 3500*time.Millisecond || d < time.Second {
			t.Fatalf("deletion reported %v after Expire, want about 2 s", d)
		} else {
			t.Logf("deletion reported %v after Expire acknowledged", d.Round(10*time.Millisecond))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no deletion reported for an expired session")
	}

	if _, _, err := a.Get(ctx, testTenant, "dev-4"); !errors.Is(err, tether.ErrNotFound) {
		t.Fatalf("Get after expiry = %v, want ErrNotFound", err)
	}

	// The watcher forgot the session: nothing more arrives for it.
	select {
	case ev := <-events:
		t.Fatalf("second event for an expired session: %+v", ev)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestRecreateAfterMarker(t *testing.T) {
	cell := startCell(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	gone := make(chan struct{}, 4)

	stop, err := a.WatchOwned(ctx, "n1", func(_, _ string, _ *SessionRecord, deleted bool) {
		if deleted {
			gone <- struct{}{}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	rec, rev, _, err := a.Acquire(ctx, testTenant, "dev-5", "n1")
	if err != nil {
		t.Fatal(err)
	}

	rec.Subscriptions = []SubRecord{{Filter: "a/b", QoS: 1}}

	if _, err = a.Expire(ctx, testTenant, "dev-5", rev, rec, time.Second); err != nil {
		t.Fatal(err)
	}

	select {
	case <-gone:
	case <-time.After(10 * time.Second):
		t.Fatal("session did not expire")
	}

	// The key reads as absent but its last message is the limit marker, so a
	// create at revision 0 would be refused.
	h, err := b.handles(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cur, err := b.read(ctx, h, mustKey(t, testTenant, "dev-5"))
	if err != nil {
		t.Fatal(err)
	}

	if cur.state != stateMarker || cur.rev == 0 {
		t.Fatalf("after expiry the key reads as state %d at revision %d, want a marker", cur.state, cur.rev)
	}

	if _, err = b.publish(ctx, h, mustKey(t, testTenant, "dev-5"), []byte(`{}`), 0, 0, nil); !isLostCAS(err) {
		t.Fatalf("create at revision 0 over a marker = %v, want a lost CAS", err)
	}

	got, newRev, existed, err := b.Acquire(ctx, testTenant, "dev-5", "n2")
	if err != nil {
		t.Fatalf("Acquire over a marker: %v", err)
	}

	if existed || got.Owner != "n2" || got.Epoch != 1 || len(got.Subscriptions) != 0 || newRev <= cur.rev {
		t.Fatalf("Acquire over a marker: existed %v, %+v, rev %d (marker %d)", existed, got, newRev, cur.rev)
	}

	// And the new session behaves like any other.
	if _, err := b.Update(ctx, testTenant, "dev-5", newRev, got); err != nil {
		t.Fatal(err)
	}
}

func mustKey(t *testing.T, tenant, clientID string) string {
	t.Helper()

	k, err := sessionKey(tenant, clientID)
	if err != nil {
		t.Fatal(err)
	}

	return k
}

func TestWatchOwned(t *testing.T) {
	cell := startCell(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 60*time.Second)

	type lost struct {
		clientID string
		rec      *SessionRecord
		deleted  bool
	}

	events := make(chan lost, 16)

	watch := func(s *Sessions, node string) func() {
		stop, err := s.WatchOwned(ctx, node, func(_, clientID string, rec *SessionRecord, deleted bool) {
			events <- lost{clientID, rec, deleted}
		})
		if err != nil {
			t.Fatal(err)
		}

		return stop
	}

	expectQuiet := func(why string) {
		t.Helper()

		select {
		case ev := <-events:
			t.Fatalf("%s: unexpected event %+v", why, ev)
		case <-time.After(500 * time.Millisecond):
		}
	}

	expectEvent := func(why string) lost {
		t.Helper()

		select {
		case ev := <-events:
			return ev
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: no event", why)

			return lost{}
		}
	}

	// A session that already exists when the watch starts is learned from the
	// watch's first pass.
	_, preRev, _, err := b.Acquire(ctx, testTenant, "pre", "n1")
	if err != nil {
		t.Fatal(err)
	}

	stopA := watch(a, "n1")
	defer stopA()

	// Own writes of every kind stay quiet.
	rec, rev, _, err := a.Acquire(ctx, testTenant, "own", "n1")
	if err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		rec.Cursor = uint64(i + 1)

		if rev, err = a.Update(ctx, testTenant, "own", rev, rec); err != nil {
			t.Fatal(err)
		}
	}

	if rev, err = a.Expire(ctx, testTenant, "own", rev, rec, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	if _, err = a.Update(ctx, testTenant, "own", rev, rec); err != nil {
		t.Fatal(err)
	}

	// Re-acquiring your own session is not losing it.
	if _, _, _, err = a.Acquire(ctx, testTenant, "own", "n1"); err != nil {
		t.Fatal(err)
	}

	expectQuiet("own writes")

	// A node taking over a session that n1 owned reports the new record. Both
	// sessions go, "own" because it was acquired here and "pre" because the
	// watch read it.
	took, _, _, err := b.Acquire(ctx, testTenant, "own", "n2")
	if err != nil {
		t.Fatal(err)
	}

	ev := expectEvent("takeover of own")
	if ev.clientID != "own" || ev.deleted || ev.rec == nil || ev.rec.Owner != "n2" || ev.rec.Epoch != took.Epoch {
		t.Fatalf("takeover event = %+v", ev)
	}

	if err = b.Delete(ctx, testTenant, "pre", preRev); err != nil {
		t.Fatal(err)
	}

	ev = expectEvent("delete of pre")
	if ev.clientID != "pre" || !ev.deleted || ev.rec != nil {
		t.Fatalf("delete event = %+v", ev)
	}

	// Each is reported once.
	expectQuiet("after losing both")

	// n1 deleting its own session is not an event.
	_, mineRev, _, err := a.Acquire(ctx, testTenant, "mine", "n1")
	if err != nil {
		t.Fatal(err)
	}

	if err = a.Delete(ctx, testTenant, "mine", mineRev); err != nil {
		t.Fatal(err)
	}

	expectQuiet("own delete")

	// n1 owns a session again after the marker, and losing it is reported once
	// more.
	_, _, existed, err := a.Acquire(ctx, testTenant, "mine", "n1")
	if err != nil || existed {
		t.Fatalf("re-create after own delete: existed %v, err %v", existed, err)
	}

	expectQuiet("re-create")

	if _, _, _, err := b.Acquire(ctx, testTenant, "mine", "n2"); err != nil {
		t.Fatal(err)
	}

	if ev = expectEvent("takeover after re-create"); ev.clientID != "mine" || ev.deleted {
		t.Fatalf("event = %+v", ev)
	}
}

func TestWatchOwnedStop(t *testing.T) {
	cell := startCell(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 30*time.Second)

	var calls atomic.Int32

	stop, err := a.WatchOwned(ctx, "n1", func(string, string, *SessionRecord, bool) { calls.Add(1) })
	if err != nil {
		t.Fatal(err)
	}

	if _, _, _, err := a.Acquire(ctx, testTenant, "dev-6", "n1"); err != nil {
		t.Fatal(err)
	}

	stop()
	stop() // idempotent

	if _, _, _, err := b.Acquire(ctx, testTenant, "dev-6", "n2"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	if n := calls.Load(); n != 0 {
		t.Fatalf("callback ran %d times after stop", n)
	}

	a.wmu.Lock()
	n := len(a.watchers)
	a.wmu.Unlock()

	if n != 0 {
		t.Fatalf("%d watchers still registered after stop", n)
	}
}

func TestSessionRecordRoundTrip(t *testing.T) {
	cell := startCell(t, 1)
	s := cell[0]
	ctx := tctx(t, 30*time.Second)

	rec, rev, _, err := s.Acquire(ctx, testTenant, "dev-7", "n1")
	if err != nil {
		t.Fatal(err)
	}

	zero := byte(0)
	want := &SessionRecord{
		Owner: "n1", Epoch: rec.Epoch, Clean: true, ProtocolVersion: 5,
		Username: "akey_1", DeviceID: "tdev_1", ExpirySeconds: 3600,
		Subscriptions: []SubRecord{
			{Filter: "a/+/c", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 1, Identifier: 7},
			{Filter: "$share/g/x", QoS: 0},
		},
		Cursor: 1 << 40,
		Will: &WillRecord{
			Topic: "last/will", Payload: []byte{0, 1, 2, 255}, QoS: 1, Retain: true, DelaySeconds: 30,
			Props: Props{
				User:            []UserProperty{{"k", "1"}, {"k", "2"}, {"j", ""}},
				ContentType:     "text/plain",
				ResponseTopic:   "reply/me",
				CorrelationData: []byte("corr"),
				MessageExpiry:   60,
				PayloadFormat:   &zero,
			},
		},
		QoS2In:         map[uint16]string{1: "d.t.a", 65535: "d.t.b"},
		QoS2Out:        map[uint16]OutRecord{2: {Seq: 99, State: 1}},
		DisconnectedAt: time.Date(2026, 10, 23, 12, 0, 0, 5, time.UTC),
	}

	if _, err = s.Update(ctx, testTenant, "dev-7", rev, want); err != nil {
		t.Fatal(err)
	}

	got, _, err := s.Get(ctx, testTenant, "dev-7")
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		gj, _ := json.Marshal(got)
		wj, _ := json.Marshal(want)
		t.Fatalf("round trip differs:\n got %s\nwant %s", gj, wj)
	}

	if got.Will.Props.PayloadFormat == nil {
		t.Fatal("a payload format of 0 came back absent")
	}
}

func TestPropsZero(t *testing.T) {
	zero := byte(0)

	for name, tc := range map[string]struct {
		p    Props
		zero bool
	}{
		"empty":          {Props{}, true},
		"user":           {Props{User: []UserProperty{{"a", "b"}}}, false},
		"content type":   {Props{ContentType: "x"}, false},
		"expiry":         {Props{MessageExpiry: 1}, false},
		"format zero":    {Props{PayloadFormat: &zero}, false},
		"correlation":    {Props{CorrelationData: []byte{0}}, false},
		"response topic": {Props{ResponseTopic: "r"}, false},
	} {
		if got := tc.p.IsZero(); got != tc.zero {
			t.Errorf("%s: IsZero = %v", name, got)
		}
	}

	b, err := json.Marshal(WillRecord{Topic: "t"})
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(b), "props") {
		t.Fatalf("a will with no properties encodes them: %s", b)
	}
}

func TestListAndGet(t *testing.T) {
	cell := startCell(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 30*time.Second)

	for _, tc := range []struct{ tenant, client string }{
		{"ttnt_one", "a"}, {"ttnt_one", "b"}, {"ttnt_one", "weird client/id"}, {"ttnt_two", "a"}, {"ttnt_onex", "a"},
	} {
		if _, _, _, err := a.Acquire(ctx, tc.tenant, tc.client, "n1"); err != nil {
			t.Fatal(err)
		}
	}

	// A deleted session is not listed.
	_, rev, _, err := a.Acquire(ctx, "ttnt_one", "gone", "n1")
	if err != nil {
		t.Fatal(err)
	}

	if err = a.Delete(ctx, "ttnt_one", "gone", rev); err != nil {
		t.Fatal(err)
	}

	list, err := b.List(ctx, "ttnt_one")
	if err != nil {
		t.Fatal(err)
	}

	if len(list) != 3 {
		t.Fatalf("List(ttnt_one) = %d records, want 3", len(list))
	}

	for _, r := range list {
		if r.Owner != "n1" {
			t.Fatalf("record %+v", r)
		}
	}

	if list, err = b.List(ctx, "ttnt_empty"); err != nil || len(list) != 0 {
		t.Fatalf("List of an empty tenant = %v, %v", list, err)
	}

	if _, _, err := b.Get(ctx, "ttnt_one", "gone"); !errors.Is(err, tether.ErrNotFound) {
		t.Fatalf("Get of a deleted session = %v, want ErrNotFound", err)
	}

	if _, _, err := b.Get(ctx, "ttnt_one", "never"); !errors.Is(err, tether.ErrNotFound) {
		t.Fatalf("Get of an unknown session = %v, want ErrNotFound", err)
	}

	if got, _, err := b.Get(ctx, "ttnt_one", "weird client/id"); err != nil || got.Owner != "n1" {
		t.Fatalf("Get of an encoded client id = %+v, %v", got, err)
	}
}

// OwnedBy reads every tenant and keeps only the records of one owner, with
// the tenant, client ID and revision a CAS needs.
func TestOwnedBy(t *testing.T) {
	cell := startCell(t, 2)
	a, b := cell[0], cell[1]
	ctx := tctx(t, 30*time.Second)

	revs := map[string]uint64{}

	for _, tc := range []struct{ tenant, client, node string }{
		{"ttnt_one", "a", "n1"}, {"ttnt_one", "weird client/id", "n1"}, {"ttnt_two", "a", "n1"},
		{"ttnt_one", "b", "n2"}, {"ttnt_two", "gone", "n1"},
	} {
		_, rev, _, err := a.Acquire(ctx, tc.tenant, tc.client, tc.node)
		if err != nil {
			t.Fatal(err)
		}

		revs[tc.tenant+"/"+tc.client] = rev
	}

	if err := a.Delete(ctx, "ttnt_two", "gone", revs["ttnt_two/gone"]); err != nil {
		t.Fatal(err)
	}

	owned, err := b.OwnedBy(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]uint64{}
	for _, e := range owned {
		if e.Record.Owner != "n1" {
			t.Fatalf("entry %+v", e)
		}

		got[e.Tenant+"/"+e.ClientID] = e.Revision
	}

	want := map[string]uint64{
		"ttnt_one/a": revs["ttnt_one/a"], "ttnt_one/weird client/id": revs["ttnt_one/weird client/id"], "ttnt_two/a": revs["ttnt_two/a"],
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("OwnedBy(n1) = %v, want %v", got, want)
	}

	if owned, err = b.OwnedBy(ctx, "n9"); err != nil || len(owned) != 0 {
		t.Fatalf("OwnedBy of a node with nothing = %v, %v", owned, err)
	}

	// Several owners in one pass.
	if owned, err = b.OwnedBy(ctx, "n1", "n2", "n9"); err != nil || len(owned) != 4 {
		t.Fatalf("OwnedBy(n1, n2, n9) = %d entries, %v; want 4", len(owned), err)
	}

	if _, err = b.OwnedBy(ctx); !errors.Is(err, tether.ErrInvalidInput) {
		t.Fatalf("OwnedBy() = %v, want ErrInvalidInput", err)
	}

	if _, err = b.OwnedBy(ctx, ""); !errors.Is(err, tether.ErrInvalidInput) {
		t.Fatalf("OwnedBy(\"\") = %v, want ErrInvalidInput", err)
	}

	// Entries carries the tenant too.
	entries, err := b.Entries(ctx, "ttnt_one")
	if err != nil || len(entries) != 3 || entries[0].Tenant != "ttnt_one" {
		t.Fatalf("Entries = %+v, %v", entries, err)
	}
}

func TestInvalidInput(t *testing.T) {
	cell := startCell(t, 1)
	s := cell[0]
	ctx := tctx(t, 30*time.Second)
	long := strings.Repeat("x", 257)

	for name, err := range map[string]error{
		"acquire long id":     second(s.Acquire(ctx, testTenant, long, "n1")),
		"acquire empty tent":  second(s.Acquire(ctx, "", "a", "n1")),
		"acquire empty node":  second(s.Acquire(ctx, testTenant, "a", "")),
		"update long id":      secondU(s.Update(ctx, testTenant, long, 1, &SessionRecord{})),
		"update nil record":   secondU(s.Update(ctx, testTenant, "a", 1, nil)),
		"expire zero ttl":     secondU(s.Expire(ctx, testTenant, "a", 1, &SessionRecord{}, 0)),
		"delete empty tenant": s.Delete(ctx, "", "a", 1),
		"delete revision 0":   s.Delete(ctx, testTenant, "a", 0),
	} {
		if !errors.Is(err, tether.ErrInvalidInput) {
			t.Errorf("%s: %v, want ErrInvalidInput", name, err)
		}
	}

	if _, _, err := s.Get(ctx, testTenant, long); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("Get long id: %v", err)
	}

	if _, err := s.List(ctx, ""); !errors.Is(err, tether.ErrInvalidInput) {
		t.Errorf("List empty tenant: %v", err)
	}

	// Nothing was written for any of them, least of all a key for "".
	if list, err := s.List(ctx, testTenant); err != nil || len(list) != 0 {
		t.Errorf("List after refusals = %v, %v", list, err)
	}
}

func second(_ *SessionRecord, _ uint64, _ bool, err error) error { return err }

func secondU(_ uint64, err error) error { return err }

func TestKeyedMutexReleasesEntries(t *testing.T) {
	var km keyedMutex

	var wg sync.WaitGroup

	for i := range 200 {
		wg.Go(func() {
			key := fmt.Sprintf("k%d", i%7)

			for range 20 {
				unlock, err := km.lock(t.Context(), key)
				if err != nil {
					t.Error(err)

					return
				}

				unlock()
			}
		})
	}

	wg.Wait()

	if n := km.size(); n != 0 {
		t.Fatalf("%d entries left after every lock was released", n)
	}

	// A waiter whose context ends gives its place up.
	unlock, err := km.lock(t.Context(), "held")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	if _, err := km.lock(ctx, "held"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock of a held key = %v, want deadline exceeded", err)
	}

	unlock()

	if n := km.size(); n != 0 {
		t.Fatalf("%d entries left", n)
	}
}

func TestIsLostCAS(t *testing.T) {
	api := func(code jetstream.ErrorCode) error {
		return &jetstream.APIError{Code: 400, ErrorCode: code, Description: "wrong last sequence"}
	}

	for name, tc := range map[string]struct {
		err  error
		lost bool
	}{
		"nil":                          {nil, false},
		"10071":                        {api(jetstream.JSErrCodeStreamWrongLastSequence), true},
		"10164":                        {api(codeWrongLastSeqConstant), true},
		"10071 wrapped":                {fmt.Errorf("publish: %w", api(10071)), true},
		"10164 wrapped twice":          {fmt.Errorf("a: %w", fmt.Errorf("b: %w", api(10164))), true},
		"key exists":                   {jetstream.ErrKeyExists, true},
		"key exists wrapped":           {fmt.Errorf("create: %w", jetstream.ErrKeyExists), true},
		"revision mismatch":            {jetstream.ErrKeyRevisionMismatch, true},
		"revision mismatch with 10164": {fmt.Errorf("%w: %w", api(10164), jetstream.ErrKeyRevisionMismatch), true},
		"message not found":            {api(jetstream.JSErrCodeMessageNotFound), false},
		"per-message ttl disabled":     {api(10166), false},
		"too many requests":            {api(10167), false},
		"plain":                        {errors.New("wrong last sequence"), false},
		"already lost":                 {ErrLostCAS, false},
	} {
		if got := isLostCAS(tc.err); got != tc.lost {
			t.Errorf("%s: isLostCAS = %v, want %v", name, got, tc.lost)
		}
	}
}

func TestTTLHeader(t *testing.T) {
	for in, want := range map[time.Duration]string{
		time.Nanosecond:         "1s",
		time.Millisecond:        "1s",
		time.Second:             "1s",
		1001 * time.Millisecond: "2s",
		1500 * time.Millisecond: "2s",
		2 * time.Second:         "2s",
		168 * time.Hour:         "604800s",
	} {
		if got := ttlHeader(in); got != want {
			t.Errorf("ttlHeader(%v) = %q, want %q", in, got, want)
		}
	}
}
