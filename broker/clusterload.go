package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/xraph/tether/cluster"
)

const (
	// defaultRateInterval is how often a broker outside a cell samples its
	// usage for rates. In a cell the sample is the load report, every
	// Config.LoadInterval.
	defaultRateInterval = cluster.DefaultLoadInterval

	// loadStaleAfter is how many sample intervals a peer's report counts
	// after it arrived, as many as the report lives in the load bucket. A
	// peer that stops reporting leaves this node's view by then, even while
	// the bucket has not removed its key yet.
	loadStaleAfter = 3

	// defaultShareInterval is shareInterval's value.
	defaultShareInterval = 10 * time.Second

	// loadWarnEvery is how often a failing load report is logged at Warn.
	// The failures in between go to Debug.
	loadWarnEvery = time.Minute

	// loadWatchRetry and loadWatchRetryMax pace a load watch that could not
	// start: 200 ms, doubling up to 5 s.
	loadWatchRetry    = 200 * time.Millisecond
	loadWatchRetryMax = 5 * time.Second

	// withdrawWait bounds the withdrawal of the load report at Stop.
	withdrawWait = 2 * time.Second
)

// shareInterval is how often a node in a cell with a Cluster.Load recomputes
// each tenant's share of its message rate (runShares). Tests may lower it
// before Start.
var shareInterval = defaultShareInterval

// Rates are per-second rates of a tenant's or a node's traffic, from two
// samples of its cumulative counters.
type Rates struct {
	MessagesInPerSec, MessagesOutPerSec float64
	BytesInPerSec, BytesOutPerSec       float64
}

// add adds o to r.
func (r *Rates) add(o Rates) {
	r.MessagesInPerSec += o.MessagesInPerSec
	r.MessagesOutPerSec += o.MessagesOutPerSec
	r.BytesInPerSec += o.BytesInPerSec
	r.BytesOutPerSec += o.BytesOutPerSec
}

// NodeUsage is one node of the cell as this node sees it.
type NodeUsage struct {
	// NodeID is the node's cluster.Config.NodeID, "" for a broker outside a
	// cell.
	NodeID string

	// Self is true for this node's own row.
	Self bool

	// StartedAt is when the node started. At is when its numbers were
	// read: for another node the time of its latest report, on its own
	// clock; for this node the time of the call.
	StartedAt, At time.Time

	// Connections is the node's live network connections, every tenant's.
	Connections int

	// Totals sums the node's TenantUsage over its tenants. Totals.Tenant is
	// empty and Totals.Connections equals Connections.
	Totals TenantUsage

	// Rates sums the node's tenants' rates.
	Rates Rates
}

// loadSnap is one node's counters at one moment: one of its load reports, or
// one of this node's samples.
type loadSnap struct {
	startedAt, at time.Time
	tenants       map[string]cluster.TenantLoad
}

// peerLoad is what this node holds of another node: its latest report, the
// one before it (for rates) and when the latest arrived, on this node's
// clock.
type peerLoad struct {
	last, prev *loadSnap
	seen       time.Time
}

// loadView is the broker's view of the cell's load: its own last two samples
// and every other node's last two reports. mu is a leaf: nothing else is
// taken while it is held, and no NATS or mochi call runs under it.
type loadView struct {
	mu                 sync.Mutex
	selfLast, selfPrev *loadSnap
	peers              map[string]*peerLoad

	// stale is how long a peer's report counts after it arrived; zero
	// before Start.
	stale time.Duration
}

// live reports whether p's latest report is not stale at now. The caller
// holds mu. Callers walk v.peers and skip a peer that is not live; nothing is
// allocated for it, since RemoteConnections runs on every CONNECT.
func (v *loadView) live(p *peerLoad, now time.Time) bool {
	return v.stale <= 0 || now.Sub(p.seen) <= v.stale
}

// selfNodeID is this node's ID in its cell, "" outside one.
func (b *Broker) selfNodeID() string {
	if b.cluster == nil {
		return ""
	}

	return b.cluster.Config.NodeID
}

// reportsLoad reports whether the broker publishes load reports and watches
// its peers': in a cell, with Cluster.Load set.
func (b *Broker) reportsLoad() bool {
	return b.cluster != nil && b.cluster.Load != nil
}

// startLoad starts the sampling loop and, in a cell with a Load, the watch of
// the other nodes' reports. Called once from Start with b.mu held; it makes
// no NATS call itself.
func (b *Broker) startLoad(started time.Time) {
	interval := b.loadInterval

	if interval <= 0 && b.cluster != nil {
		interval = b.cluster.Config.LoadInterval
	}

	if interval <= 0 {
		interval = defaultRateInterval
	}

	b.load.mu.Lock()
	b.load.stale = loadStaleAfter * interval
	b.load.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118 -- Stop calls it, through b.loadCancel.
	b.loadCancel = cancel

	b.loadWG.Go(func() { b.runLoad(ctx, started, interval) })

	if b.reportsLoad() {
		b.loadWG.Go(func() { b.watchLoad(ctx) })

		every := shareInterval
		if every <= 0 {
			every = defaultShareInterval
		}

		b.loadWG.Go(func() { b.runShares(ctx, every) })
	}
}

// stopLoad ends the sampling loop and the watch and waits for both. A report
// or a watch start in progress ends with the context, so a NATS outage does
// not hold it. Then the view is emptied: a stopped broker has no rates and
// no peers.
func (b *Broker) stopLoad(cancel context.CancelFunc) {
	if cancel == nil {
		return
	}

	cancel()
	b.loadWG.Wait()

	b.load.mu.Lock()
	b.load.selfLast, b.load.selfPrev = nil, nil
	b.load.peers = nil
	b.load.mu.Unlock()
}

// withdrawLoad removes this node's load report from the cell, so its peers
// stop counting its connections at once instead of for up to three intervals.
// Stop calls it once mochi has shut down, after the last report. It waits
// withdrawWait at most, and a failure is only logged at Debug: the report's
// lifetime takes it down anyway.
func (b *Broker) withdrawLoad() {
	if !b.reportsLoad() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), withdrawWait)
	defer cancel()

	if err := b.cluster.Load.Withdraw(ctx); err != nil {
		b.log.Debug("tether: withdrawing the load report failed; it expires on its own", slog.Any("error", err))
	}
}

// runLoad samples this node's usage at once and then every interval until
// ctx ends, and in a cell with a Load reports each sample. A failed report is
// not retried: the next tick sends a fresh one.
func (b *Broker) runLoad(ctx context.Context, started time.Time, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	var (
		warned time.Time
		// reported is every tenant's entry as of the last report NATS
		// took, the ones left out of it included.
		reported map[string]cluster.TenantLoad
	)

	for {
		snap := b.sampleLoad(started)

		if b.reportsLoad() {
			tenants := reportTenants(snap.tenants, reported)

			err := b.reportLoad(ctx, snap, tenants, interval)

			switch {
			case err == nil:
				reported = snap.tenants
			case ctx.Err() != nil:
			default:
				now := time.Now()
				loud := now.Sub(warned) >= loadWarnEvery

				if loud {
					warned = now
				}

				b.logReportFailure(err, snap, tenants, loud)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// sampleLoad reads this node's usage, keeps it as the latest sample (the one
// before becomes the previous, for rates) and drops the peers whose reports
// went stale. The sample holds the tenants with a connection or any count.
func (b *Broker) sampleLoad(started time.Time) *loadSnap {
	now := time.Now()
	snap := &loadSnap{startedAt: started, at: now, tenants: make(map[string]cluster.TenantLoad)}

	for _, u := range b.usageAt(now) {
		tl := tenantLoad(u)
		if tl != (cluster.TenantLoad{}) {
			snap.tenants[u.Tenant] = tl
		}
	}

	b.load.mu.Lock()
	b.load.selfPrev, b.load.selfLast = b.load.selfLast, snap

	for id, p := range b.load.peers {
		if b.load.stale > 0 && now.Sub(p.seen) > b.load.stale {
			delete(b.load.peers, id)
		}
	}
	b.load.mu.Unlock()

	return snap
}

// reportLoad publishes snap's time and tenants as this node's load report.
// It gives up after the report's lifetime (loadStaleAfter intervals): a
// report that lands later is of no use.
func (b *Broker) reportLoad(ctx context.Context, snap *loadSnap, tenants map[string]cluster.TenantLoad, interval time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, loadStaleAfter*interval)
	defer cancel()

	return b.cluster.Load.Report(ctx, &cluster.NodeLoad{
		StartedAt: snap.startedAt,
		At:        snap.at,
		Tenants:   tenants,
	})
}

// reportTenants is what goes in a load report out of a sample's tenants: all
// of them but those with no connection whose entry is the same as in the last
// report NATS took (reported, nil before the first). Every peer already holds
// that entry and keeps it (onLoad), so the report grows with the tenants that
// are active, not with every tenant the node has seen since it started.
func reportTenants(all, reported map[string]cluster.TenantLoad) map[string]cluster.TenantLoad {
	out := make(map[string]cluster.TenantLoad, len(all))

	for id, tl := range all {
		if last, ok := reported[id]; ok && tl.Connections == 0 && last == tl {
			continue
		}

		out[id] = tl
	}

	return out
}

// logReportFailure logs a load report that failed, at Warn or Error when loud
// and at Debug otherwise. A report over NATS's max_payload gets Error with
// the tenant count and size: until it fits, the peers forget this node once
// its last report expires, so cell-wide limits stop counting it.
func (b *Broker) logReportFailure(err error, snap *loadSnap, tenants map[string]cluster.TenantLoad, loud bool) {
	if !loud {
		b.log.Debug("tether: load report failed", slog.Any("error", err))

		return
	}

	if errors.Is(err, cluster.ErrTooLarge) {
		size := 0
		if data, merr := json.Marshal(&cluster.NodeLoad{
			NodeID: b.selfNodeID(), StartedAt: snap.startedAt, At: snap.at, Tenants: tenants,
		}); merr == nil {
			size = len(data)
		}

		b.log.Error("tether: load report is over NATS max_payload and was not sent; once this node's last report "+
			"expires its peers count none of its connections, so cell connection and rate limits miss this node's share",
			slog.Int("tenants", len(tenants)), slog.Int("bytes", size), slog.Any("error", err))

		return
	}

	b.log.Warn("tether: load report failed; peers count this node's last report until it expires",
		slog.Any("error", err))
}

// watchLoad keeps the view up to date with the other nodes' reports until
// ctx ends. A watch that cannot start is tried again, 200 ms doubling to 5 s;
// once running, Load.Watch restarts itself.
func (b *Broker) watchLoad(ctx context.Context) {
	delay := loadWatchRetry

	for {
		stop, err := b.cluster.Load.Watch(ctx, b.onLoad)
		if err == nil {
			<-ctx.Done()
			stop()

			return
		}

		if ctx.Err() != nil {
			return
		}

		b.log.Warn("tether: watching the cell's load reports failed, retrying", slog.Any("error", err),
			slog.Duration("retry_in", delay))

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		delay = min(2*delay, loadWatchRetryMax)
	}
}

// onLoad is the load watch's callback: it keeps another node's latest report
// and the one before, or forgets a node whose report is gone. This node's own
// report is ignored; its numbers come from its counters.
func (b *Broker) onLoad(nodeID string, r *cluster.NodeLoad, deleted bool) {
	if nodeID == b.selfNodeID() {
		return
	}

	b.load.mu.Lock()
	defer b.load.mu.Unlock()

	if deleted || r == nil {
		delete(b.load.peers, nodeID)

		return
	}

	if b.load.peers == nil {
		b.load.peers = make(map[string]*peerLoad)
	}

	p := b.load.peers[nodeID]
	if p == nil {
		p = &peerLoad{}
		b.load.peers[nodeID] = p
	}

	p.prev, p.last = p.last, &loadSnap{startedAt: r.StartedAt, at: r.At, tenants: keepIdle(p.last, r)}
	p.seen = time.Now()
}

// keepIdle is r's tenants plus those of last that r leaves out, with no
// connections: a node leaves out a tenant whose entry has not moved since its
// last report and that has no connection (reportTenants), so its counters
// are the ones last holds. Only within one run of the node: a report with
// another StartedAt counts from zero again.
func keepIdle(last *loadSnap, r *cluster.NodeLoad) map[string]cluster.TenantLoad {
	tenants := r.Tenants

	if last == nil || !last.startedAt.Equal(r.StartedAt) {
		return tenants
	}

	for id, tl := range last.tenants {
		if _, ok := tenants[id]; ok {
			continue
		}

		if tenants == nil {
			tenants = make(map[string]cluster.TenantLoad, len(last.tenants))
		}

		tl.Connections = 0
		tenants[id] = tl
	}

	return tenants
}

// CellUsage returns tenantID's usage across the cell: this node's live usage
// plus the latest report of every other node. Connections is the tenant's
// live connections on every node; the counters are each node's since its own
// start. A node whose report has not arrived yet, or has gone, is missing.
// Outside a cell, or in one without Cluster.Load, it is this node's usage.
func (b *Broker) CellUsage(tenantID string) TenantUsage {
	now := time.Now()
	u := b.localUsage(tenantID, now)

	b.load.mu.Lock()
	defer b.load.mu.Unlock()

	for _, p := range b.load.peers {
		if !b.load.live(p, now) {
			continue
		}

		tl, ok := p.last.tenants[tenantID]
		if !ok {
			continue
		}

		u.Connections += tl.Connections
		u.MessagesIn += tl.MessagesIn
		u.MessagesOut += tl.MessagesOut
		u.BytesIn += tl.BytesIn
		u.BytesOut += tl.BytesOut
		u.ConnectionTime += millis(tl.ConnectionMillis)
	}

	return u
}

// RemoteConnections returns how many network connections tenantID holds on
// the other nodes of the cell, from their latest reports: 0 outside a cell,
// in one without Cluster.Load, and before the reports arrive. It runs on every
// CONNECT of a tenant with a connection limit: it takes only the view's lock
// and allocates nothing.
func (b *Broker) RemoteConnections(tenantID string) int {
	if !b.reportsLoad() {
		return 0
	}

	now := time.Now()

	b.load.mu.Lock()
	defer b.load.mu.Unlock()

	n := 0

	for _, p := range b.load.peers {
		if b.load.live(p, now) {
			n += p.last.tenants[tenantID].Connections
		}
	}

	return n
}

// TenantRates returns tenantID's traffic rates summed over the cell's nodes:
// each other node's from its last two reports, this node's from its last two
// samples. A node with one report so far, or whose two reports straddle a
// restart, adds nothing.
func (b *Broker) TenantRates(tenantID string) Rates {
	now := time.Now()

	b.load.mu.Lock()
	defer b.load.mu.Unlock()

	r := pairRates(b.load.selfPrev, b.load.selfLast, tenantID)

	for _, p := range b.load.peers {
		if b.load.live(p, now) {
			r.add(pairRates(p.prev, p.last, tenantID))
		}
	}

	return r
}

// Nodes lists the cell's nodes this node knows, itself included, sorted by
// node ID: this node from its live usage, every other from its latest report.
// Outside a cell it is one row, NodeID "" and Self true.
func (b *Broker) Nodes() []NodeUsage {
	now := time.Now()

	self := NodeUsage{NodeID: b.selfNodeID(), Self: true, StartedAt: b.StartedAt(), At: now}

	for _, u := range b.usageAt(now) {
		addUsage(&self.Totals, u)
	}

	self.Connections = self.Totals.Connections

	b.load.mu.Lock()

	self.Rates = nodeRates(b.load.selfPrev, b.load.selfLast)
	out := make([]NodeUsage, 0, 1+len(b.load.peers))
	out = append(out, self)

	for id, p := range b.load.peers {
		if !b.load.live(p, now) {
			continue
		}

		n := NodeUsage{NodeID: id, StartedAt: p.last.startedAt, At: p.last.at, Rates: nodeRates(p.prev, p.last)}

		for _, tl := range p.last.tenants {
			addUsage(&n.Totals, usageOfLoad(tl))
		}

		n.Connections = n.Totals.Connections
		out = append(out, n)
	}

	b.load.mu.Unlock()

	slices.SortFunc(out, func(x, y NodeUsage) int { return strings.Compare(x.NodeID, y.NodeID) })

	return out
}

// tenantLoad is u as a load report entry.
func tenantLoad(u TenantUsage) cluster.TenantLoad {
	ms := u.ConnectionTime.Milliseconds()

	return cluster.TenantLoad{
		Connections:      u.Connections,
		MessagesIn:       u.MessagesIn,
		MessagesOut:      u.MessagesOut,
		BytesIn:          u.BytesIn,
		BytesOut:         u.BytesOut,
		ConnectionMillis: uint64(max(ms, 0)), // #nosec G115 -- not negative.
	}
}

// usageOfLoad is a load report entry as a TenantUsage without a tenant.
func usageOfLoad(tl cluster.TenantLoad) TenantUsage {
	return TenantUsage{
		Connections:    tl.Connections,
		MessagesIn:     tl.MessagesIn,
		MessagesOut:    tl.MessagesOut,
		BytesIn:        tl.BytesIn,
		BytesOut:       tl.BytesOut,
		ConnectionTime: millis(tl.ConnectionMillis),
	}
}

// addUsage adds u's numbers to sum; sum's Tenant stays as it is.
func addUsage(sum *TenantUsage, u TenantUsage) {
	sum.Connections += u.Connections
	sum.MessagesIn += u.MessagesIn
	sum.MessagesOut += u.MessagesOut
	sum.BytesIn += u.BytesIn
	sum.BytesOut += u.BytesOut
	sum.ConnectionTime += u.ConnectionTime
}

// millis is ms milliseconds, held to the largest Duration.
func millis(ms uint64) time.Duration {
	const maxMillis = uint64(math.MaxInt64 / int64(time.Millisecond))

	return time.Duration(min(ms, maxMillis)) * time.Millisecond // #nosec G115 -- held to maxMillis.
}

// pairRates is tenantID's rates between two samples of one node. Zero when
// either is missing, when they straddle a restart (StartedAt changed), when
// no time passed between them, and for each counter that went down.
func pairRates(prev, last *loadSnap, tenantID string) Rates {
	secs, ok := pairSeconds(prev, last)
	if !ok {
		return Rates{}
	}

	return tenantRates(prev.tenants[tenantID], last.tenants[tenantID], secs)
}

// nodeRates is pairRates summed over the node's tenants in last. A tenant in
// prev and not in last went down to zero, which counts as no rate.
func nodeRates(prev, last *loadSnap) Rates {
	secs, ok := pairSeconds(prev, last)
	if !ok {
		return Rates{}
	}

	var r Rates
	for id, tl := range last.tenants {
		r.add(tenantRates(prev.tenants[id], tl, secs))
	}

	return r
}

// pairSeconds is the time between two samples of one node, and whether they
// can give a rate at all.
func pairSeconds(prev, last *loadSnap) (float64, bool) {
	if prev == nil || last == nil || !prev.startedAt.Equal(last.startedAt) {
		return 0, false
	}

	secs := last.at.Sub(prev.at).Seconds()

	return secs, secs > 0
}

// tenantRates is the per-second change from p to l over secs.
func tenantRates(p, l cluster.TenantLoad, secs float64) Rates {
	return Rates{
		MessagesInPerSec:  perSec(p.MessagesIn, l.MessagesIn, secs),
		MessagesOutPerSec: perSec(p.MessagesOut, l.MessagesOut, secs),
		BytesInPerSec:     perSec(p.BytesIn, l.BytesIn, secs),
		BytesOutPerSec:    perSec(p.BytesOut, l.BytesOut, secs),
	}
}

// perSec is (to - from) / secs, or zero when the counter went down.
func perSec(from, to uint64, secs float64) float64 {
	if to < from {
		return 0
	}

	return float64(to-from) / secs
}

// runShares sets each tenant's share of its message rate every interval until
// ctx ends. It runs only in a cell with a Cluster.Load, whose reports tell it
// where the tenant's connections are.
func (b *Broker) runShares(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	var held map[string]struct{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			held = b.updateShares(held)
		}
	}
}

// updateShares gives each tenant this node's share of its message budget: its
// local connections over all its connections in the cell when it has local
// ones, else one slice per live node (a publish over HTTP on a node without
// the tenant's connections still gets a slice). The budget is not a lock-step
// split: reports are an interval old, so the shares of all nodes may briefly
// sum above 1. held holds the tenants that got a share under 1 last time, so
// one that left every node is still recomputed: in a cell of more than one
// node its share is 1/nodes and it stays in held, and only when this node is
// alone does it go back to the whole budget (1). It returns the new set. No lock is held across the Limiter or Members calls.
func (b *Broker) updateShares(held map[string]struct{}) map[string]struct{} {
	now := time.Now()
	local := make(map[string]int)

	for _, u := range b.usageAt(now) {
		local[u.Tenant] = u.Connections
	}

	remote := make(map[string]int)
	peers := 0

	b.load.mu.Lock()

	for _, p := range b.load.peers {
		if !b.load.live(p, now) {
			continue
		}

		peers++

		for id, tl := range p.last.tenants {
			remote[id] += tl.Connections
		}
	}

	b.load.mu.Unlock()

	nodes := 1 + peers

	if b.cluster != nil && b.cluster.Members != nil {
		if live := b.cluster.Members.Live(); len(live) > 0 {
			nodes = len(live)
		}
	}

	tenants := make(map[string]struct{}, len(local)+len(remote)+len(held))

	for _, m := range []map[string]int{local, remote} {
		for id := range m {
			tenants[id] = struct{}{}
		}
	}

	for id := range held {
		tenants[id] = struct{}{}
	}

	next := make(map[string]struct{})

	for id := range tenants {
		share := 1 / float64(nodes)

		if l := local[id]; l > 0 {
			share = float64(l) / float64(l+remote[id])
		}

		b.deps.Limits.SetShare(id, share)

		if share < 1 {
			next[id] = struct{}{}
		}
	}

	return next
}
