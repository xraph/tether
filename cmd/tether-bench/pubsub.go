package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// payloadHeader is the part of a payload the bench reads: the send time as an
// offset on the bench clock, then the publisher's sequence number.
const payloadHeader = 16

// stormTail is how long past the last reconnect the storm phase runs, so the
// brokers' cleanup of the dropped sessions falls inside it.
const stormTail = 2 * time.Second

// pubsubOpts is what a pubsub run does.
type pubsubOpts struct {
	QoS         byte          `json:"qos"`
	Clients     int           `json:"measured_clients"`
	Rate        float64       `json:"rate_per_s"`
	Duration    time.Duration `json:"-"`
	Warmup      time.Duration `json:"-"`
	Placement   string        `json:"placement"`
	Size        int           `json:"payload_bytes"`
	Background  int           `json:"background_clients"`
	ConnRate    float64       `json:"connect_rate_per_s"`
	Parallel    int           `json:"-"`
	Storm       int           `json:"storm_clients"`
	StormWindow time.Duration `json:"-"`
	StormAt     time.Duration `json:"-"`
	// StormExternal means another process runs the storm (tether-bench storm),
	// so this one only marks the phases.
	StormExternal bool `json:"storm_external,omitempty"`
	// StartAt, when set, is the moment the publishers start, so a second process
	// can aim a storm at the measured window.
	StartAt time.Time `json:"-"`
}

// stormPhases reports whether the run has baseline, storm and after phases.
func (o pubsubOpts) stormPhases() bool { return o.Storm > 0 || o.StormExternal }

// phaseResult is the delivery latency of the messages sent in one stretch of
// the measured window.
type phaseResult struct {
	Name      string  `json:"name"`
	FromS     float64 `json:"from_s"`
	ToS       float64 `json:"to_s"`
	Delivered summary `json:"publish_to_deliver"`
}

// stormResult is what the reconnect storm did.
type stormResult struct {
	Clients   int       `json:"clients"`
	WindowS   float64   `json:"window_s"`
	PerSecond float64   `json:"reconnects_per_s"`
	Reconnect connStats `json:"reconnect"`
}

// pubsubResult is the report of the pubsub command.
type pubsubResult struct {
	Machine    machine       `json:"machine"`
	Targets    []string      `json:"targets"`
	Opts       pubsubOpts    `json:"options"`
	DurationS  float64       `json:"duration_s"`
	WarmupS    float64       `json:"warmup_s"`
	Background *connStats    `json:"background,omitempty"`
	Measured   connStats     `json:"measured_connect"`
	Phases     []phaseResult `json:"phases"`
	Overall    summary       `json:"overall"`
	AckLatency summary       `json:"publish_to_ack,omitempty"`
	Sent       int64         `json:"sent"`
	Acked      int64         `json:"acked"`
	Failed     int64         `json:"publish_failed"`
	Stalled    int64         `json:"publish_skipped_window_full"`
	Received   int64         `json:"received_unique"`
	Duplicates int64         `json:"duplicates"`
	// Lost counts messages the broker took (a PUBACK at QoS 1, a clean write at
	// QoS 0) that the paired subscriber never received. Delivered counts the
	// other way: messages received that were never acknowledged, which are not
	// losses and must not hide one.
	Lost       int64        `json:"lost"`
	Unacked    int64        `json:"received_but_not_acked"`
	Lateness   summary      `json:"send_lateness"`
	StormSetup *connStats   `json:"storm_group_connect,omitempty"`
	CPU        []phaseCPU   `json:"cpu_cores,omitempty"`
	Storm      *stormResult `json:"storm,omitempty"`
	// StormP99Ratio is the storm phase's p99 over the baseline phase's.
	StormP99Ratio float64     `json:"storm_p99_ratio,omitempty"`
	Memory        *memReport  `json:"memory,omitempty"`
	Connected     int         `json:"connected_total"`
	NodeSide      []nodeStats `json:"node_side,omitempty"`
	LostConns     int         `json:"measured_connections_lost"`
}

// phaseCPU is the CPU the cell's processes used in one phase, in cores (CPU
// seconds per wall second), summed over the brokers and over the NATS servers.
type phaseCPU struct {
	Name    string  `json:"name"`
	Brokers float64 `json:"brokers"`
	NATS    float64 `json:"nats_servers"`
}

// sample is one delivered message: when it was sent and how long it took.
type sample struct{ sent, lat time.Duration }

// subscriber is the receiving end of one pair.
type subscriber struct {
	mu      sync.Mutex
	samples []sample
	seen    map[uint64]struct{}
	dups    int64
	bad     int64
}

func (s *subscriber) onMsg(p *paho.Publish) { s.receive(p.Payload, time.Since(epoch)) }

// payload is the message for slot k of a publisher, stamped with the time that
// slot was scheduled for.
func payload(sched time.Time, k uint64, size int) []byte {
	buf := make([]byte, max(size, payloadHeader))
	binary.BigEndian.PutUint64(buf[0:8], uint64(sched.Sub(epoch))) //nolint:gosec // G115: a duration since start, never negative.
	binary.BigEndian.PutUint64(buf[8:16], k)

	return buf
}

// receive records a message that arrived at now on the bench clock.
func (s *subscriber) receive(payload []byte, now time.Duration) {
	if len(payload) < payloadHeader {
		s.mu.Lock()
		s.bad++
		s.mu.Unlock()

		return
	}

	sent := time.Duration(binary.BigEndian.Uint64(payload[0:8])) //nolint:gosec // G115: a send time, far below 2^63 ns.
	seq := binary.BigEndian.Uint64(payload[8:16])

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, dup := s.seen[seq]; dup {
		s.dups++

		return
	}

	s.seen[seq] = struct{}{}
	s.samples = append(s.samples, sample{sent: sent, lat: now - sent})
}

// publisher is the sending end of one pair.
type publisher struct {
	c        *client
	topic    string
	sent     atomic.Int64
	acked    atomic.Int64
	failed   atomic.Int64
	stalled  atomic.Int64
	mu       sync.Mutex
	ackLat   []time.Duration
	late     []time.Duration
	good     map[uint64]struct{} // sequences the broker took: acked at QoS 1, written at QoS 0
	inflight chan struct{}
}

// took records that the broker took message k.
func (p *publisher) took(k uint64) {
	p.mu.Lock()
	p.good[k] = struct{}{}
	p.mu.Unlock()
}

// run publishes until ctx ends: every interval, one message. QoS 0 writes inline.
// QoS 1 sends from its own goroutine and counts a message it had no room for as
// skipped, rather than let a slow broker make the offered rate drift.
//
// A message is stamped with the time it was scheduled to go, start + k*interval,
// not the time the loop got round to it. A publisher that wakes late (a garbage
// collection, a busy scheduler, a blocked write) then shows the delay in the
// latency of the messages it holds up, instead of hiding it. The lateness itself
// is recorded, so a run can say how often that happened.
func (p *publisher) run(ctx context.Context, qos byte, size int, interval, offset time.Duration, wg *sync.WaitGroup) {
	start := time.Now().Add(offset)

	for k := 0; ctx.Err() == nil; k++ {
		sched := start.Add(time.Duration(k) * interval)

		sleepUntil(ctx, sched)

		if ctx.Err() != nil {
			return
		}

		p.mu.Lock()
		p.late = append(p.late, max(time.Since(sched), 0))
		p.mu.Unlock()

		seq := uint64(k)
		buf := payload(sched, seq, size)

		if qos == 0 {
			p.sent.Add(1)

			if _, err := p.c.pc.Publish(ctx, &paho.Publish{Topic: p.topic, QoS: 0, Payload: buf}); err != nil {
				p.failed.Add(1)
			} else {
				p.took(seq)
			}

			continue
		}

		select {
		case p.inflight <- struct{}{}:
		default:
			p.stalled.Add(1)

			continue
		}

		p.sent.Add(1)

		wg.Go(func() {
			defer func() { <-p.inflight }()

			pctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			resp, err := p.c.pc.Publish(pctx, &paho.Publish{Topic: p.topic, QoS: qos, Payload: buf})
			lat := time.Since(sched)

			if err != nil || resp == nil || resp.ReasonCode >= 0x80 {
				p.failed.Add(1)

				return
			}

			p.acked.Add(1)
			p.mu.Lock()
			p.ackLat = append(p.ackLat, lat)
			p.good[seq] = struct{}{}
			p.mu.Unlock()
		})
	}
}

// runPubsub measures. It connects the background clients, the storm group and
// the measured pairs, runs the publishers for warmup plus duration, and reports.
func (o pubsubOpts) measure(ctx context.Context, t *target) (*pubsubResult, error) {
	pairs := o.Clients / 2
	if pairs < 1 {
		return nil, errors.New("pubsub: --clients must be at least 2")
	}

	if o.QoS > 1 {
		return nil, errors.New("pubsub: --qos must be 0 or 1")
	}

	if o.stormPhases() && o.Duration < o.StormAt+o.StormWindow+stormTail+5*time.Second {
		return nil, fmt.Errorf("pubsub: --duration %s is too short for a storm at %s over %s: need at least %s",
			o.Duration, o.StormAt, o.StormWindow, o.StormAt+o.StormWindow+stormTail+5*time.Second)
	}

	res := &pubsubResult{
		Machine: describeMachine(ctx), Targets: t.addrs, Opts: o,
		DurationS: o.Duration.Seconds(), WarmupS: o.Warmup.Seconds(),
	}

	rssBefore := rssBytes(ctx, t.pids)

	var all []*client

	// A reset, not a FIN: a closed port then leaves no TIME_WAIT, which on one
	// loopback address is the whole port supply for the next run.
	defer func() { closeAll(all, true) }()

	// Idle background connections, the load the measured pairs share the
	// brokers with.
	if o.Background > 0 {
		cs, st := t.connectMany(ctx, "g", o.Background, o.ConnRate, o.Parallel, nil, nil)
		all = append(all, cs...)
		res.Background = &st
	}

	// The storm group, connected and idle until the storm drops it.
	var group []*client

	if o.Storm > 0 {
		var st connStats

		group, st = t.connectMany(ctx, "s", o.Storm, o.ConnRate, o.Parallel, nil, nil)
		all = append(all, group...)
		res.StormSetup = &st
	}

	// The measured pairs. Subscriber j sits on node j mod N. Its publisher sits
	// on the next node when the placement is cross, so a message leaves one
	// broker and reaches the other through NATS, and on the same node otherwise.
	nodes := len(t.addrs)
	subNode := func(j int) int { return j % nodes }
	pubNode := func(j int) int {
		if o.Placement == "same" {
			return subNode(j)
		}

		return (j + 1) % nodes
	}

	subs := make([]*subscriber, pairs)
	for j := range subs {
		subs[j] = &subscriber{seen: map[uint64]struct{}{}}
	}

	subCs, subSt := t.connectMany(ctx, "sub", pairs, o.ConnRate, o.Parallel, subNode,
		func(j int) func(*paho.Publish) { return subs[j].onMsg })
	pubCs, pubSt := t.connectMany(ctx, "pub", pairs, o.ConnRate, o.Parallel, pubNode, nil)
	all = append(all, subCs...)
	all = append(all, pubCs...)
	res.Measured = mergeStats(subSt, pubSt)

	if len(subCs) < pairs || len(pubCs) < pairs {
		return res, fmt.Errorf("pubsub: only %d of %d subscribers and %d of %d publishers connected: %v",
			len(subCs), pairs, len(pubCs), pairs, res.Measured.Errors)
	}

	// connectMany returns clients in completion order, so find each one by its index.
	subByIdx := indexClients(subCs, t.prefix+"-sub-")
	pubByIdx := indexClients(pubCs, t.prefix+"-pub-")

	for j := range pairs {
		sc, pc := subByIdx[j], pubByIdx[j]
		if sc == nil || pc == nil {
			return res, fmt.Errorf("pubsub: pair %d is missing a client", j)
		}

		sctx, cancel := context.WithTimeout(ctx, t.timeout)
		_, err := sc.pc.Subscribe(sctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{
			Topic: fmt.Sprintf("%s/t%d", benchTopicRoot, j), QoS: o.QoS,
		}}})

		cancel()

		if err != nil {
			return res, fmt.Errorf("pubsub: subscribe %d: %w", j, err)
		}
	}

	res.Connected = len(all)

	// Let the subscriptions reach every node and the connections settle, then
	// take the memory and the node-side counts of the loaded cell.
	sleepUntil(ctx, time.Now().Add(2*time.Second))

	res.Memory = memoryReport(rssBefore, rssBytes(ctx, t.pids), len(all))
	if o.StormExternal && res.Memory != nil {
		// The storm process's clients sit on the brokers too, and this process
		// doesn't count them, so a per-connection figure would come out far too big.
		res.Memory.PerConnKiB = 0
	}

	if ns, err := t.adminStats(ctx); err == nil {
		res.NodeSide = ns
	}

	// Publishers.
	pubs := make([]*publisher, pairs)
	for j := range pubs {
		pubs[j] = &publisher{c: pubByIdx[j], topic: fmt.Sprintf("%s/t%d", benchTopicRoot, j), inflight: make(chan struct{}, 64), good: map[uint64]struct{}{}}
	}

	interval := time.Duration(float64(time.Second) * float64(pairs) / o.Rate)

	if !o.StartAt.IsZero() {
		if time.Now().After(o.StartAt) {
			return res, fmt.Errorf("pubsub: --start-at %s has passed; setup took too long", o.StartAt.Format(time.RFC3339))
		}

		sleepUntil(ctx, o.StartAt)
	}

	pctx, stopPub := context.WithCancel(ctx)
	defer stopPub()

	var pubWG, inflightWG sync.WaitGroup

	t0 := time.Since(epoch) // run start on the bench clock
	mStart := t0 + o.Warmup // the measured window starts here

	for j, p := range pubs {
		offset := time.Duration(float64(interval) * float64(j) / float64(pairs)) // spread the pairs across one interval

		pubWG.Go(func() { p.run(pctx, o.QoS, o.Size, interval, offset, &inflightWG) })
	}

	// The storm.
	type stormOut struct {
		res stormResult
		cs  []*client
	}

	var stormDone chan stormOut

	if o.Storm > 0 {
		stormDone = make(chan stormOut, 1)
		at := time.Now().Add(o.Warmup + o.StormAt)

		go func() {
			sleepUntil(ctx, at)

			sr, cs := runStorm(ctx, t, group, o.StormWindow, o.Parallel)
			stormDone <- stormOut{sr, cs}
		}()
	}

	var cpuDone chan []phaseCPU

	if len(t.pids)+len(t.serverPids) > 0 {
		cpuDone = make(chan []phaseCPU, 1)
		windowStart := time.Now().Add(o.Warmup)

		go func() { cpuDone <- o.sampleCPU(ctx, t, windowStart) }()
	}

	sleepUntil(ctx, time.Now().Add(o.Warmup+o.Duration))
	stopPub()
	pubWG.Wait()
	inflightWG.Wait()

	// Late deliveries still count: give them a moment.
	sleepUntil(ctx, time.Now().Add(2*time.Second))

	if stormDone != nil {
		select {
		case so := <-stormDone:
			res.Storm = &so.res
			all = append(all, so.cs...)
		case <-time.After(o.StormWindow + 30*time.Second):
			return res, errors.New("pubsub: the storm did not finish")
		}
	}

	res.NodeSide = firstNonEmpty(res.NodeSide, t.adminStatsQuiet(ctx))
	collect(res, o, subs, pubs, append(append([]*client(nil), subCs...), pubCs...), mStart)

	if cpuDone != nil {
		res.CPU = <-cpuDone
	}

	return res, ctx.Err()
}

// runStorm drops every client of the group at once, then has each reconnect at a
// random moment inside the window, under the ID it had. It returns the
// reconnected clients, which the caller closes.
func runStorm(ctx context.Context, t *target, group []*client, window time.Duration, parallel int) (stormResult, []*client) {
	byIdx := indexClients(group, t.prefix+"-s-")

	closeAll(group, true)

	cn := newConnector(len(t.addrs))
	start := time.Now()

	var wg sync.WaitGroup

	sem := make(chan struct{}, parallel)

	for i, old := range byIdx {
		delay := time.Duration(rand.Float64() * float64(window)) //nolint:gosec // G404: jitter, not security.
		node := old.node

		wg.Go(func() {
			sleepUntil(ctx, start.Add(delay))

			sem <- struct{}{}

			defer func() { <-sem }()

			c, lat, err := t.dial(ctx, "s", i, node, nil)
			cn.add(c, lat, err)
		})
	}

	wg.Wait()

	// The reconnected clients stay up until the run ends, and the caller closes them.
	return stormResult{
		Clients: len(group), WindowS: window.Seconds(),
		PerSecond: float64(len(group)) / window.Seconds(), Reconnect: cn.stats(time.Since(start)),
	}, cn.clients
}

// phaseBound is a stretch of the measured window, from and to counted from its start.
type phaseBound struct {
	name     string
	from, to time.Duration
}

// bounds are the phases of the run: one, or baseline, storm and after.
func (o pubsubOpts) bounds() []phaseBound {
	if o.stormPhases() {
		end := o.StormAt + o.StormWindow + stormTail

		return []phaseBound{{"baseline", 0, o.StormAt}, {"storm", o.StormAt, end}, {"after", end, o.Duration}}
	}

	return []phaseBound{{"steady", 0, o.Duration}}
}

// sampleCPU reads the CPU time of the cell's processes at the edges of every
// phase and returns the cores each phase used. It reads nothing and returns nil
// when it cannot (ps failed, or no process IDs).
func (o pubsubOpts) sampleCPU(ctx context.Context, t *target, windowStart time.Time) []phaseCPU {
	bs := o.bounds()
	edges := make([]time.Duration, 1, len(bs)+1)
	edges[0] = bs[0].from

	for _, b := range bs {
		edges = append(edges, b.to)
	}

	brokers := make([]float64, len(edges))
	servers := make([]float64, len(edges))

	for i, e := range edges {
		sleepUntil(ctx, windowStart.Add(e))

		brokers[i], servers[i] = sum(cpuSeconds(ctx, t.pids)), sum(cpuSeconds(ctx, t.serverPids))
	}

	out := make([]phaseCPU, len(bs))

	for i, b := range bs {
		secs := (b.to - b.from).Seconds()
		out[i] = phaseCPU{Name: b.name, Brokers: (brokers[i+1] - brokers[i]) / secs, NATS: (servers[i+1] - servers[i]) / secs}
	}

	return out
}

func sum(v []float64) float64 {
	var t float64

	for _, x := range v {
		t += x
	}

	return t
}

// collect turns what the pairs saw into the result. cs are the measured clients.
func collect(res *pubsubResult, o pubsubOpts, subs []*subscriber, pubs []*publisher, cs []*client, mStart time.Duration) {
	var (
		all  []sample
		acks []time.Duration
		late []time.Duration
	)

	for j, s := range subs {
		s.mu.Lock()
		all = append(all, s.samples...)
		res.Received += int64(len(s.seen))
		res.Duplicates += s.dups

		p := pubs[j]
		p.mu.Lock()

		for seq := range p.good {
			if _, ok := s.seen[seq]; !ok {
				res.Lost++
			}
		}

		for seq := range s.seen {
			if _, ok := p.good[seq]; !ok {
				res.Unacked++
			}
		}

		acks = append(acks, p.ackLat...)
		late = append(late, p.late...)
		p.mu.Unlock()
		s.mu.Unlock()
	}

	for _, p := range pubs {
		res.Sent += p.sent.Load()
		res.Acked += p.acked.Load()
		res.Failed += p.failed.Load()
		res.Stalled += p.stalled.Load()
	}

	for _, c := range cs {
		select {
		case <-c.lost:
			res.LostConns++
		default:
		}
	}

	// Phases are by send time, relative to the start of the measured window.
	bounds := o.bounds()
	inWindow := make([]time.Duration, 0, len(all))

	for _, b := range bounds {
		var d []time.Duration

		for _, s := range all {
			if rel := s.sent - mStart; rel >= b.from && rel < b.to {
				d = append(d, s.lat)
			}
		}

		inWindow = append(inWindow, d...)
		res.Phases = append(res.Phases, phaseResult{Name: b.name, FromS: b.from.Seconds(), ToS: b.to.Seconds(), Delivered: summarize(d)})
	}

	res.Overall = summarize(inWindow)
	res.AckLatency = summarize(acks)
	res.Lateness = summarize(late)

	if len(res.Phases) == 3 && res.Phases[0].Delivered.P99 > 0 {
		res.StormP99Ratio = res.Phases[1].Delivered.P99 / res.Phases[0].Delivered.P99
	}
}

func (t *target) adminStatsQuiet(ctx context.Context) []nodeStats {
	ns, err := t.adminStats(ctx)
	if err != nil {
		return nil
	}

	return ns
}

func firstNonEmpty(a, b []nodeStats) []nodeStats {
	if len(b) > 0 {
		return b
	}

	return a
}

// mergeStats adds two batches of connection attempts together. The batches ran
// one after the other, so the elapsed times add.
func mergeStats(a, b connStats) connStats {
	out := connStats{
		OK: a.OK + b.OK, Failed: a.Failed + b.Failed, Elapsed: a.Elapsed + b.Elapsed,
		Errors: map[string]int{}, raw: append(append([]time.Duration(nil), a.raw...), b.raw...),
	}

	out.Latency = summarize(append([]time.Duration(nil), out.raw...))

	if out.Elapsed > 0 {
		out.Achieved = float64(out.OK+out.Failed) / out.Elapsed
	}

	for _, m := range []map[string]int{a.Errors, b.Errors} {
		for k, v := range m {
			out.Errors[k] += v
		}
	}

	out.PerNode = make([]int, len(a.PerNode))
	for i := range a.PerNode {
		out.PerNode[i] = a.PerNode[i] + b.PerNode[i]
	}

	return out
}

// indexClients maps a client's number to it, by the number at the end of its ID.
func indexClients(cs []*client, prefix string) map[int]*client {
	out := make(map[int]*client, len(cs))

	for _, c := range cs {
		var n int

		if _, err := fmt.Sscanf(c.id[len(prefix):], "%d", &n); err == nil {
			out[n] = c
		}
	}

	return out
}

func runPubsub(args []string, stdout, stderr io.Writer) error {
	fs := newFlags("pubsub", stderr)
	tf := addTargetFlags(fs)

	var o pubsubOpts

	qos := fs.Int("qos", 1, "QoS of the publishes and the subscriptions, 0 or 1")
	fs.IntVar(&o.Clients, "clients", 200, "measured clients: half subscribe, half publish, one publisher per subscriber")
	fs.Float64Var(&o.Rate, "rate", 2000, "publishes per second across all publishers")
	fs.DurationVar(&o.Duration, "duration", 30*time.Second, "length of the measured window")
	fs.DurationVar(&o.Warmup, "warmup", 5*time.Second, "publishing before the window, not measured")
	fs.StringVar(&o.Placement, "placement", "cross", "cross: a pair's publisher and subscriber sit on different nodes; same: on one node")
	fs.IntVar(&o.Size, "size", 64, "payload bytes (at least 16)")
	fs.IntVar(&o.Background, "background", 0, "idle connections to hold while measuring")
	fs.Float64Var(&o.ConnRate, "conn-rate", 1000, "connections opened per second while setting up")
	fs.IntVar(&o.Parallel, "parallel", 256, "most dials in flight at once")
	fs.IntVar(&o.Storm, "storm", 0, "clients a reconnect storm drops and reconnects mid-run (0 for none)")
	fs.DurationVar(&o.StormWindow, "storm-window", 10*time.Second, "the storm's reconnects are spread over this long")
	fs.DurationVar(&o.StormAt, "storm-at", 10*time.Second, "storm start, from the start of the measured window")
	fs.BoolVar(&o.StormExternal, "storm-external", false, "another process (tether-bench storm) runs the storm; only mark the phases")
	startAt := fs.String("start-at", "", "start publishing at this time (RFC 3339 or Unix seconds), to line up with a storm process")
	asJSON := fs.Bool("json", false, "print the result as JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}

	o.QoS = byte(*qos) //nolint:gosec // G115: validated to be 0 or 1 below.

	if *qos < 0 || *qos > 1 || (o.Placement != "cross" && o.Placement != "same") || o.Rate <= 0 || o.Parallel < 1 {
		return errors.New("pubsub: --qos must be 0 or 1, --placement cross or same, --rate above 0 and --parallel at least 1")
	}

	if *startAt != "" {
		var err error

		if o.StartAt, err = parseWhen(*startAt); err != nil {
			return err
		}
	}

	t, err := tf.build()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res, err := o.measure(ctx, t)
	if res != nil {
		if *asJSON {
			if jerr := writeJSON(stdout, res); jerr != nil {
				return jerr
			}
		} else {
			printPubsub(stdout, res)
		}
	}

	return err
}

func printPubsub(w io.Writer, r *pubsubResult) {
	o := r.Opts

	fmt.Fprintf(w, "machine: %s, %d cores, load %s\n", r.Machine.CPU, r.Machine.Cores, r.Machine.LoadAvg)
	fmt.Fprintf(w, "targets: %v\n", r.Targets)
	fmt.Fprintf(w, "QoS %d, %d measured clients (%d pairs, %s), %.0f msg/s offered, %dB payloads, %.0fs measured after %.0fs warmup\n",
		o.QoS, o.Clients, o.Clients/2, o.Placement, o.Rate, o.Size, r.DurationS, r.WarmupS)

	if b := r.Background; b != nil {
		fmt.Fprintf(w, "background: %d connected, %d failed, per node %v\n", b.OK, b.Failed, b.PerNode)
	}

	fmt.Fprintf(w, "connected in total: %d", r.Connected)

	if len(r.NodeSide) > 0 {
		fmt.Fprintf(w, " (node side: %v)", r.NodeSide)
	}

	fmt.Fprintln(w)

	if m := r.Memory; m != nil {
		fmt.Fprintf(w, "broker memory (RSS MiB) before %v, loaded %v; %.1f KiB per connection\n",
			roundAll(m.BeforeMiB), roundAll(m.AfterMiB), m.PerConnKiB)
	}

	for _, p := range r.Phases {
		fmt.Fprintf(w, "%-9s [%3.0fs,%3.0fs) publish-to-deliver: %s\n", p.Name, p.FromS, p.ToS, p.Delivered)
	}

	fmt.Fprintf(w, "overall   publish-to-deliver: %s\n", r.Overall)

	if r.AckLatency.Count > 0 {
		fmt.Fprintf(w, "publish-to-ack: %s\n", r.AckLatency)
	}

	fmt.Fprintf(w, "sent %d, acked %d, failed %d, skipped %d, received %d, duplicates %d, lost %d, received but not acked %d, measured connections lost %d\n",
		r.Sent, r.Acked, r.Failed, r.Stalled, r.Received, r.Duplicates, r.Lost, r.Unacked, r.LostConns)
	fmt.Fprintf(w, "publisher lateness (a message is stamped with its scheduled time): %s\n", r.Lateness)

	for _, c := range r.CPU {
		fmt.Fprintf(w, "cpu %-9s brokers %.2f cores, nats servers %.2f cores\n", c.Name, c.Brokers, c.NATS)
	}

	if r.StormP99Ratio > 0 {
		fmt.Fprintf(w, "storm phase p99 over baseline p99: %.2f\n", r.StormP99Ratio)
	}

	if s := r.Storm; s != nil {
		fmt.Fprintf(w, "storm: %d clients over %.0fs (%.0f/s): %d reconnected, %d failed, reconnect %s\n",
			s.Clients, s.WindowS, s.PerSecond, s.Reconnect.OK, s.Reconnect.Failed, s.Reconnect.Latency)

		for k, v := range s.Reconnect.Errors {
			fmt.Fprintf(w, "  error x%d: %s\n", v, k)
		}
	}
}
