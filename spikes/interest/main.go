// Command interest is the Phase 0, Task 3 spike: NATS subject interest at 1M
// distinct filters across a 3-server cluster, a reconnect storm against it, the
// overlapping-filter double delivery and its dedup, and the per-tenant fallback.
//
// Run from this directory with GOWORK=off; see README.md.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type config struct {
	servers     int
	brokers     int
	filters     int // per broker
	rate        int
	baseline    time.Duration
	stormDur    time.Duration
	gap         time.Duration
	churn       int // total across brokers
	fallbackDur time.Duration
	window      time.Duration // >0: interleave storm on and off windows instead of one continuous storm
	targetsPer  int
	pairs       int
	verifyPer   int
	overlapN    int
	runs        int
	mode        string
	out         string
}

// metrics keeps insertion order so tables print in a stable, readable order.
type metrics struct {
	Keys []string           `json:"keys"`
	Vals map[string]float64 `json:"vals"`
	Note map[string]string  `json:"note"`
}

func newMetrics() *metrics {
	return &metrics{Vals: map[string]float64{}, Note: map[string]string{}}
}

func (m *metrics) add(k string, v float64) {
	if _, ok := m.Vals[k]; !ok {
		m.Keys = append(m.Keys, k)
	}

	m.Vals[k] = v
	fmt.Printf("  %-44s %14.3f\n", k, v)
}

func (m *metrics) note(k, v string) {
	m.Note[k] = v
	fmt.Printf("  %-44s %s\n", k, v)
}

func main() {
	serve := flag.Bool("serve", false, "internal: run one nats-server child")
	name := flag.String("name", "", "internal: child server name")
	routes := flag.String("routes", "", "internal: child seed route")

	var cfg config

	flag.IntVar(&cfg.servers, "servers", 3, "NATS servers in the cluster")
	flag.IntVar(&cfg.brokers, "brokers", 10, "broker connections")
	flag.IntVar(&cfg.filters, "filters", 100_000, "distinct subjects per broker connection")
	flag.IntVar(&cfg.rate, "rate", 5000, "traffic msgs/s on unaffected subjects")
	flag.DurationVar(&cfg.baseline, "baseline", 30*time.Second, "baseline traffic duration")
	flag.DurationVar(&cfg.stormDur, "storm", 60*time.Second, "storm duration")
	flag.DurationVar(&cfg.gap, "gap", time.Second, "time between a device's unsubscribe and its resubscribe")
	flag.IntVar(&cfg.churn, "churn", 100_000, "devices unsubscribed and resubscribed by the storm, total")
	flag.DurationVar(&cfg.window, "window", 0, "if set, run the storm in alternating off/on windows of this length over twice -storm")
	flag.DurationVar(&cfg.fallbackDur, "fallback", 30*time.Second, "fallback traffic duration")
	flag.IntVar(&cfg.pairs, "pairs", 4, "cpu mode: alternating window pairs per run")
	flag.IntVar(&cfg.targetsPer, "targets", 100, "unaffected traffic subjects per broker")
	flag.IntVar(&cfg.verifyPer, "verify", 1000, "sampled subjects per broker in the propagation check")
	flag.IntVar(&cfg.overlapN, "overlap", 10_000, "messages per subject in the overlap check")
	flag.IntVar(&cfg.runs, "runs", 3, "runs of each mode")
	flag.StringVar(&cfg.mode, "mode", "all", "all, interest, fallback or cpu (paired interest and fallback CPU windows of -window)")
	flag.StringVar(&cfg.out, "out", "", "write per-run metrics as JSON into this directory")
	flag.Parse()

	if *serve {
		if err := runChild(*name, *routes); err != nil {
			fmt.Fprintln(os.Stderr, "child:", err)
			os.Exit(1)
		}

		return
	}

	if err := harness(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func sysctl(k string) string {
	out, err := exec.Command("sysctl", "-n", k).Output()
	if err != nil {
		return "?"
	}

	return strings.TrimSpace(string(out))
}

func loadavg() string { return sysctl("vm.loadavg") }

// load1 is the 1-minute load average as a number.
func load1() float64 {
	f := strings.Fields(strings.Trim(loadavg(), "{} "))
	if len(f) == 0 {
		return 0
	}

	v, _ := strconv.ParseFloat(f[0], 64)

	return v
}

func harness(cfg config) error {
	fmt.Printf("machine: %s, %s cores, %s bytes RAM, %s/%s, %s, nats-server %s, nats.go %s\n",
		sysctl("machdep.cpu.brand_string"), sysctl("hw.ncpu"), sysctl("hw.memsize"),
		runtime.GOOS, runtime.GOARCH, runtime.Version(), server.VERSION, nats.Version)
	fmt.Printf("config: %+v\n", cfg)
	fmt.Printf("load at start: %s\n", loadavg())

	var interest, fallback, cpu []*metrics

	for r := 1; r <= cfg.runs; r++ {
		if cfg.mode == "all" || cfg.mode == "interest" {
			fmt.Printf("\n== run %d: interest routing ==\n", r)

			m, err := runInterest(cfg)
			if err != nil {
				return fmt.Errorf("interest run %d: %w", r, err)
			}

			interest = append(interest, m)
			save(cfg, fmt.Sprintf("interest-%d", r), m)
		}

		if cfg.mode == "cpu" {
			fmt.Printf("\n== run %d: paired cpu ==\n", r)

			m, err := runCPUPaired(cfg)
			if err != nil {
				return fmt.Errorf("cpu run %d: %w", r, err)
			}

			cpu = append(cpu, m)
			save(cfg, fmt.Sprintf("cpu-%d", r), m)
		}

		if cfg.mode == "all" || cfg.mode == "fallback" {
			fmt.Printf("\n== run %d: per-tenant fallback ==\n", r)

			m, err := runFallback(cfg)
			if err != nil {
				return fmt.Errorf("fallback run %d: %w", r, err)
			}

			fallback = append(fallback, m)
			save(cfg, fmt.Sprintf("fallback-%d", r), m)
		}
	}

	table("interest routing", interest)
	table("per-tenant fallback", fallback)
	table("paired cpu", cpu)
	fmt.Printf("\nload at end: %s\n", loadavg())

	return nil
}

func save(cfg config, name string, m *metrics) {
	if cfg.out == "" {
		return
	}

	_ = os.MkdirAll(cfg.out, 0o755)

	b, _ := json.MarshalIndent(m, "", "  ")
	_ = os.WriteFile(filepath.Join(cfg.out, name+".json"), b, 0o644)
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}

	s := slices.Clone(v)
	sort.Float64s(s)

	if len(s)%2 == 1 {
		return s[len(s)/2]
	}

	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func table(title string, runs []*metrics) {
	if len(runs) == 0 {
		return
	}

	fmt.Printf("\n== %s: %d runs ==\n", title, len(runs))
	fmt.Printf("%-44s", "metric")

	for i := range runs {
		fmt.Printf(" %12s", fmt.Sprintf("run %d", i+1))
	}

	fmt.Printf(" %12s\n", "median")

	for _, k := range runs[0].Keys {
		vals := make([]float64, 0, len(runs))

		fmt.Printf("%-44s", k)

		for _, r := range runs {
			vals = append(vals, r.Vals[k])
			fmt.Printf(" %12.3f", r.Vals[k])
		}

		fmt.Printf(" %12.3f\n", median(vals))
	}
}

func connectPubs(cl *cluster) ([]*nats.Conn, error) {
	pubs := make([]*nats.Conn, len(cl.children))

	for i, c := range cl.children {
		nc, err := nats.Connect(c.clientURL, nats.Name(fmt.Sprintf("pub-%d", i)))
		if err != nil {
			closeAll(pubs)

			return nil, err
		}

		pubs[i] = nc
	}

	return pubs, nil
}

func closeAll(ncs []*nats.Conn) {
	for _, nc := range ncs {
		if nc != nil {
			nc.Close()
		}
	}
}

func startBrokers(cfg config, cl *cluster, interest bool) ([]*broker, error) {
	brokers := make([]*broker, cfg.brokers)

	for i := range brokers {
		s := i % cfg.servers

		b, err := newBroker(i, s, i*cfg.filters, cfg.filters, cl.children[s].clientURL, interest)
		if err != nil {
			stopBrokers(brokers)

			return nil, err
		}

		brokers[i] = b
	}

	var wg sync.WaitGroup
	for _, b := range brokers {
		wg.Go(b.buildIndex)
	}

	wg.Wait()
	runtime.GC()

	return brokers, nil
}

func stopBrokers(brokers []*broker) {
	for _, b := range brokers {
		if b != nil {
			b.close()
		}
	}
}

func recordMem(m *metrics, prefix string, before, after []serverStats, filters int) {
	for i := range after {
		m.add(fmt.Sprintf("%s.%s.rss_mib", prefix, after[i].Name), float64(after[i].RSS)/(1<<20))
		m.add(fmt.Sprintf("%s.%s.rss_after_gc_mib", prefix, after[i].Name), float64(after[i].RSSAfter)/(1<<20))
		m.add(fmt.Sprintf("%s.%s.heap_inuse_mib", prefix, after[i].Name), float64(after[i].HeapInuse)/(1<<20))

		if filters > 0 {
			d := float64(after[i].HeapInuse) - float64(before[i].HeapInuse)
			m.add(fmt.Sprintf("%s.%s.heap_bytes_per_filter", prefix, after[i].Name), d/float64(filters))
		}
	}
}

func recordLatency(m *metrics, prefix string, brokers []*broker, phase int) {
	h := mergeHist(brokers, phase)
	m.add(prefix+".p50_us", float64(h.ValueAtQuantile(50)))
	m.add(prefix+".p99_us", float64(h.ValueAtQuantile(99)))
	m.add(prefix+".p999_us", float64(h.ValueAtQuantile(99.9)))
	m.add(prefix+".max_us", float64(h.Max()))

	var recv int64
	for _, b := range brokers {
		recv += b.recv[phase].Load()
	}

	m.add(prefix+".received", float64(recv))
}

// phaseCPU runs fn and records server and harness CPU microseconds per message.
func phaseCPU(m *metrics, prefix string, cl *cluster, fn func() int64) int64 {
	s0 := cl.stats(false)
	u0, k0 := rusageSelf()
	l0 := load1()
	t0 := time.Now()

	sent := fn()

	el := time.Since(t0)
	s1 := cl.stats(false)
	u1, k1 := rusageSelf()

	srv := float64(sumCPU(s1) - sumCPU(s0))
	hns := float64((u1 + k1) - (u0 + k0))

	m.add(prefix+".sent", float64(sent))
	m.add(prefix+".achieved_rate", float64(sent)/el.Seconds())
	m.add(prefix+".server_cpu_us_per_msg", srv/float64(sent))
	m.add(prefix+".harness_cpu_us_per_msg", hns/float64(sent))
	m.add(prefix+".total_cpu_us_per_msg", (srv+hns)/float64(sent))
	m.add(prefix+".server_cores", srv/1e6/el.Seconds())
	m.add(prefix+".load1_start", l0)
	m.add(prefix+".load1_end", load1())

	return sent
}

func runInterest(cfg config) (*metrics, error) {
	m := newMetrics()
	m.note("load_start", loadavg())
	m.add("load1_start", load1())

	cl, err := startCluster(cfg.servers)
	if err != nil {
		return nil, err
	}
	defer cl.stop()

	pubs, err := connectPubs(cl)
	if err != nil {
		return nil, err
	}
	defer closeAll(pubs)

	brokers, err := startBrokers(cfg, cl, true)
	if err != nil {
		return nil, err
	}
	defer stopBrokers(brokers)

	total := cfg.brokers * cfg.filters
	before := cl.stats(true)

	// 1. Interest propagation. Probes for each broker's last subject start at
	// T0 from every other server, so the time recorded is when that last
	// subscription became reachable cluster-wide.
	trk := newTracker(brokers, cfg.servers)
	for _, b := range brokers {
		b.trk.Store(trk)
	}

	stop := make(chan struct{})

	go probe(trk, brokers, pubs, 5*time.Millisecond, stop)

	local := make([]time.Duration, len(brokers))
	errs := make([]error, len(brokers))

	var wg sync.WaitGroup

	for i, b := range brokers {
		wg.Go(func() {
			errs[i] = b.subscribeDevices()
			local[i] = time.Since(trk.start)
		})
	}

	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		close(stop)

		return nil, err
	}

	select {
	case <-trk.all:
	case <-time.After(15 * time.Minute):
		close(stop)

		return nil, errors.New("propagation did not finish in 15 minutes")
	}

	close(stop)

	var prop, lag time.Duration

	trk.mu.Lock()
	for k, at := range trk.at {
		prop = max(prop, at)
		lag = max(lag, at-local[k[0]])
	}
	trk.mu.Unlock()

	m.add("filters_total", float64(total))
	m.add("subscribe.local_flush_max_s", slices.Max(local).Seconds())
	m.add("propagation.last_sub_reachable_s", prop.Seconds())
	m.add("propagation.lag_after_local_flush_s", lag.Seconds())

	vs, vg := verifySample(brokers, pubs, func(b *broker) []int { return randomIdxs(0, b.n, cfg.verifyPer) })
	m.add("propagation.verify_sent", float64(vs))
	m.add("propagation.verify_received", float64(vg))

	after := cl.stats(true)
	for i := range after {
		m.add(fmt.Sprintf("mem.%s.num_subs", after[i].Name), float64(after[i].NumSubs))
	}

	recordMem(m, "mem.idle", nil, before, 0)
	recordMem(m, "mem.loaded", before, after, total)

	targets := pickTargets(brokers, cfg.targetsPer)

	// 2a. Baseline latency on unaffected subjects.
	phaseCPU(m, "baseline", cl, func() int64 {
		return traffic(pubs, brokers, targets, cfg.rate, cfg.baseline, phaseBaseline)
	})
	time.Sleep(2 * time.Second)
	recordLatency(m, "baseline", brokers, phaseBaseline)

	churnPer := cfg.churn / cfg.brokers

	if cfg.window > 0 {
		if err := interleavedStorm(m, cfg, cl, pubs, brokers, targets, churnPer); err != nil {
			return nil, err
		}
	} else if err := continuousStorm(m, cfg, cl, pubs, brokers, targets, churnPer); err != nil {
		return nil, err
	}

	vs, vg = verifySample(brokers, pubs, func(b *broker) []int { return randomIdxs(0, churnPer, cfg.verifyPer) })
	m.add("storm.post_verify_churned_sent", float64(vs))
	m.add("storm.post_verify_churned_received", float64(vg))

	var match, slow int64
	for _, b := range brokers {
		match += b.match.Load()
		slow += b.slow.Load()
	}

	m.add("traffic.locally_matched", float64(match))
	m.add("slow_consumer_events", float64(slow))

	// 3. Overlapping filters on one node.
	if err := overlap(m, cl, cfg.overlapN); err != nil {
		return nil, err
	}

	m.note("load_end", loadavg())
	m.add("load1_end", load1())

	return m, nil
}

func runFallback(cfg config) (*metrics, error) {
	m := newMetrics()
	m.note("load_start", loadavg())
	m.add("load1_start", load1())

	cl, err := startCluster(cfg.servers)
	if err != nil {
		return nil, err
	}
	defer cl.stop()

	pubs, err := connectPubs(cl)
	if err != nil {
		return nil, err
	}
	defer closeAll(pubs)

	brokers, err := startBrokers(cfg, cl, false)
	if err != nil {
		return nil, err
	}
	defer stopBrokers(brokers)

	before := cl.stats(true)

	// One subscription per tenant per node: both TenantAll subjects, since a
	// tenant's "$" topics live under ==<tenant>.
	for _, b := range brokers {
		for _, s := range TenantAll(tenant) {
			if _, err := b.nc.ChanSubscribe("f."+s, b.ch); err != nil {
				return nil, err
			}
		}

		if err := b.nc.Flush(); err != nil {
			return nil, err
		}
	}

	trk := newTracker(brokers, cfg.servers)
	for _, b := range brokers {
		b.trk.Store(trk)
	}

	stop := make(chan struct{})

	go probe(trk, brokers, pubs, 5*time.Millisecond, stop)

	select {
	case <-trk.all:
	case <-time.After(2 * time.Minute):
		close(stop)

		return nil, errors.New("fallback propagation did not finish")
	}

	close(stop)

	after := cl.stats(true)
	recordMem(m, "mem.loaded", before, after, 0)

	// 4. The same traffic, every message fanned out to every broker connection
	// and matched locally.
	targets := pickTargets(brokers, cfg.targetsPer)
	sent := phaseCPU(m, "fallback", cl, func() int64 {
		return traffic(pubs, brokers, targets, cfg.rate, cfg.fallbackDur, phaseFallback)
	})

	time.Sleep(2 * time.Second)
	recordLatency(m, "fallback", brokers, phaseFallback)

	var match, miss, slow int64
	for _, b := range brokers {
		match += b.match.Load()
		miss += b.miss.Load()
		slow += b.slow.Load()
	}

	m.add("fallback.locally_matched", float64(match))
	m.add("fallback.locally_unmatched", float64(miss))
	m.add("fallback.deliveries_per_msg", float64(match+miss)/float64(sent))
	m.add("slow_consumer_events", float64(slow))

	m.note("load_end", loadavg())
	m.add("load1_end", load1())

	return m, nil
}

// overlap subscribes one connection to the subjects for MQTT a/+ and a/#,
// publishes from another server, and counts deliveries before and after dedup.
func overlap(m *metrics, cl *cluster, n int) error {
	sub, err := nats.Connect(cl.children[0].clientURL, nats.Name("overlap-sub"))
	if err != nil {
		return err
	}
	defer sub.Close()

	pub, err := nats.Connect(cl.children[1%len(cl.children)].clientURL, nats.Name("overlap-pub"))
	if err != nil {
		return err
	}
	defer pub.Close()

	ch := make(chan *nats.Msg, 1<<18)

	var subjects []string
	for _, f := range []string{"a/+", "a/#"} {
		for _, s := range EncodeFilter(tenant, f) {
			subjects = append(subjects, "f."+s)
		}
	}

	m.note("overlap.subjects", strings.Join(subjects, " "))

	for _, s := range subjects {
		if _, err := sub.ChanSubscribe(s, ch); err != nil {
			return err
		}
	}

	if err := sub.Flush(); err != nil {
		return err
	}

	probeSubj := "f." + EncodeTopic(tenant, "a/probe")

	for seen := false; !seen; {
		_ = pub.Publish(probeSubj, []byte("probe"))
		_ = pub.Flush()

		select {
		case <-ch:
			seen = true
		case <-time.After(20 * time.Millisecond):
		}
	}

	time.Sleep(200 * time.Millisecond)

	for len(ch) > 0 {
		<-ch
	}

	topics := []string{"a/b", "a", "a/b/c"}
	for ti, t := range topics {
		subj := "f." + EncodeTopic(tenant, t)
		for i := range n {
			msg := nats.NewMsg(subj)
			msg.Header.Set("Tm-Id", fmt.Sprintf("n1-%d-%d", ti, i))
			msg.Data = []byte(t)

			if err := pub.PublishMsg(msg); err != nil {
				return err
			}
		}
	}

	if err := pub.Flush(); err != nil {
		return err
	}

	d := newDedup(1<<16, 2*time.Second)
	raw := map[string]int{}
	kept := map[string]int{}

	var dedupNS int64

	for {
		select {
		case msg := <-ch:
			t := string(msg.Data)
			raw[t]++

			t0 := time.Now()
			dup := d.IsDuplicate(msg.Header.Get("Tm-Id"))
			dedupNS += int64(time.Since(t0))

			if !dup {
				kept[t]++
			}

			continue
		case <-time.After(2 * time.Second):
		}

		break
	}

	var deliveries int
	for _, t := range topics {
		m.add("overlap."+t+".published", float64(n))
		m.add("overlap."+t+".raw_deliveries", float64(raw[t]))
		m.add("overlap."+t+".after_dedup", float64(kept[t]))
		deliveries += raw[t]
	}

	m.add("overlap.dedup_ns_per_delivery", float64(dedupNS)/float64(max(deliveries, 1)))

	return nil
}

// continuousStorm is measurement 2 as the plan states it: the same traffic
// during one 100k-device storm, compared with the baseline phase before it.
func continuousStorm(m *metrics, cfg config, cl *cluster, pubs []*nats.Conn, brokers []*broker, targets []target, churnPer int) error {
	var ops [64]int64

	var stormErrs [64]error

	stormDone := make([]time.Duration, len(brokers))

	phaseCPU(m, "storm", cl, func() int64 {
		var sw sync.WaitGroup

		t0 := time.Now()

		for i, b := range brokers {
			sw.Go(func() {
				ops[i], stormErrs[i] = storm(b, churnPer, cfg.stormDur, cfg.gap)
				stormDone[i] = time.Since(t0)
			})
		}

		sent := traffic(pubs, brokers, targets, cfg.rate, cfg.stormDur, phaseStorm)

		sw.Wait()

		return sent
	})

	if err := errors.Join(stormErrs[:]...); err != nil {
		return err
	}

	var totalOps int64
	for _, o := range ops {
		totalOps += o
	}

	m.add("storm.sub_unsub_ops", float64(totalOps))
	m.add("storm.duration_max_s", slices.Max(stormDone).Seconds())
	time.Sleep(2 * time.Second)
	recordLatency(m, "storm", brokers, phaseStorm)

	b := mergeHist(brokers, phaseBaseline)
	s := mergeHist(brokers, phaseStorm)
	m.add("storm.p99_over_baseline_p99", float64(s.ValueAtQuantile(99))/float64(b.ValueAtQuantile(99)))
	m.add("storm.p50_over_baseline_p50", float64(s.ValueAtQuantile(50))/float64(b.ValueAtQuantile(50)))

	return nil
}

// interleavedStorm runs the traffic for twice -storm in alternating windows,
// off first, with the storm active only in on windows. External load on the
// shared machine then hits both arms about equally, which a before and after
// comparison can't promise. The first second of each window is not reported.
func interleavedStorm(m *metrics, cfg config, cl *cluster, pubs []*nats.Conn, brokers []*broker, targets []target, churnPer int) error {
	x := window{cfg.window}
	total := 2 * cfg.stormDur

	var ops [64]int64

	var errs [64]error

	phaseCPU(m, "interleaved", cl, func() int64 {
		var sw sync.WaitGroup

		for i, b := range brokers {
			sw.Go(func() {
				ops[i], errs[i] = stormWindowed(b, churnPer, cfg.stormDur, cfg.gap, x, total)
			})
		}

		sent := trafficFn(pubs, brokers, targets, cfg.rate, total, x.phaseAt)

		sw.Wait()

		return sent
	})

	if err := errors.Join(errs[:]...); err != nil {
		return err
	}

	var totalOps int64
	for _, o := range ops {
		totalOps += o
	}

	m.add("interleaved.sub_unsub_ops", float64(totalOps))
	time.Sleep(2 * time.Second)
	recordLatency(m, "interleaved.off", brokers, phaseOff)
	recordLatency(m, "interleaved.on", brokers, phaseOn)

	off := mergeHist(brokers, phaseOff)
	on := mergeHist(brokers, phaseOn)
	base := mergeHist(brokers, phaseBaseline)
	m.add("interleaved.on_p99_over_off_p99", float64(on.ValueAtQuantile(99))/float64(off.ValueAtQuantile(99)))
	m.add("interleaved.on_p50_over_off_p50", float64(on.ValueAtQuantile(50))/float64(off.ValueAtQuantile(50)))
	m.add("interleaved.on_p99_over_baseline_p99", float64(on.ValueAtQuantile(99))/float64(base.ValueAtQuantile(99)))

	return nil
}
