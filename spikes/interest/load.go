package main

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/nats-io/nats.go"
)

const tenant = "ttnt_x"

// Payload kinds. Traffic payload: kind, phase, 8 byte send time (ns since
// epoch, monotonic, same process), 8 byte sequence number (the node-local ID).
const (
	kindProbe   = 'P'
	kindVerify  = 'V'
	kindTraffic = 'T'
)

const (
	phaseBaseline = iota
	phaseStorm
	phaseFallback
	phaseOff   // interleaved mode: a window with no storm
	phaseOn    // interleaved mode: a window with the storm running
	phaseGuard // interleaved mode: the first second after a switch, not reported
	numPhases
)

var epoch = time.Now()

func nowNS() int64 { return int64(time.Since(epoch)) }

func deviceTopic(n int) string { return fmt.Sprintf("devices/%d/cmd", n) }

// deviceSubject is the QoS 0 subject for a device's command topic, built with
// spike 2's encoder: f.ttnt_x.devices.<n>.cmd.
func deviceSubject(n int) string { return "f." + EncodeTopic(tenant, deviceTopic(n)) }

type broker struct {
	idx    int
	server int
	nc     *nats.Conn
	ch     chan *nats.Msg
	first  int // global device number of local device 0
	n      int
	subs   []*nats.Subscription
	index  *mqtt.TopicsIndex
	dedup  *dedup // interest mode only: overlapping filters can deliver twice
	last   string // subject of the last device subscription
	trk    atomic.Pointer[tracker]
	mu     sync.Mutex
	hist   [numPhases]*hdrhistogram.Histogram
	recv   [numPhases]atomic.Int64
	match  atomic.Int64 // traffic messages with a local subscriber
	miss   atomic.Int64 // traffic messages with none (fallback fan-out)
	dups   atomic.Int64
	verify atomic.Int64
	slow   atomic.Int64
	done   chan struct{}
	wg     sync.WaitGroup
}

func newBroker(idx, server, first, n int, url string, interest bool) (*broker, error) {
	b := &broker{
		idx: idx, server: server, first: first, n: n,
		ch:   make(chan *nats.Msg, 1<<16),
		done: make(chan struct{}),
		last: deviceSubject(first + n - 1),
	}

	for i := range b.hist {
		b.hist[i] = hdrhistogram.New(1, 120_000_000, 3) // microseconds, up to 120 s
	}

	if interest {
		b.dedup = newDedup(1<<16, 2*time.Second)
	}

	nc, err := nats.Connect(url,
		nats.Name(fmt.Sprintf("broker-%d", idx)),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if err == nats.ErrSlowConsumer {
				b.slow.Add(1)
			}
		}),
	)
	if err != nil {
		return nil, err
	}

	b.nc = nc
	b.wg.Add(1)

	go b.read()

	return b, nil
}

// buildIndex loads this broker's device filters into a mochi topic index, the
// structure Tether's local delivery matches against in both modes.
func (b *broker) buildIndex() {
	b.index = mqtt.NewTopicsIndex()
	for i := range b.n {
		d := b.first + i
		b.index.Subscribe(fmt.Sprintf("d%d", d), packets.Subscription{Filter: deviceTopic(d)})
	}
}

func (b *broker) read() {
	defer b.wg.Done()

	for {
		select {
		case <-b.done:
			return
		case m := <-b.ch:
			b.handle(m)
		}
	}
}

func (b *broker) handle(m *nats.Msg) {
	if len(m.Data) == 0 {
		return
	}

	switch m.Data[0] {
	case kindProbe:
		if t := b.trk.Load(); t != nil && m.Subject == b.last && len(m.Data) > 1 {
			t.mark(b.idx, int(m.Data[1]))
		}
	case kindVerify:
		b.verify.Add(1)
	case kindTraffic:
		if len(m.Data) < 18 {
			return
		}

		lat := nowNS() - int64(binary.BigEndian.Uint64(m.Data[2:10]))
		phase := int(m.Data[1])

		b.mu.Lock()
		_ = b.hist[phase].RecordValue(max(lat/1e3, 1))
		b.mu.Unlock()
		b.recv[phase].Add(1)

		if b.dedup != nil && b.dedup.IsDuplicate(string(m.Data[10:18])) {
			b.dups.Add(1)

			return
		}

		// Local matching, the work a Tether node does for every message NATS
		// hands it: decode the subject and ask mochi's index for subscribers.
		_, topic, err := DecodeTopic(strings.TrimPrefix(m.Subject, "f."))
		if err != nil {
			return
		}

		if len(b.index.Subscribers(topic).Subscriptions) > 0 {
			b.match.Add(1)
		} else {
			b.miss.Add(1)
		}
	}
}

func (b *broker) subscribeDevices() error {
	b.subs = make([]*nats.Subscription, b.n)

	for i := range b.n {
		s, err := b.nc.ChanSubscribe(deviceSubject(b.first+i), b.ch)
		if err != nil {
			return err
		}

		b.subs[i] = s
	}

	return b.nc.FlushTimeout(10 * time.Minute)
}

func (b *broker) close() {
	close(b.done)
	b.wg.Wait()
	b.nc.Close()
}

// tracker records when each broker's last subscription first received a probe
// from each other server.
type tracker struct {
	mu      sync.Mutex
	start   time.Time
	at      map[[2]int]time.Duration
	pending map[[2]int]bool
	all     chan struct{}
}

func newTracker(brokers []*broker, servers int) *tracker {
	t := &tracker{start: time.Now(), at: map[[2]int]time.Duration{}, pending: map[[2]int]bool{}, all: make(chan struct{})}

	for _, b := range brokers {
		for s := range servers {
			if s != b.server {
				t.pending[[2]int{b.idx, s}] = true
			}
		}
	}

	return t
}

func (t *tracker) mark(b, from int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := [2]int{b, from}
	if !t.pending[k] {
		return
	}

	delete(t.pending, k)
	t.at[k] = time.Since(t.start)

	if len(t.pending) == 0 {
		close(t.all)
	}
}

func (t *tracker) waiting() [][2]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([][2]int, 0, len(t.pending))
	for k := range t.pending {
		out = append(out, k)
	}

	return out
}

// probe publishes a probe to every pending broker's last subject from the
// pending server every interval until the tracker is complete.
func probe(t *tracker, brokers []*broker, pubs []*nats.Conn, every time.Duration, stop <-chan struct{}) {
	tick := time.NewTicker(every)
	defer tick.Stop()

	for {
		select {
		case <-t.all:
			return
		case <-stop:
			return
		case <-tick.C:
			for _, k := range t.waiting() {
				_ = pubs[k[1]].Publish(brokers[k[0]].last, []byte{kindProbe, byte(k[1])})
			}

			for _, p := range pubs {
				_ = p.Flush()
			}
		}
	}
}

// verifySample publishes one message per sampled device from a server other
// than its broker's and returns how many arrived.
func verifySample(brokers []*broker, pubs []*nats.Conn, idxs func(b *broker) []int) (sent, got int64) {
	for _, b := range brokers {
		b.verify.Store(0)
	}

	for _, b := range brokers {
		from := (b.server + 1) % len(pubs)
		for _, i := range idxs(b) {
			_ = pubs[from].Publish(deviceSubject(b.first+i), []byte{kindVerify})
			sent++
		}
	}

	for _, p := range pubs {
		_ = p.FlushTimeout(time.Minute)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		got = 0
		for _, b := range brokers {
			got += b.verify.Load()
		}

		if got >= sent {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return sent, got
}

type target struct {
	subject string
	broker  int
}

// traffic publishes rate msgs/s round robin over targets for dur, always from a
// server other than the target broker's, so every message crosses a route.
func traffic(pubs []*nats.Conn, brokers []*broker, targets []target, rate int, dur time.Duration, phase int) int64 {
	return trafficFn(pubs, brokers, targets, rate, dur, func(time.Duration) int { return phase })
}

// trafficFn is traffic with the phase chosen per message from the elapsed time.
func trafficFn(pubs []*nats.Conn, brokers []*broker, targets []target, rate int, dur time.Duration, phaseAt func(time.Duration) int) int64 {
	buf := make([]byte, 18)
	buf[0] = kindTraffic

	var sent int64

	start := time.Now()

	for {
		el := time.Since(start)
		if el >= dur {
			break
		}

		want := int64(el.Seconds() * float64(rate))
		phase := phaseAt(el)
		buf[1] = byte(phase)

		for sent < want {
			t := targets[sent%int64(len(targets))]
			hop := 1 + int(sent/int64(len(targets)))%(len(pubs)-1)
			from := (brokers[t.broker].server + hop) % len(pubs)

			binary.BigEndian.PutUint64(buf[2:10], uint64(nowNS()))
			binary.BigEndian.PutUint64(buf[10:18], uint64(sent)|uint64(phase)<<56)
			_ = pubs[from].Publish(t.subject, buf)
			sent++
		}

		time.Sleep(200 * time.Microsecond)
	}

	for _, p := range pubs {
		_ = p.Flush()
	}

	return sent
}

// storm unsubscribes the broker's first churn devices evenly over dur and
// resubscribes each one gap later, the shape of a reconnect storm.
func storm(b *broker, churn int, dur, gap time.Duration) (ops int64, err error) {
	type pend struct {
		i  int
		at time.Time
	}

	var q []pend

	next := 0
	start := time.Now()
	tick := time.NewTicker(10 * time.Millisecond)

	defer tick.Stop()

	resub := func(i int) error {
		s, err := b.nc.ChanSubscribe(deviceSubject(b.first+i), b.ch)
		if err != nil {
			return err
		}

		b.subs[i] = s
		ops++

		return nil
	}

	for range tick.C {
		el := time.Since(start)
		want := min(churn, int(float64(churn)*el.Seconds()/dur.Seconds()))

		for ; next < want; next++ {
			if err := b.subs[next].Unsubscribe(); err != nil {
				return ops, err
			}

			ops++
			q = append(q, pend{next, time.Now()})
		}

		for len(q) > 0 && time.Since(q[0].at) >= gap {
			if err := resub(q[0].i); err != nil {
				return ops, err
			}

			q = q[1:]
		}

		if el >= dur && next >= churn {
			break
		}
	}

	for _, p := range q {
		if err := resub(p.i); err != nil {
			return ops, err
		}
	}

	return ops, b.nc.FlushTimeout(time.Minute)
}

// window describes the interleaved storm: windows of length w alternate, off
// first. activeAt is how much on-time has passed by el.
type window struct{ w time.Duration }

func (x window) on(el time.Duration) bool { return (el/x.w)%2 == 1 }

func (x window) activeAt(el time.Duration) time.Duration {
	cycles := el / (2 * x.w)
	in := el % (2 * x.w)

	return cycles*x.w + max(0, in-x.w)
}

func (x window) phaseAt(el time.Duration) int {
	if el%x.w < time.Second {
		return phaseGuard
	}

	if x.on(el) {
		return phaseOn
	}

	return phaseOff
}

// stormWindowed is storm, but it unsubscribes and resubscribes only inside on
// windows, spreading churn devices over activeDur of on-time.
func stormWindowed(b *broker, churn int, activeDur, gap time.Duration, x window, total time.Duration) (ops int64, err error) {
	type pend struct {
		i  int
		at time.Time
	}

	var q []pend

	next := 0
	start := time.Now()
	tick := time.NewTicker(10 * time.Millisecond)

	defer tick.Stop()

	resub := func(i int) error {
		s, err := b.nc.ChanSubscribe(deviceSubject(b.first+i), b.ch)
		if err != nil {
			return err
		}

		b.subs[i] = s
		ops++

		return nil
	}

	for range tick.C {
		el := time.Since(start)
		if el >= total {
			break
		}

		if !x.on(el) {
			continue
		}

		want := min(churn, int(float64(churn)*x.activeAt(el).Seconds()/activeDur.Seconds()))

		for ; next < want; next++ {
			if err := b.subs[next].Unsubscribe(); err != nil {
				return ops, err
			}

			ops++
			q = append(q, pend{next, time.Now()})
		}

		for len(q) > 0 && time.Since(q[0].at) >= gap {
			if err := resub(q[0].i); err != nil {
				return ops, err
			}

			q = q[1:]
		}
	}

	// Rounding can leave a few devices of the last on window; finish them.
	for ; next < churn; next++ {
		if err := b.subs[next].Unsubscribe(); err != nil {
			return ops, err
		}

		ops++
		q = append(q, pend{next, time.Now()})
	}

	for _, p := range q {
		if err := resub(p.i); err != nil {
			return ops, err
		}
	}

	return ops, b.nc.FlushTimeout(time.Minute)
}

func mergeHist(brokers []*broker, phase int) *hdrhistogram.Histogram {
	h := hdrhistogram.New(1, 120_000_000, 3)

	for _, b := range brokers {
		b.mu.Lock()
		h.Merge(b.hist[phase])
		b.mu.Unlock()
	}

	return h
}

func pickTargets(brokers []*broker, perBroker int) []target {
	var out []target

	for i := range perBroker {
		for _, b := range brokers {
			// The tail of each broker's range: never churned by the storm.
			out = append(out, target{deviceSubject(b.first + b.n - 1 - i), b.idx})
		}
	}

	return out
}

func randomIdxs(lo, hi, k int) []int {
	out := make([]int, k)
	for i := range out {
		out[i] = lo + rand.IntN(hi-lo)
	}

	return out
}
