package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var epoch = time.Now()

func nowNS() int64 { return int64(time.Since(epoch)) }

// cell is one configuration measured in interleaved windows.
type cell struct {
	name   string
	target float64
	rec    *rec
	run    func(d time.Duration) // sends at the target rate for d, then drains
}

// runCells visits every cell once per round for one window each, rotating the
// start so that no cell always follows the same neighbour or always runs at
// the same point in a run. Outside load drifts over minutes on this machine,
// and interleaving spreads that drift over every cell instead of over the
// cells that happened to run late.
func runCells(cells []*cell, rounds int, window time.Duration, run int) []row {
	for round := range rounds {
		off := (run*7 + round*5) % len(cells)

		for i := range cells {
			c := cells[(off+i)%len(cells)]
			c.rec.load = append(c.rec.load, load1())

			start := time.Now()
			c.run(window)
			c.rec.busy += window

			fmt.Printf("  round %d %-26s %6.1fs load %.1f\n", round+1, c.name, time.Since(start).Seconds(), load1())
		}
	}

	rows := make([]row, len(cells))
	for i, c := range cells {
		rows[i] = c.rec.row(c.name, c.target)
	}

	return rows
}

// pace calls send rate times per second for d, catching up after a late wakeup
// so the average rate holds even when the scheduler is slow.
func pace(rate float64, d time.Duration, send func()) {
	start := time.Now()

	var sent int64

	for {
		el := time.Since(start)
		if el >= d {
			return
		}

		due := int64(el.Seconds() * rate)
		for ; sent < due; sent++ {
			send()
		}

		time.Sleep(500 * time.Microsecond)
	}
}

func parseInts(s string) []int {
	var out []int

	for _, f := range strings.Split(s, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(f))
		if err == nil {
			out = append(out, v)
		}
	}

	return out
}

// pubConn is one publishing connection with an async ack handler that routes
// each ack to its cell's recorder by a header.
type pubConn struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	recs *sync.Map // cell index -> *rec
}

const hdrT = "Tm"

func newPubConn(url string, recs *sync.Map, onAck func(m *nats.Msg, a *jetstream.PubAck)) (*pubConn, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	pc := &pubConn{nc: nc, recs: recs}

	ack := func(_ jetstream.JetStream, m *nats.Msg, a *jetstream.PubAck) {
		idx, sent, ok := parseT(m.Header.Get(hdrT))
		if !ok {
			return
		}

		if r, ok := recs.Load(idx); ok {
			r.(*rec).add(time.Duration(nowNS() - sent))
		}

		if onAck != nil {
			onAck(m, a)
		}
	}

	errh := func(_ jetstream.JetStream, m *nats.Msg, err error) {
		idx, _, ok := parseT(m.Header.Get(hdrT))
		if !ok {
			return
		}

		if r, ok := recs.Load(idx); ok {
			r.(*rec).err(errName(err))
		}

		if onAck != nil {
			onAck(m, nil)
		}
	}

	pc.js, err = jetstream.New(nc,
		jetstream.WithPublishAsyncMaxPending(2048),
		jetstream.WithPublishAsyncAckHandler(ack),
		jetstream.WithPublishAsyncErrHandler(errh),
		jetstream.WithPublishAsyncTimeout(30*time.Second),
	)
	if err != nil {
		nc.Close()

		return nil, err
	}

	return pc, nil
}

func parseT(v string) (int, int64, bool) {
	f := strings.SplitN(v, ":", 3)
	if len(f) < 2 {
		return 0, 0, false
	}

	idx, err1 := strconv.Atoi(f[0])
	ns, err2 := strconv.ParseInt(f[1], 10, 64)

	return idx, ns, err1 == nil && err2 == nil
}

func errName(err error) string {
	var api *jetstream.APIError
	if errors.As(err, &api) {
		return fmt.Sprintf("api-%d", api.ErrorCode)
	}

	s := err.Error()
	if len(s) > 48 {
		s = s[:48]
	}

	return s
}

// drain waits until every connection's async publishes are acked or failed.
func drain(conns []*pubConn, max time.Duration) {
	for _, c := range conns {
		select {
		case <-c.js.PublishAsyncComplete():
		case <-time.After(max):
			fmt.Printf("  drain: %d still pending after %v\n", c.js.PublishAsyncPending(), max)
		}
	}
}

// runPublish measures publish-ack latency on an R3 file stream for each
// payload size and rate, with sync publishes (one goroutine waits for each ack,
// like a blocking OnPublish) and async publishes (PUBACK written when the ack
// comes back, the path spike 1 recommends). Traffic is split over one
// connection per server, so a third of it reaches the stream leader directly
// and two thirds go through a follower first.
func runPublish(cfg config, cl *Cluster, run int) ([]row, error) {
	recs := &sync.Map{}

	var conns []*pubConn

	for _, u := range cl.URLs() {
		pc, err := newPubConn(u, recs, nil)
		if err != nil {
			return nil, err
		}

		defer pc.nc.Close()

		conns = append(conns, pc)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s, err := conns[0].js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "LOG", Subjects: []string{"d.>"}, Replicas: 3, Storage: jetstream.FileStorage,
		MaxBytes: 1 << 30, Discard: jetstream.DiscardOld, MaxAge: time.Hour,
	})
	if err != nil {
		return nil, err
	}

	defer func() {
		c, cc := context.WithTimeout(context.Background(), 60*time.Second)
		defer cc()

		_ = conns[0].js.DeleteStream(c, "LOG")
	}()

	info, _ := s.Info(ctx)
	fmt.Printf("  stream LOG leader %s, replicas %v\n", info.Cluster.Leader, replicaNames(info))

	var cells []*cell

	for _, size := range parseInts(cfg.sizes) {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte('a' + i%26)
		}

		for _, rate := range parseInts(cfg.rates) {
			for _, mode := range []string{"async", "sync"} {
				idx := len(cells)
				c := &cell{name: fmt.Sprintf("%s %s %d/s", mode, sizeName(size), rate), target: float64(rate), rec: newRec()}
				recs.Store(idx, c.rec)

				if mode == "async" {
					c.run = asyncPublisher(conns, idx, payload, float64(rate))
				} else {
					c.run = syncPublisher(conns, idx, payload, float64(rate), cfg.workers, c.rec)
				}

				cells = append(cells, c)
			}
		}
	}

	return runCells(cells, cfg.rounds, cfg.window, run), nil
}

func replicaNames(info *jetstream.StreamInfo) []string {
	if info == nil || info.Cluster == nil {
		return nil
	}

	var out []string
	for _, r := range info.Cluster.Replicas {
		out = append(out, fmt.Sprintf("%s(current=%v)", r.Name, r.Current))
	}

	return out
}

func sizeName(n int) string {
	if n >= 1024 && n%1024 == 0 {
		return fmt.Sprintf("%dKiB", n/1024)
	}

	return fmt.Sprintf("%dB", n)
}

func subjectFor(i int64) string {
	return "d.ttnt_x.devices." + strconv.FormatInt(i%1000, 10) + ".telemetry"
}

func asyncPublisher(conns []*pubConn, idx int, payload []byte, rate float64) func(time.Duration) {
	var seq atomic.Int64

	return func(d time.Duration) {
		var wg sync.WaitGroup

		for _, c := range conns {
			wg.Go(func() {
				pace(rate/float64(len(conns)), d, func() {
					m := &nats.Msg{Subject: subjectFor(seq.Add(1)), Data: payload, Header: nats.Header{}}
					m.Header.Set(hdrT, fmt.Sprintf("%d:%d", idx, nowNS()))

					if _, err := c.js.PublishMsgAsync(m); err != nil {
						if r, ok := c.recs.Load(idx); ok {
							r.(*rec).err(errName(err))
						}
					}
				})
			})
		}

		wg.Wait()
		drain(conns, 30*time.Second)
	}
}

func syncPublisher(conns []*pubConn, idx int, payload []byte, rate float64, workers int, r *rec) func(time.Duration) {
	var seq atomic.Int64

	return func(d time.Duration) {
		var wg sync.WaitGroup

		for _, c := range conns {
			tokens := make(chan struct{}, workers)

			var ww sync.WaitGroup

			for range workers {
				ww.Go(func() {
					for range tokens {
						m := &nats.Msg{Subject: subjectFor(seq.Add(1)), Data: payload}

						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						t0 := time.Now()
						_, err := c.js.PublishMsg(ctx, m)

						cancel()

						if err != nil {
							r.err(errName(err))

							continue
						}

						r.add(time.Since(t0))
					}
				})
			}

			wg.Go(func() {
				pace(rate/float64(len(conns)), d, func() {
					select {
					case tokens <- struct{}{}:
					default:
						r.err("no free worker")
					}
				})
				close(tokens)
				ww.Wait()
			})
		}

		wg.Wait()
	}
}
