package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// casState tracks every session key's current revision. Keys are partitioned
// by connection (key k belongs to connection k%3), like sessions owned by
// nodes, so no two writers race on a key and every failure is unexpected.
type casState struct {
	keys    int
	rev     []atomic.Uint64
	pending []atomic.Bool
	value   []byte
}

func casKey(k int) string { return fmt.Sprintf("s.ttnt_x.dev-%d", k) }

// casMsg is the cluster layer's session write: a raw publish to the KV stream
// with the session expiry as Nats-TTL and the expected last subject sequence,
// so it stays a compare-and-set.
func (s *casState) casMsg(k int) *nats.Msg {
	m := &nats.Msg{Subject: "$KV.SESS." + casKey(k), Data: s.value, Header: nats.Header{}}
	m.Header.Set(jetstream.MsgTTLHeader, "168h")
	m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(s.rev[k].Load(), 10))

	return m
}

// runCAS measures session-key CAS on one R3 bucket with limit markers, at the
// reconnect storm's 1.7k/s, at 5k/s and at 10k/s. "sync" waits for each ack
// before the next write on that worker (a CONNECT takeover); "async" pipelines
// writes per connection and updates the key's revision from the ack (batched
// cursor writes), never with two writes on one key in flight.
func runCAS(cfg config, cl *Cluster, run int) ([]row, error) {
	st := &casState{keys: cfg.casKeys, rev: make([]atomic.Uint64, cfg.casKeys), pending: make([]atomic.Bool, cfg.casKeys), value: make([]byte, 512)}
	for i := range st.value {
		st.value[i] = byte('a' + i%26)
	}

	recs := &sync.Map{}
	dirty := make(chan int, 1<<16)

	onAck := func(m *nats.Msg, a *jetstream.PubAck) {
		f := strings.SplitN(m.Header.Get(hdrT), ":", 3)
		if len(f) != 3 {
			return
		}

		k, _ := strconv.Atoi(f[2])

		if a != nil {
			st.rev[k].Store(a.Sequence)
		} else {
			select {
			case dirty <- k:
			default:
			}
		}

		st.pending[k].Store(false)
	}

	var conns []*pubConn

	for _, u := range cl.URLs() {
		pc, err := newPubConn(u, recs, onAck)
		if err != nil {
			return nil, err
		}

		defer pc.nc.Close()

		conns = append(conns, pc)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	kv, err := conns[0].js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "SESS", History: 1, Replicas: 3, Storage: jetstream.FileStorage, LimitMarkerTTL: time.Hour,
	})
	if err != nil {
		return nil, err
	}

	defer func() {
		c, cc := context.WithTimeout(context.Background(), 60*time.Second)
		defer cc()

		_ = conns[0].js.DeleteKeyValue(c, "SESS")
	}()

	// Create every key (expected sequence 0), pipelined per connection.
	t0 := time.Now()

	var wg sync.WaitGroup

	for ci, c := range conns {
		wg.Go(func() {
			for k := ci; k < st.keys; k += len(conns) {
				m := st.casMsg(k)
				m.Header.Set(hdrT, fmt.Sprintf("-1:0:%d", k))
				st.pending[k].Store(true)

				for {
					if _, err := c.js.PublishMsgAsync(m); err == nil {
						break
					}

					time.Sleep(time.Millisecond)
				}
			}
		})
	}

	wg.Wait()
	drain(conns, 60*time.Second)

	missing := 0

	for k := range st.keys {
		if st.rev[k].Load() == 0 {
			missing++
		}
	}

	if missing > 0 {
		return nil, fmt.Errorf("%d keys not created", missing)
	}

	s, _ := kv.Status(ctx)
	fmt.Printf("  created %d session keys in %v; bucket %s, %d values\n", st.keys, time.Since(t0).Round(time.Millisecond), s.Bucket(), s.Values())

	if js, ok := s.(*jetstream.KeyValueBucketStatus); ok {
		si := js.StreamInfo()
		fmt.Printf("  KV_SESS leader %s, replicas %v\n", si.Cluster.Leader, replicaNames(si))
	}

	// Refresh any key whose write failed, from the stream leader.
	stop := make(chan struct{})
	defer close(stop)

	go func() {
		stream, err := conns[0].js.Stream(context.Background(), "KV_SESS")
		if err != nil {
			return
		}

		for {
			var k int

			select {
			case k = <-dirty:
			case <-stop:
				return
			}

			c, cc := context.WithTimeout(context.Background(), 10*time.Second)
			m, err := stream.GetLastMsgForSubject(c, "$KV.SESS."+casKey(k))

			cc()

			if err == nil {
				st.rev[k].Store(m.Sequence)
			}
		}
	}()

	var cells []*cell

	for _, rate := range parseInts(cfg.casRates) {
		for _, mode := range []string{"sync", "async"} {
			idx := len(cells)
			c := &cell{name: fmt.Sprintf("cas %s %d/s", mode, rate), target: float64(rate), rec: newRec()}
			recs.Store(idx, c.rec)

			if mode == "async" {
				c.run = asyncCAS(conns, st, idx, float64(rate), c.rec)
			} else {
				c.run = syncCAS(conns, st, float64(rate), cfg.workers, c.rec, dirty)
			}

			cells = append(cells, c)
		}
	}

	return runCells(cells, cfg.rounds, cfg.window, run), nil
}

// nextKey hands out connection ci's keys round robin.
type keyCursor struct {
	n atomic.Int64
}

func (kc *keyCursor) next(ci, conns, keys int) int {
	per := (keys - ci + conns - 1) / conns
	i := int(kc.n.Add(1)) % per

	return ci + i*conns
}

func asyncCAS(conns []*pubConn, st *casState, idx int, rate float64, r *rec) func(time.Duration) {
	curs := make([]keyCursor, len(conns))

	return func(d time.Duration) {
		var wg sync.WaitGroup

		for ci, c := range conns {
			wg.Go(func() {
				pace(rate/float64(len(conns)), d, func() {
					k := curs[ci].next(ci, len(conns), st.keys)
					if !st.pending[k].CompareAndSwap(false, true) {
						r.err("key still pending")

						return
					}

					m := st.casMsg(k)
					m.Header.Set(hdrT, fmt.Sprintf("%d:%d:%d", idx, nowNS(), k))

					if _, err := c.js.PublishMsgAsync(m); err != nil {
						st.pending[k].Store(false)
						r.err(errName(err))
					}
				})
			})
		}

		wg.Wait()
		drain(conns, 30*time.Second)
	}
}

func syncCAS(conns []*pubConn, st *casState, rate float64, workers int, r *rec, dirty chan<- int) func(time.Duration) {
	curs := make([]keyCursor, len(conns))

	return func(d time.Duration) {
		var wg sync.WaitGroup

		for ci, c := range conns {
			tokens := make(chan struct{}, workers)

			var ww sync.WaitGroup

			for range workers {
				ww.Go(func() {
					for range tokens {
						k := curs[ci].next(ci, len(conns), st.keys)
						if !st.pending[k].CompareAndSwap(false, true) {
							r.err("key still pending")

							continue
						}

						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						t0 := time.Now()
						a, err := c.js.PublishMsg(ctx, st.casMsg(k))

						cancel()

						if err != nil {
							r.err(errName(err))

							select {
							case dirty <- k:
							default:
							}
						} else {
							r.add(time.Since(t0))
							st.rev[k].Store(a.Sequence)
						}

						st.pending[k].Store(false)
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
