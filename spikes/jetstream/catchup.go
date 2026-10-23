package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Catch-up rows reuse row: Target is the message count, Achieved msgs/s, P50
// the total seconds, P90 the milliseconds to the first message.
func printCatch(title string, rows []row) {
	fmt.Printf("\n%s\n", title)
	fmt.Printf("  %-34s %10s %10s %10s %12s %7s\n", "variant", "messages", "msgs/s", "seconds", "first msg ms", "load")

	for _, r := range rows {
		fmt.Printf("  %-34s %10.0f %10.0f %10.2f %12.1f %7.1f\n", r.Cell, r.Target, r.Achieved, r.P50, r.P90, r.Load)
	}
}

func tenantName(t int) string { return fmt.Sprintf("ttnt_%d", t) }

// sysLevel is spike 2's encoding of the MQTT level "$SYS".
var sysLevel = "=" + base64.RawURLEncoding.EncodeToString([]byte("$SYS"))

// logSubject is message j of a tenant: 99 in 100 are ordinary telemetry, one
// in 100 is a `$` topic, which spike 2 puts under the `==<tenant>` token.
func logSubject(t, j int) string {
	if j%100 == 99 {
		return fmt.Sprintf("d.==%s.%s.broker.x%d", tenantName(t), sysLevel, j%10)
	}

	return fmt.Sprintf("d.%s.devices.%d.telemetry", tenantName(t), j%1000)
}

func tenantAll(t int) []string {
	return []string{"d." + tenantName(t) + ".>", "d.==" + tenantName(t) + ".>"}
}

// runCatchup fills one R3 file stream with tenants x perTenant messages,
// interleaved round robin across tenants, then times ordered consumers reading
// one tenant's share from the start: how fast a node replays a tenant's log
// for sessions that were offline or lost their node.
func runCatchup(cfg config, cl *Cluster, run int) ([]row, error) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	stream, err := conns[0].js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "TLOG", Subjects: []string{"d.>"}, Replicas: 3, Storage: jetstream.FileStorage, MaxAge: 24 * time.Hour,
	})
	if err != nil {
		return nil, err
	}

	defer func() {
		c, cc := context.WithTimeout(context.Background(), 120*time.Second)
		defer cc()

		_ = conns[0].js.DeleteStream(c, "TLOG")
	}()

	total := cfg.tenants * cfg.perTenant
	payload := make([]byte, 128)

	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	fmt.Printf("  filling %d messages (%d tenants x %d, 128 B) ...\n", total, cfg.tenants, cfg.perTenant)

	fillLoad := load1()
	t0 := time.Now()

	var (
		wg     sync.WaitGroup
		stalls atomic.Int64
	)

	for ci, c := range conns {
		wg.Go(func() {
			for i := ci; i < total; i += len(conns) {
				m := &nats.Msg{Subject: logSubject(i%cfg.tenants, i/cfg.tenants), Data: payload}

				for {
					if _, err := c.js.PublishMsgAsync(m); err == nil {
						break
					}

					stalls.Add(1)
					time.Sleep(time.Millisecond)
				}
			}
		})
	}

	wg.Wait()
	drain(conns, 5*time.Minute)

	fill := time.Since(t0)

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Printf("  filled %d messages in %v (%.0f msgs/s, %d stalled retries, load %.1f -> %.1f); %d MiB per replica; leader %s, replicas %v\n",
		info.State.Msgs, fill.Round(time.Millisecond), float64(info.State.Msgs)/fill.Seconds(), stalls.Load(), fillLoad, load1(),
		info.State.Bytes>>20, info.Cluster.Leader, replicaNames(info))

	if info.State.Msgs != uint64(total) {
		return nil, fmt.Errorf("stream holds %d messages, want %d (async publish errors)", info.State.Msgs, total)
	}

	// The exact count each filter must reach, from the stream itself.
	count := func(filter string) (int, error) {
		si, err := stream.Info(ctx, jetstream.WithSubjectFilter(filter))
		if err != nil {
			return 0, err
		}

		n := 0
		for _, v := range si.State.Subjects {
			n += int(v)
		}

		return n, nil
	}

	want := map[string]int{}

	for t := range cfg.tenants {
		for _, f := range tenantAll(t) {
			n, err := count(f)
			if err != nil {
				return nil, err
			}

			want[f] = n
		}
	}

	wantAll := func(t int) int { return want[tenantAll(t)[0]] + want[tenantAll(t)[1]] }

	fmt.Printf("  tenant 0: %d ordinary + %d `$` = %d\n", want[tenantAll(0)[0]], want[tenantAll(0)[1]], wantAll(0))

	type variant struct {
		name string
		run  func() (msgs int, first, total time.Duration, err error)
	}

	one := []string{tenantAll(0)[0]}
	oneWant := want[one[0]]

	const capped = 20_000 // multi-filter reads are slow; time only their first 20k

	variants := []variant{
		{"one tenant, 1 filter d.T.>", func() (int, time.Duration, time.Duration, error) {
			return readStream(conns[0].js, "TLOG", one, oneWant, readOpts{})
		}},
		{"one tenant, 1 filter, pull 10k", func() (int, time.Duration, time.Duration, error) {
			return readStream(conns[0].js, "TLOG", one, oneWant, readOpts{consume: []jetstream.PullConsumeOpt{jetstream.PullMaxMessages(10_000)}})
		}},
		{"unfiltered, first 1/tenants", func() (int, time.Duration, time.Duration, error) {
			return readStream(conns[0].js, "TLOG", nil, cfg.perTenant, readOpts{})
		}},
		{"TenantAll 2 filters, first 20k", func() (int, time.Duration, time.Duration, error) {
			return readStream(conns[0].js, "TLOG", tenantAll(0), min(capped, wantAll(0)), readOpts{})
		}},
		{"TenantAll 2 filters, plain, first 20k", func() (int, time.Duration, time.Duration, error) {
			return readStream(conns[0].js, "TLOG", tenantAll(0), min(capped, wantAll(0)), readOpts{plain: true})
		}},
		{fmt.Sprintf("%d tenants parallel, 1 filter", cfg.tenants), func() (int, time.Duration, time.Duration, error) {
			var (
				wg    sync.WaitGroup
				mu    sync.Mutex
				n     int
				first time.Duration
				errs  []error
			)

			start := time.Now()

			for t := range cfg.tenants {
				wg.Go(func() {
					f := tenantAll(t)[0]
					m, fst, _, err := readStream(conns[t%len(conns)].js, "TLOG", []string{f}, want[f], readOpts{})

					mu.Lock()
					defer mu.Unlock()

					n += m
					if first == 0 || fst < first {
						first = fst
					}

					if err != nil {
						errs = append(errs, err)
					}
				})
			}

			wg.Wait()

			if len(errs) > 0 {
				return n, first, time.Since(start), errs[0]
			}

			return n, first, time.Since(start), nil
		}},
	}

	rows := make([]row, len(variants))

	off := run % len(variants)

	for i := range variants {
		vi := (off + i) % len(variants)
		v := variants[vi]
		ld := load1()

		n, first, tot, err := v.run()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.name, err)
		}

		ld = (ld + load1()) / 2
		rows[vi] = row{Cell: v.name, Target: float64(n), Achieved: float64(n) / tot.Seconds(), P50: tot.Seconds(), P90: float64(first.Microseconds()) / 1000, Load: ld}
		fmt.Printf("  %-34s %d msgs in %v (%.0f/s), first after %v, load %.1f\n", v.name, n, tot.Round(time.Millisecond), float64(n)/tot.Seconds(), first.Round(time.Microsecond), ld)
	}

	return rows, nil
}

type readOpts struct {
	plain   bool // a plain ephemeral pull consumer (AckNone) instead of an ordered one
	consume []jetstream.PullConsumeOpt
}

// readStream reads want messages through a new consumer from the start of the
// stream and reports how long the first and the last took.
func readStream(js jetstream.JetStream, stream string, filters []string, want int, o readOpts) (int, time.Duration, time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	start := time.Now()

	var (
		cons jetstream.Consumer
		err  error
	)

	if o.plain {
		cons, err = js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
			FilterSubjects: filters, AckPolicy: jetstream.AckNonePolicy, InactiveThreshold: time.Minute,
		})
	} else {
		cons, err = js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{FilterSubjects: filters})
	}

	if err != nil {
		return 0, 0, 0, err
	}

	var (
		n     atomic.Int64
		first atomic.Int64
	)

	done := make(chan struct{})

	cc, err := cons.Consume(func(m jetstream.Msg) {
		c := n.Add(1)
		if c == 1 {
			first.Store(int64(time.Since(start)))
		}

		if c == int64(want) {
			close(done)
		}
	}, o.consume...)
	if err != nil {
		return 0, 0, 0, err
	}

	defer cc.Stop()

	select {
	case <-done:
	case <-ctx.Done():
		return int(n.Load()), time.Duration(first.Load()), time.Since(start), fmt.Errorf("read %d of %d: %w", n.Load(), want, ctx.Err())
	}

	return int(n.Load()), time.Duration(first.Load()), time.Since(start), nil
}

// runFilterScaling shows why a multi-filter consumer is slow: its cost grows
// with the number of distinct subjects in each message block. For 10, 100 and
// 1000 subjects per tenant it fills a stream of 10 tenants x 20k messages and
// times the first 5000 messages through an ordered consumer with one filter
// (one tenant) and with two (two tenants).
func runFilterScaling(js jetstream.JetStream) ([]row, error) {
	var rows []row

	payload := make([]byte, 128)

	for _, subjects := range []int{10, 100, 1000} {
		name := fmt.Sprintf("SCALE%d", subjects)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: name, Subjects: []string{name + ".>"}, Replicas: 3, Storage: jetstream.FileStorage,
		}); err != nil {
			cancel()

			return nil, err
		}

		const tenants, per = 10, 20_000

		for i := range tenants * per {
			m := &nats.Msg{Subject: fmt.Sprintf("%s.t%d.dev.%d", name, i%tenants, (i/tenants)%subjects), Data: payload}

			for {
				if _, err := js.PublishMsgAsync(m); err == nil {
					break
				}

				time.Sleep(time.Millisecond)
			}
		}

		<-js.PublishAsyncComplete()

		for _, filters := range [][]string{{name + ".t0.>"}, {name + ".t0.>", name + ".t1.>"}} {
			ld := load1()

			n, first, tot, err := readStream(js, name, filters, 5000, readOpts{})
			if err != nil {
				cancel()

				return nil, err
			}

			r := row{
				Cell:   fmt.Sprintf("%d subjects/tenant, %d filter(s)", subjects, len(filters)),
				Target: float64(n), Achieved: float64(n) / tot.Seconds(), P50: tot.Seconds(), P90: float64(first.Microseconds()) / 1000,
				Load: (ld + load1()) / 2,
			}
			rows = append(rows, r)
			fmt.Printf("  %-34s %.0f msgs/s\n", r.Cell, r.Achieved)
		}

		_ = js.DeleteStream(ctx, name)

		cancel()
	}

	return rows, nil
}
