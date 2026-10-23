package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// setup is one cluster with its broker connections, ready for traffic.
type setup struct {
	name    string
	cl      *cluster
	pubs    []*nats.Conn
	brokers []*broker
	targets []target
}

func (s *setup) close() {
	if s.brokers != nil {
		stopBrokers(s.brokers)
	}

	if s.pubs != nil {
		closeAll(s.pubs)
	}

	if s.cl != nil {
		s.cl.stop()
	}
}

// newSetup builds an interest cluster (1M device subscriptions) or a fallback
// cluster (TenantAll per broker) and waits until every broker is reachable.
func newSetup(cfg config, interest bool) (*setup, error) {
	s := &setup{name: map[bool]string{true: "interest", false: "fallback"}[interest]}

	var err error

	if s.cl, err = startCluster(cfg.servers); err != nil {
		return nil, err
	}

	if s.pubs, err = connectPubs(s.cl); err != nil {
		s.close()

		return nil, err
	}

	if s.brokers, err = startBrokers(cfg, s.cl, interest); err != nil {
		s.close()

		return nil, err
	}

	errs := make([]error, len(s.brokers))

	var wg sync.WaitGroup

	for i, b := range s.brokers {
		wg.Go(func() {
			if interest {
				errs[i] = b.subscribeDevices()

				return
			}

			for _, t := range TenantAll(tenant) {
				if _, err := b.nc.ChanSubscribe("f."+t, b.ch); err != nil {
					errs[i] = err

					return
				}
			}

			errs[i] = b.nc.Flush()
		})
	}

	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		s.close()

		return nil, err
	}

	trk := newTracker(s.brokers, cfg.servers)
	for _, b := range s.brokers {
		b.trk.Store(trk)
	}

	stop := make(chan struct{})

	go probe(trk, s.brokers, s.pubs, 5*time.Millisecond, stop)

	select {
	case <-trk.all:
	case <-time.After(15 * time.Minute):
		close(stop)
		s.close()

		return nil, errors.New("propagation did not finish")
	}

	close(stop)

	s.targets = pickTargets(s.brokers, cfg.targetsPer)

	return s, nil
}

type cpuSample struct{ server, harness, sent float64 }

func (s *setup) window(cfg config, d time.Duration, traffic bool) cpuSample {
	s0 := s.cl.stats(false)
	u0, k0 := rusageSelf()

	var sent int64
	if traffic {
		sent = trafficFn(s.pubs, s.brokers, s.targets, cfg.rate, d, func(time.Duration) int { return phaseFallback })
	} else {
		time.Sleep(d)
	}

	s1 := s.cl.stats(false)
	u1, k1 := rusageSelf()

	return cpuSample{
		server:  float64(sumCPU(s1) - sumCPU(s0)),
		harness: float64((u1 + k1) - (u0 + k0)),
		sent:    float64(sent),
	}
}

// runCPUPaired keeps an interest cluster and a fallback cluster up at the same
// time and alternates traffic windows between them (I F, F I, I F, ...), so
// both modes see the same load on the shared machine. Each mode also gets one
// idle window, whose CPU rate is subtracted to give a net cost per message.
func runCPUPaired(cfg config) (*metrics, error) {
	m := newMetrics()
	m.note("load_start", loadavg())
	m.add("load1_start", load1())

	in, err := newSetup(cfg, true)
	if err != nil {
		return nil, err
	}
	defer in.close()

	fb, err := newSetup(cfg, false)
	if err != nil {
		return nil, err
	}
	defer fb.close()

	d := cfg.window
	idle := map[string]cpuSample{in.name: in.window(cfg, d, false), fb.name: fb.window(cfg, d, false)}
	samples := map[string][]cpuSample{}

	for p := range cfg.pairs {
		order := []*setup{in, fb}
		if p%2 == 1 {
			order = []*setup{fb, in}
		}

		for _, s := range order {
			x := s.window(cfg, d, true)
			samples[s.name] = append(samples[s.name], x)
			fmt.Printf("  window %d %-8s server %.1f us/msg, harness %.1f us/msg, load1 %.1f\n",
				p, s.name, x.server/x.sent, x.harness/x.sent, load1())
		}
	}

	totals := map[string]float64{}

	for _, name := range []string{in.name, fb.name} {
		var srv, hns, net []float64

		id := idle[name]
		for _, x := range samples[name] {
			srv = append(srv, x.server/x.sent)
			hns = append(hns, x.harness/x.sent)
			net = append(net, (x.server+x.harness-id.server-id.harness)/x.sent)
		}

		m.add("paired."+name+".idle_server_cores", id.server/1e6/d.Seconds())
		m.add("paired."+name+".server_cpu_us_per_msg", median(srv))
		m.add("paired."+name+".harness_cpu_us_per_msg", median(hns))
		m.add("paired."+name+".total_cpu_us_per_msg", median(srv)+median(hns))
		m.add("paired."+name+".net_of_idle_cpu_us_per_msg", median(net))
		totals[name] = median(net)
	}

	m.add("paired.fallback_over_interest_net_cpu", totals[fb.name]/totals[in.name])

	m.note("load_end", loadavg())
	m.add("load1_end", load1())

	return m, nil
}
