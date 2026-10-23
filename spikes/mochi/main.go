// Command mochi is the Phase 0 spike for Tether's cluster hooks on mochi v2.7.9.
//
// The tests answer the yes/no questions (lazy_test.go, evict_test.go,
// inject_test.go). Running the command measures a blocking OnPublish:
//
//	GOWORK=off go run . -runs 3
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
)

// startServer starts a mochi server on a random localhost port with the
// allow-all auth hook followed by hooks. caps may change the capabilities;
// wire, when set, runs before Serve so hooks can hold the server.
func startServer(caps func(*mqtt.Capabilities), wire func(*mqtt.Server), hooks ...mqtt.Hook) (*mqtt.Server, string, error) {
	c := mqtt.NewDefaultServerCapabilities()
	if caps != nil {
		caps(c)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("SPIKE_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	srv := mqtt.New(&mqtt.Options{InlineClient: true, Capabilities: c, Logger: logger})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		return nil, "", err
	}

	for _, h := range hooks {
		if err := srv.AddHook(h, nil); err != nil {
			return nil, "", err
		}
	}

	if wire != nil {
		wire(srv)
	}

	ln, err := new(net.ListenConfig).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}

	if err := srv.AddListener(listeners.NewNet("t1", ln)); err != nil {
		return nil, "", err
	}

	if err := srv.Serve(); err != nil {
		return nil, "", err
	}

	return srv, ln.Addr().String(), nil
}

// closeServer waits up to 5 s for every connection's goroutine to end, then
// closes srv. mochi v2.7.9's Close can deadlock when a client is added or
// deleted while it runs (Clients.GetByListener read-locks twice), which
// Tether's broker works around the same way.
func closeServer(srv *mqtt.Server) {
	done := make(chan struct{})

	go func() {
		srv.Listeners.ClientsWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}

	_ = srv.Close()
}

// ---------------------------------------------------------------------------
// Questions 1 and 2: load a session at CONNECT, evict it after DISCONNECT.
// ---------------------------------------------------------------------------

// storedSession is what the cluster layer would keep in the session KV key.
type storedSession struct {
	ProtocolVersion byte
	Expiry          uint32
	Subs            []packets.Subscription
	Inflight        []packets.Packet
}

// sessionHook stands in for Tether's session layer. It loads a session from
// store just before mochi looks for one (OnSessionEstablish), and flushes and
// evicts it when the connection ends (OnDisconnect).
type sessionHook struct {
	mqtt.HookBase

	srv *mqtt.Server

	// noConnectGuard drops the connecting check from eviction, so a test can
	// show what it protects against.
	noConnectGuard bool

	mu         sync.Mutex
	store      map[string]storedSession
	connecting map[string]int // connections between OnSessionEstablish and OnSessionEstablished

	materialized atomic.Int64
	evicted      atomic.Int64
	skipped      atomic.Int64
}

func newSessionHook(store map[string]storedSession) *sessionHook {
	if store == nil {
		store = map[string]storedSession{}
	}

	return &sessionHook{store: store, connecting: map[string]int{}}
}

func (h *sessionHook) ID() string { return "spike-sessions" }

// wire gives the hook its server; pass it to startServer.
func (h *sessionHook) wire(srv *mqtt.Server) { h.srv = srv }

func (h *sessionHook) Provides(b byte) bool {
	return b == mqtt.OnSessionEstablish || b == mqtt.OnSessionEstablished || b == mqtt.OnDisconnect
}

// OnSessionEstablish runs after authentication and before mochi's
// inheritClientSession. A session that only lives in the store is built as
// an offline mochi client, the same shape mochi's own loadClients gives a
// session restored from storage, so mochi takes it over as if it had always
// been here.
func (h *sessionHook) OnSessionEstablish(cl *mqtt.Client, pk packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.connecting[cl.ID]++

	if pk.Connect.Clean {
		delete(h.store, cl.ID)

		return
	}

	if _, ok := h.srv.Clients.Get(cl.ID); ok {
		return // already in memory on this node
	}

	ss, ok := h.store[cl.ID]
	if !ok {
		return
	}

	materialize(h.srv, cl.ID, cl.Net.Listener, ss)
	h.materialized.Add(1)
}

func (h *sessionHook) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.doneConnecting(cl.ID)
}

func (h *sessionHook) doneConnecting(id string) {
	if h.connecting[id] <= 1 {
		delete(h.connecting, id)

		return
	}

	h.connecting[id]--
}

// OnDisconnect flushes a persistent session to the store and evicts it from
// mochi, unless another connection has it or is about to take it.
func (h *sessionHook) OnDisconnect(cl *mqtt.Client, _ error, expire bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if expire {
		if !cl.IsTakenOver() {
			delete(h.store, cl.ID)
		}

		return
	}

	if !h.noConnectGuard && h.connecting[cl.ID] > 0 {
		h.skipped.Add(1)

		return
	}

	cur, ok := h.srv.Clients.Get(cl.ID)
	if !ok || cur != cl || cl.IsTakenOver() {
		h.skipped.Add(1)

		return
	}

	h.store[cl.ID] = flush(cl)
	evictSession(h.srv, cl)
	h.evicted.Add(1)
}

func (h *sessionHook) stored(id string) (storedSession, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ss, ok := h.store[id]

	return ss, ok
}

// materialize adds an offline client holding ss to srv, with its
// subscriptions in the topic index and its inflight messages queued.
func materialize(srv *mqtt.Server, id, listener string, ss storedSession) *mqtt.Client {
	off := srv.NewClient(nil, listener, id, false)
	off.Properties.ProtocolVersion = ss.ProtocolVersion
	off.Properties.Clean = false
	off.Properties.Props.SessionExpiryInterval = ss.Expiry
	off.Properties.Props.SessionExpiryIntervalFlag = true

	for _, sub := range ss.Subs {
		if srv.Topics.Subscribe(id, sub) {
			atomic.AddInt64(&srv.Info.Subscriptions, 1)
		}

		off.State.Subscriptions.Add(sub.Filter, sub)
	}

	for _, pk := range ss.Inflight {
		if off.State.Inflight.Set(pk) {
			atomic.AddInt64(&srv.Info.Inflight, 1)
		}
	}

	// Closed, with a disconnect time, like a client loadClients restored.
	off.Stop(packets.ErrServerShuttingDown)
	srv.Clients.Add(off)

	return off
}

// flush captures what the cluster layer would write to the session key.
func flush(cl *mqtt.Client) storedSession {
	ss := storedSession{
		ProtocolVersion: cl.Properties.ProtocolVersion,
		Expiry:          cl.Properties.Props.SessionExpiryInterval,
		Inflight:        cl.State.Inflight.GetAll(false),
	}

	for _, sub := range cl.State.Subscriptions.GetAll() {
		ss.Subs = append(ss.Subs, sub)
	}

	return ss
}

// evictSession removes cl's session from mochi's memory without the hook
// events an UNSUBSCRIBE or a session expiry would fire (OnUnsubscribed,
// OnQosDropped, OnClientExpired), so no storage hook mistakes the eviction
// for a client dropping its own state. The caller has checked that cl still
// owns its ID.
func evictSession(srv *mqtt.Server, cl *mqtt.Client) {
	for filter := range cl.State.Subscriptions.GetAll() {
		if srv.Topics.Unsubscribe(filter, cl.ID) {
			atomic.AddInt64(&srv.Info.Subscriptions, -1)
		}

		cl.State.Subscriptions.Delete(filter)
	}

	for _, pk := range cl.State.Inflight.GetAll(false) {
		if cl.State.Inflight.Delete(pk.PacketID) {
			atomic.AddInt64(&srv.Info.Inflight, -1)
		}
	}

	srv.Clients.Delete(cl.ID)
}

// ---------------------------------------------------------------------------
// Question 3: inject a message from the cluster for local delivery.
// ---------------------------------------------------------------------------

// clusterClientID names the inline client that injects cluster messages.
const clusterClientID = "tether-cluster"

// clusterHook stands in for the part of Tether's hook that forwards local
// publishes to the cluster. Anything the cluster injector publishes came
// from the cluster and must not go back.
type clusterHook struct {
	mqtt.HookBase

	injector *mqtt.Client

	sentToCluster  atomic.Int64 // publishes forwarded
	inlineSeen     atomic.Int64 // publishes where cl.Net.Inline was true
	retainForwards atomic.Int64 // retained writes forwarded to the KV
}

func (h *clusterHook) ID() string { return "spike-cluster" }

func (h *clusterHook) Provides(b byte) bool {
	return b == mqtt.OnPublish || b == mqtt.OnRetainMessage
}

func (h *clusterHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		h.inlineSeen.Add(1)
	}

	if cl != h.injector {
		h.sentToCluster.Add(1)
	}

	return pk, nil
}

func (h *clusterHook) OnRetainMessage(cl *mqtt.Client, _ packets.Packet, _ int64) {
	if cl != h.injector {
		h.retainForwards.Add(1)
	}
}

// keepPublishProps works around mochi dropping user properties from every
// packet, PUBLISH included, to a client that set Request Problem
// Information to 0. MQTT 5 [MQTT-3.1.2-29] only restricts packets other than
// PUBLISH, CONNACK and DISCONNECT. OnPacketEncode runs after mochi sets the
// flag, so the hook can clear it for PUBLISH.
type keepPublishProps struct {
	mqtt.HookBase
}

func (h *keepPublishProps) ID() string { return "spike-keep-publish-props" }

func (h *keepPublishProps) Provides(b byte) bool { return b == mqtt.OnPacketEncode }

func (h *keepPublishProps) OnPacketEncode(_ *mqtt.Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type == packets.Publish {
		pk.Mods.DisallowProblemInfo = false
	}

	return pk
}

// newInjector returns the inline client cluster messages are injected
// through. It is never added to srv.Clients. ProtocolVersion 5 keeps the
// packet's 5.0 properties meaningful to hooks that look at the version.
func newInjector(srv *mqtt.Server) *mqtt.Client {
	cl := srv.NewClient(nil, mqtt.LocalListener, clusterClientID, true)
	cl.Properties.ProtocolVersion = 5

	return cl
}

// inject delivers a message that arrived from the cluster to local
// subscribers, with its 5.0 properties.
func inject(srv *mqtt.Server, injector *mqtt.Client, topic string, payload []byte, qos byte, retain bool, props packets.Properties) error {
	return srv.InjectPacket(injector, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos, Retain: retain},
		TopicName:   topic,
		Payload:     payload,
		PacketID:    1, // unused for inline publishes; mochi assigns one per subscriber
		Properties:  props,
	})
}

// ---------------------------------------------------------------------------
// Question 4: a commit inside OnPublish.
// ---------------------------------------------------------------------------

type commitMode int

const (
	// modeBlocking sleeps for the commit inside OnPublish and lets mochi
	// send the PUBACK afterwards.
	modeBlocking commitMode = iota
	// modeAsyncAck starts the commit, returns packets.ErrRejectPacket so
	// mochi neither acks nor delivers, and writes the PUBACK (or PUBREC)
	// from the hook when the commit is done, in the order the PUBLISHes
	// arrived.
	modeAsyncAck
)

func (m commitMode) String() string {
	if m == modeAsyncAck {
		return "async-ack"
	}

	return "blocking"
}

// commitHook simulates the JetStream commit for QoS 1 publishes from
// clients whose ID starts with "load".
type commitHook struct {
	mqtt.HookBase

	mode   commitMode
	commit time.Duration
	srv    *mqtt.Server // for the inflight counter; set by wire

	queues sync.Map // *mqtt.Client -> chan pendingAck

	mu      sync.Mutex
	commits []time.Duration
}

type pendingAck struct {
	id   uint16
	qos  byte
	done <-chan struct{}
}

// wire gives the hook its server; pass it to startServer.
func (h *commitHook) wire(srv *mqtt.Server) { h.srv = srv }

func (h *commitHook) ID() string { return "spike-commit" }

func (h *commitHook) Provides(b byte) bool {
	return b == mqtt.OnPublish || b == mqtt.OnDisconnect
}

func (h *commitHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline || pk.FixedHeader.Qos == 0 || !strings.HasPrefix(cl.ID, "load") {
		return pk, nil
	}

	if h.mode == modeBlocking {
		h.doCommit()

		return pk, nil
	}

	q, ok := h.queues.Load(cl)
	if !ok {
		ch := make(chan pendingAck, 1024)
		q, ok = h.queues.LoadOrStore(cl, ch)

		if !ok {
			go h.writeAcks(cl, ch)
		}
	}

	done := make(chan struct{})

	go func() {
		h.doCommit()
		close(done)
	}()

	q.(chan pendingAck) <- pendingAck{id: pk.PacketID, qos: pk.FixedHeader.Qos, done: done}

	return pk, packets.ErrRejectPacket
}

func (h *commitHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	if q, ok := h.queues.LoadAndDelete(cl); ok {
		close(q.(chan pendingAck))
	}
}

func (h *commitHook) doCommit() {
	start := time.Now()
	time.Sleep(h.commit)
	d := time.Since(start)

	h.mu.Lock()
	h.commits = append(h.commits, d)
	h.mu.Unlock()
}

func (h *commitHook) medianCommit() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()

	return percentile(h.commits, 0.5)
}

// writeAcks sends each PUBACK or PUBREC once its commit is done, in arrival
// order. A PUBREC goes into the client's inflight first, the way mochi's
// processPublish leaves it, so mochi's own PUBREL handling finds it and
// answers PUBCOMP.
func (h *commitHook) writeAcks(cl *mqtt.Client, q <-chan pendingAck) {
	for p := range q {
		<-p.done

		ack := packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback},
			PacketID:    p.id,
			ReasonCode:  packets.CodeSuccess.Code,
			Created:     time.Now().Unix(),
		}

		if p.qos == 2 {
			ack.FixedHeader.Type = packets.Pubrec
			if cl.State.Inflight.Set(ack) {
				atomic.AddInt64(&h.srv.Info.Inflight, 1)
			}
		}

		_ = cl.WritePacket(ack)
	}
}

type loadConfig struct {
	mode           commitMode
	receiveMaximum uint16
	loadClients    int
	duration       time.Duration
	commit         time.Duration
}

type loadResult struct {
	rate         float64 // QoS 1 publishes acknowledged per second, all load clients
	probeP50     time.Duration
	probeP99     time.Duration
	probeSamples int
	commitMedian time.Duration
}

// runLoad measures QoS 1 throughput of cfg.loadClients clients against a
// commit inside OnPublish, while a probe client measures its own QoS 0 round
// trip. With loadClients 0 it measures the probe alone (the baseline).
func runLoad(ctx context.Context, cfg loadConfig) (loadResult, error) {
	hook := &commitHook{mode: cfg.mode, commit: cfg.commit}

	srv, addr, err := startServer(func(c *mqtt.Capabilities) {
		c.ReceiveMaximum = cfg.receiveMaximum
	}, hook.wire, hook)
	if err != nil {
		return loadResult{}, err
	}
	defer closeServer(srv)

	probe, err := newProbe(ctx, addr)
	if err != nil {
		return loadResult{}, err
	}
	defer probe.close()

	var acked atomic.Int64

	runCtx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	var wg sync.WaitGroup

	clients := make([]*paho.Client, 0, cfg.loadClients)
	defer func() {
		for _, c := range clients {
			_ = c.Disconnect(&paho.Disconnect{})
		}
	}()

	for i := range cfg.loadClients {
		c, _, err := dial(ctx, addr, fmt.Sprintf("load-%d", i), true, nil)
		if err != nil {
			return loadResult{}, err
		}

		clients = append(clients, c)

		// paho holds each client to the server's receive maximum, so one
		// more worker than that keeps the window full.
		for range int(cfg.receiveMaximum) + 1 {
			wg.Go(func() {
				payload := make([]byte, 64)
				for runCtx.Err() == nil {
					_, err := c.Publish(runCtx, &paho.Publish{Topic: "load/x", QoS: 1, Payload: payload})
					if err == nil {
						acked.Add(1)
					}
				}
			})
		}
	}

	start := time.Now()
	rtts := probe.run(runCtx)
	elapsed := time.Since(start)

	wg.Wait()

	return loadResult{
		rate:         float64(acked.Load()) / elapsed.Seconds(),
		probeP50:     percentile(rtts, 0.5),
		probeP99:     percentile(rtts, 0.99),
		probeSamples: len(rtts),
		commitMedian: hook.medianCommit(),
	}, nil
}

// probe publishes QoS 0 messages to its own topic and times each round trip.
type probe struct {
	c    *paho.Client
	recv chan time.Time
}

func newProbe(ctx context.Context, addr string) (*probe, error) {
	p := &probe{recv: make(chan time.Time, 16)}

	c, _, err := dial(ctx, addr, "probe", true, func(pr paho.PublishReceived) {
		var sent int64

		_, _ = fmt.Sscan(string(pr.Packet.Payload), &sent)
		select {
		case p.recv <- time.Unix(0, sent):
		default:
		}
	})
	if err != nil {
		return nil, err
	}

	p.c = c

	if _, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "probe/rtt", QoS: 0}}}); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *probe) run(ctx context.Context) []time.Duration {
	var rtts []time.Duration

	for ctx.Err() == nil {
		now := time.Now()
		if _, err := p.c.Publish(ctx, &paho.Publish{Topic: "probe/rtt", Payload: fmt.Appendf(nil, "%d", now.UnixNano())}); err != nil {
			continue
		}

		select {
		case sent := <-p.recv:
			rtts = append(rtts, time.Since(sent))
		case <-time.After(time.Second):
		case <-ctx.Done():
		}

		time.Sleep(2 * time.Millisecond)
	}

	return rtts
}

func (p *probe) close() { _ = p.c.Disconnect(&paho.Disconnect{}) }

// dial connects a 5.0 client. A persistent client asks for a one-hour
// session. onMsg, when set, gets every PUBLISH; paho acks it afterwards.
func dial(ctx context.Context, addr, id string, clean bool, onMsg func(paho.PublishReceived)) (*paho.Client, *paho.Connack, error) {
	conn, err := new(net.Dialer).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	cfg := paho.ClientConfig{Conn: conn}
	if onMsg != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				onMsg(pr)

				return true, nil
			},
		}
	}

	c := paho.NewClient(cfg)

	expiry := uint32(0)
	if !clean {
		expiry = 3600
	}

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ca, err := c.Connect(cctx, &paho.Connect{
		ClientID:   id,
		CleanStart: clean,
		KeepAlive:  30,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry, RequestProblemInfo: true},
	})
	if err != nil {
		_ = conn.Close()

		return nil, ca, err
	}

	return c, ca, nil
}

func percentile(in []time.Duration, p float64) time.Duration {
	if len(in) == 0 {
		return 0
	}

	s := slices.Clone(in)
	slices.Sort(s)

	return s[min(len(s)-1, int(float64(len(s))*p))]
}

func median(v []float64) float64 {
	s := slices.Clone(v)
	slices.Sort(s)

	return s[len(s)/2]
}

func durMedian(v []time.Duration) time.Duration { return percentile(v, 0.5) }

func sysctl(name string) string {
	out, err := exec.CommandContext(context.Background(), "sysctl", "-n", name).Output()
	if err != nil {
		return "?"
	}

	return strings.TrimSpace(string(out))
}

func main() {
	runs := flag.Int("runs", 3, "runs per configuration; medians are reported")
	dur := flag.Duration("dur", 3*time.Second, "measurement window per configuration")
	commit := flag.Duration("commit", 2*time.Millisecond, "simulated commit inside OnPublish")
	flag.Parse()

	if err := measure(context.Background(), *runs, *dur, *commit); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type row struct {
	name string
	cfg  loadConfig
}

func measure(ctx context.Context, runs int, dur, commit time.Duration) error {
	fmt.Printf("cpu: %s, %s cores, %s bytes RAM\n", sysctl("machdep.cpu.brand_string"), sysctl("hw.ncpu"), sysctl("hw.memsize"))
	fmt.Printf("commit %v, window %v, %d runs, medians\n", commit, dur, runs)

	rows := []row{{name: "baseline (probe only)", cfg: loadConfig{receiveMaximum: 10}}}
	for _, mode := range []commitMode{modeBlocking, modeAsyncAck} {
		for _, rm := range []uint16{1, 10, 100} {
			rows = append(rows, row{
				name: fmt.Sprintf("%s rm=%d, 1 client", mode, rm),
				cfg:  loadConfig{mode: mode, receiveMaximum: rm, loadClients: 1},
			})
		}
	}

	rows = append(rows, row{name: "blocking rm=10, 50 clients", cfg: loadConfig{mode: modeBlocking, receiveMaximum: 10, loadClients: 50}})

	results := make([][]loadResult, len(rows))

	for r := range runs {
		fmt.Printf("run %d: loadavg %s\n", r+1, sysctl("vm.loadavg"))

		for i, rw := range rows {
			cfg := rw.cfg
			cfg.duration = dur
			cfg.commit = commit

			res, err := runLoad(ctx, cfg)
			if err != nil {
				return fmt.Errorf("%s: %w", rw.name, err)
			}

			results[i] = append(results[i], res)
		}
	}

	fmt.Printf("loadavg at end: %s\n\n", sysctl("vm.loadavg"))
	fmt.Println("| configuration | QoS 1 msgs/s | probe p50 | probe p99 | probe samples | commit median |")
	fmt.Println("|---|---|---|---|---|---|")

	for i, rw := range rows {
		var rates []float64

		var p50, p99, cm []time.Duration

		samples := 0
		for _, res := range results[i] {
			rates = append(rates, res.rate)
			p50 = append(p50, res.probeP50)
			p99 = append(p99, res.probeP99)
			cm = append(cm, res.commitMedian)
			samples += res.probeSamples
		}

		fmt.Printf("| %s | %.0f | %v | %v | %d | %v |\n", rw.name, median(rates),
			durMedian(p50).Round(time.Microsecond), durMedian(p99).Round(time.Microsecond),
			samples/len(results[i]), durMedian(cm).Round(time.Microsecond))
	}

	return nil
}
