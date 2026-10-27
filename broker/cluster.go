package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/topic"
)

// Cluster makes the broker one node of a cell: the node's NATS connection and
// the cluster services built on it. Pass it as Deps.Cluster; nil runs the
// broker alone, as before.
//
// The caller connects, provisions the cell and builds the services; the
// broker uses them and closes none of them, except the Router (always its
// own) and a Shared it made itself.
type Cluster struct {
	// Conn is the node's NATS connection. Required.
	Conn *cluster.Conn

	// Config is the cluster configuration Conn was opened with. It must be
	// enabled; NodeID names this node in the cell.
	Config cluster.Config

	// Sessions is the cell's session store. Every CONNECT takes its client
	// ID's key there, and persistent sessions live there while their client
	// is away. Leave it nil and the broker makes one on Conn.
	Sessions *cluster.Sessions

	// Log is the QoS 1 and 2 message log. Every QoS 1 and 2 publish goes
	// through it, and the node delivers QoS 1 and 2 to its sessions only
	// from it. Leave it nil and the broker makes one on Conn.
	Log *cluster.Log

	// Members tracks the cell's live nodes. When it is set, New registers
	// the broker's fence, unfence and dead-peer callbacks on it
	// (clusterlive.go); starting and stopping it is the caller's job, Start
	// after the broker's and Stop after it. Nil: the node neither fences
	// itself nor takes over for dead nodes.
	Members *cluster.Members

	// Retained is the cell's retained message store. Every retained publish
	// is written there, and mochi's retained store becomes this node's cache
	// of it (clusterretained.go). Leave it nil and the broker makes one on
	// Conn.
	Retained *cluster.Retained

	// Shared runs this node's side of the cell's shared subscription groups
	// ("$share/<group>/<filter>", clustershared.go). Leave it nil and the
	// broker makes one on Conn over Log, and closes it on Stop. One you make
	// yourself (cluster.NewShared) you close, after the broker's Stop.
	Shared *cluster.Shared

	// Control is the cell's control bus (clustercontrol.go). When it is set,
	// New subscribes the broker's handlers on it (tenant.updated,
	// session.kick, session.delete), UpdateTenant sends tenant changes to
	// every node through it, and Kick and DeleteSession reach sessions other
	// nodes serve. Starting and stopping it is the caller's job: Start it
	// after New and after every other handler is subscribed (package events'
	// reactor), and before the broker's Start, so no client connects before
	// the node hears the bus; Stop it after the broker's Stop. Nil: tenant
	// changes stay on this node and a session another node serves can be
	// neither kicked nor deleted from here.
	Control *cluster.Control
}

// injectorID is the client ID of the injector, the inline client that
// publishes messages from other nodes into mochi. It never enters mochi's
// clients map, so no client can collide with it.
const injectorID = "tether-cluster"

// clusterNode is the broker's side of a cell.
type clusterNode struct {
	Cluster

	log *slog.Logger

	// metrics gets the node's cluster counters: Deps.Metrics when it is a
	// ClusterMetrics, else nothing.
	metrics ClusterMetrics

	// router moves QoS 0 messages between the nodes. New makes it on Conn
	// with deliverRouted as its deliver, and close closes it.
	router *cluster.Router

	// injector is the inline client that publishes routed messages into
	// mochi. The hook recognises it by pointer: mochi's own inline client
	// (admin publishes, $SYS) is inline too, and its QoS 0 publishes are
	// routed, while the injector's never are. Set once at Start.
	injector atomic.Pointer[mqtt.Client]

	// gate guards srv for inject. Stop clears srv under the write lock
	// before mochi shuts down, so no injection runs into a closing server.
	gate sync.RWMutex
	srv  *mqtt.Server

	// routes holds, per namespaced client ID, the tenant-relative filters
	// that hold a Router reference. Every reference is taken and dropped
	// through it, so each is released exactly once.
	routesMu sync.Mutex
	routes   map[string]map[string]*routeRef

	// published counts successful Router.Publish calls, routeErrors every
	// Router call that failed. Tests read them.
	published   atomic.Uint64
	routeErrors atomic.Uint64

	// The log side (clusterlog.go). readers holds one reader per tenant the
	// node serves, under readersMu, which is never held across a Log call.
	// readersClosed is set by Stop; no reader starts after it.
	readersMu     sync.Mutex
	readers       map[string]*tenantReader
	readersClosed bool
	sweepStop     chan struct{}
	sweepDone     chan struct{}

	// qos2In holds, per namespaced client ID, the message ID of each QoS 2
	// publish whose PUBREL has not come (the session's pending entries).
	qos2Mu sync.Mutex
	qos2In map[string]map[uint16]string

	// persistQoS2 writes a pending QoS 2 entry into the session's record
	// before the append (persistQoS2In). Nil: memory only (tests).
	persistQoS2 func(tenantID, rawID string, packetID uint16, msgID string) error

	// Timings, defaults in clusterlog.go; tests shorten them before Start.
	readerIdle, readerSweep                                time.Duration
	appendAttempt, appendLate, appendRetry, appendRetryMax time.Duration
	logProbe                                               time.Duration

	// logUp is what the node's pipes know about the log's health, for the
	// pacing of their retries (clusterlog.go).
	logUp logHealth

	// Counters for tests: appends committed (client and host publishes),
	// acks written, log messages injected, readers restarted, and attempts
	// to append a client publish (every send, retries included), and
	// outbound messages discarded over a client's Maximum Packet Size.
	appends, acks, logDelivered, readerRestarts, appendAttempts, oversized atomic.Uint64

	// holdAppend, when a test sets it before Start, runs before every
	// append of a client publish and may block until ctx ends. An error
	// fails that attempt, as a log that refuses appends would.
	holdAppend func(ctx context.Context) error

	// The session side (clustersession.go, cursor.go). mochi is the server,
	// kept after detach so the sessions that end during Stop can still be
	// evicted. sess holds the sessions this node owns, by namespaced client
	// ID, under sessMu; locks serialises the work on one session. tenants
	// holds the delivery state of each tenant's log, under tenantsMu.
	mochi       *mqtt.Server
	sessMu      sync.Mutex
	sess        map[string]*session
	locks       sessionLocks
	tenantsMu   sync.Mutex
	tenants     map[string]*tenantTrack
	writeSlots  chan struct{}
	maxInflight int

	watchStop            func()
	quit                 chan struct{} // closed when the watch stops
	flushStop, flushDone chan struct{}
	// flushInterval is how often changed records are written; tests may
	// shorten it before Start.
	flushInterval time.Duration

	// Counters for tests: sessions evicted from mochi, session records
	// written, sessions lost to another node, Acquire retries after a lost
	// race, messages sent by replays, replays that went live.
	evictions, sessionWrites, takeovers, casRetries, replayed, replaysDone atomic.Uint64
	// repins counts live sessions sent back to replaying after a pin;
	// ownedEvents the owned-keys watch's reports.
	repins, ownedEvents atomic.Uint64
	// replayRetries counts replays that ended short of the reader's last
	// message and went again.
	replayRetries atomic.Uint64
	// fetchRead counts the log messages fetchLogs' ranged reads went
	// through, for tests.
	fetchRead atomic.Uint64

	// b is the broker, for the work a session starts on its own (repin).
	b *Broker

	// liveness is the fence, the dead-node reclaims and the delayed wills
	// (clusterlive.go).
	liveness

	// retainedNode is the retained cache and the writes to the cell's
	// retained store (clusterretained.go).
	retainedNode

	// sharedNode is the node's memberships of shared groups and their
	// deliveries (clustershared.go).
	sharedNode

	// controlNode is the control bus side (clustercontrol.go).
	controlNode

	// Test seams, set before Start: afterAcquire runs in a CONNECT right
	// after its key is acquired, before the session is registered;
	// loadHook runs first in loadSession and fails it with its error.
	afterAcquire func()
	loadHook     func() error
	// beforeInject runs in deliverLog between pinFull and the injection,
	// under the tenant's lock.
	beforeInject func(seq uint64)
}

// routeRef is one session's reference on a (tenant, filter) route. It is
// pending while Router.Subscribe runs: a release that finds it pending
// leaves the Router alone and the subscriber gives the reference back.
type routeRef struct {
	active bool
}

// newClusterNode checks c and returns the broker's cluster side. deliver
// receives the messages a Router made here delivers.
func newClusterNode(c *Cluster, log *slog.Logger, deliver func(tenantID, name string, payload []byte, props cluster.Props, retain bool)) (*clusterNode, error) {
	if c.Conn == nil {
		return nil, invalid("deps.Cluster.Conn is required")
	}

	if !c.Config.Enabled {
		return nil, invalid("deps.Cluster.Config is not enabled")
	}

	if err := c.Config.Validate(); err != nil {
		return nil, fmt.Errorf("tether/broker: %w: cluster config: %w", tether.ErrInvalidInput, err)
	}

	n := &clusterNode{
		Cluster: *c,
		log:     log.With(slog.String("node", c.Config.NodeID)),
		metrics: noClusterMetrics{},
		routes:  make(map[string]map[string]*routeRef),

		readers:        make(map[string]*tenantReader),
		qos2In:         make(map[string]map[uint16]string),
		sess:           make(map[string]*session),
		tenants:        make(map[string]*tenantTrack),
		writeSlots:     make(chan struct{}, sessionWriteSlots),
		flushInterval:  defaultFlushInterval,
		readerIdle:     defaultReaderIdle,
		readerSweep:    defaultReaderSweep,
		appendAttempt:  defaultAppendAttempt,
		appendLate:     defaultAppendLate,
		appendRetry:    defaultAppendRetry,
		appendRetryMax: defaultAppendRetryMax,
		logProbe:       defaultLogProbe,
		liveness:       newLiveness(),
		retainedNode:   newRetainedNode(),
		sharedNode:     newSharedNode(),
	}

	if n.Log == nil {
		n.Log = cluster.NewLog(c.Conn, c.Config)
	}

	if n.Sessions == nil {
		n.Sessions = cluster.NewSessions(c.Conn, c.Config)
	}

	if n.Retained == nil {
		n.Retained = cluster.NewRetained(c.Conn, c.Config)
	}

	if n.Shared == nil {
		n.Shared = cluster.NewShared(c.Conn, c.Config, n.Log)
		n.ownShared = true
	}

	n.persistQoS2 = n.persistQoS2In

	n.router = cluster.NewRouter(c.Conn, c.Config.NodeID, deliver)

	return n, nil
}

// attach makes srv the server injections go to and creates the injector, a
// 5.0 inline client (spike 1): 5.0 so mochi keeps the properties of what it
// injects. Called once, from Start.
func (n *clusterNode) attach(srv *mqtt.Server) {
	inj := srv.NewClient(nil, mqtt.LocalListener, injectorID, true)
	inj.Properties.ProtocolVersion = 5
	n.injector.Store(inj)

	n.mochi = srv
	n.maxInflight = int(srv.Options.Capabilities.MaximumInflight)

	n.gate.Lock()
	n.srv = srv
	n.gate.Unlock()
}

// detach stops injections. When it returns none is running and none starts.
func (n *clusterNode) detach() {
	n.gate.Lock()
	n.srv = nil
	n.gate.Unlock()
}

// close closes the Router and the Shared the broker made. Called after
// mochi has shut down, so the releases of the last clean sessions still
// reach them.
func (n *clusterNode) close() {
	n.router.Close()

	if n.ownShared {
		n.Shared.Close()
	}
}

// isInjector reports whether cl is this node's injector.
func (n *clusterNode) isInjector(cl *mqtt.Client) bool {
	return cl == n.injector.Load()
}

// injectedOrigin moves the publisher's client ID that inject put in an
// injected packet into its origin, which mochi checks for No Local.
func injectedOrigin(pk *packets.Packet) {
	if o := pk.Connect.ClientIdentifier; o != "" {
		pk.Origin = o
		pk.Connect.ClientIdentifier = ""
	}
}

// errNotInjecting is what inject returns before Start and once Stop began.
var errNotInjecting = fmt.Errorf("tether/broker: inject: %w: broker not running", tether.ErrConflict)

// inject publishes a message from the cell into mochi through the injector,
// on tenantID's namespaced name, so local delivery, the ACL check on every
// delivery and the outbound stripping run as for any message. The injector
// is inline: no rate limit, no message counted in, never routed or appended
// again. qos is the QoS mochi delivers at most: mochi takes an inline publish
// of any QoS as already received, fans it out at once and keeps no inflight
// state for the injector (its PacketID only passes mochi's validation).
// origin, when not empty, is the namespaced client ID of the publisher, which
// OnPublish makes the packet's origin so a No Local subscription of that
// client skips it.
//
// seq, when not zero, is the message's log sequence, tagged on the packet
// (tagSeq) so the copies mochi makes for each subscriber carry it.
//
// retain is the publish's retain flag. The packet goes in with its retain
// flag clear, so mochi never stores it (the node's retained cache follows the
// cell's store, clusterretained.go), and tagged instead (tagRetained), so a
// 5.0 subscriber with Retain As Published gets it with the flag set
// (OnPacketEncode).
func (b *Broker) inject(tenantID, name string, payload []byte, qos byte, props cluster.Props, origin string, seq uint64, retain bool) error {
	n := b.cluster

	n.gate.RLock()
	defer n.gate.RUnlock()

	if n.srv == nil {
		return errNotInjecting
	}

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
		TopicName:   topic.Namespace(tenantID, name),
		Payload:     payload,
		Properties:  packetProps(props),
	}

	if qos > 0 {
		// Unused for an inline publish, but mochi's validation wants one.
		pk.PacketID = 1
	}

	// Mochi sets the origin to the injector's ID before OnPublish, so the
	// publisher's ID rides in a field a PUBLISH never uses and OnPublish
	// moves it over (injectedOrigin).
	pk.Connect.ClientIdentifier = origin

	if seq > 0 {
		tagSeq(&pk, seq)
	}

	if retain {
		tagRetained(&pk)
	}

	if err := n.srv.InjectPacket(n.injector.Load(), pk); err != nil {
		return fmt.Errorf("tether/broker: inject: %w", err)
	}

	return nil
}

// deliverRouted hands the broker a QoS 0 message that another node routed,
// as tenantID's message on the tenant-relative topic name, with its retain
// flag. It is the deliver function of the broker's Router. It does not block
// on clients. The retain flag reaches only 5.0 subscriptions with Retain As
// Published; the retained message itself comes from the cell's retained
// store, never from here.
//
// The message comes off NATS, so it is checked like an admin publish: a
// tenant ID that is not canonical, an invalid name and a name under $SYS
// (in any casing) are dropped and logged. Before Start and after Stop it
// drops everything.
func (b *Broker) deliverRouted(tenantID, name string, payload []byte, props cluster.Props, retain bool) {
	if b.cluster == nil {
		return
	}

	if !validTenant(tenantID) || topic.Valid(name, false, subject.MaxTopicBytes) != nil || hasSysPrefix(name) {
		b.cluster.log.Warn("tether: routed message dropped, not a tenant topic", slog.String("tenant", tenantID), slog.String("topic", name))

		return
	}

	if err := b.inject(tenantID, name, payload, 0, props, "", 0, retain); err != nil && !errors.Is(err, errNotInjecting) {
		b.cluster.log.Warn("tether: routed message not injected", slog.String("tenant", tenantID), slog.String("topic", name), slog.Any("error", err))
	}
}

// publish routes one QoS 0 message to the other nodes of the cell. QoS 0 is
// best effort: a failure is logged and counted, and the local delivery goes
// ahead regardless. The retain flag travels (ruling R33b), whether pk still
// has it or the hook already moved it to the tag, for the other nodes'
// Retain As Published subscribers; the retained message itself reaches every
// node through the retained store.
func (n *clusterNode) publish(tenantID, rel string, pk *packets.Packet) {
	retain := pk.FixedHeader.Retain || retainedTag(*pk)

	if err := n.router.Publish(tenantID, rel, pk.Payload, clusterProps(pk.Properties), retain); err != nil {
		n.routeErrors.Add(1)
		n.log.Warn("tether: qos 0 message not routed", slog.String("tenant", tenantID), slog.String("topic", rel), slog.Any("error", err))

		return
	}

	n.published.Add(1)
}

// publishInline routes a QoS 0 publish of mochi's inline client (Publish,
// DevicePublish, DeleteRetained) to the other nodes. The name is
// namespaced; the tenant comes from it. The tenant $SYS stats each node
// publishes stay on that node, and mochi's own $SYS/broker names belong to
// no tenant.
func (n *clusterNode) publishInline(pk *packets.Packet) {
	tenantID, ok := tenantOfName(pk.TopicName)
	if !ok {
		return
	}

	rel, ok := topic.Strip(tenantID, pk.TopicName)
	if !ok || hasSysPrefix(rel) {
		return
	}

	n.publish(tenantID, rel, pk)
}

// subscribe takes nsID's reference on (tenantID, filter) unless it holds one
// already, which a SUBSCRIBE that replaces a subscription finds. The Router
// call runs outside routesMu. It returns the Router's error, after which
// nsID holds no reference.
func (n *clusterNode) subscribe(nsID, tenantID, filter string) error {
	n.routesMu.Lock()

	set := n.routes[nsID]
	if _, ok := set[filter]; ok {
		n.routesMu.Unlock()

		return nil
	}

	if set == nil {
		set = make(map[string]*routeRef)
		n.routes[nsID] = set
	}

	ref := &routeRef{}
	set[filter] = ref
	n.routesMu.Unlock()

	err := n.router.Subscribe(tenantID, filter)

	n.routesMu.Lock()

	if n.routes[nsID][filter] == ref {
		if err == nil {
			ref.active = true
		} else {
			n.forget(nsID, filter)
		}

		n.routesMu.Unlock()

		return err
	}

	n.routesMu.Unlock()

	// Released while the subscribe ran. The release skipped the Router
	// because the reference was pending, so it is given back here.
	if err == nil {
		n.unsubscribe(tenantID, filter)
	}

	return err
}

// release drops nsID's references on filters, the tenant-relative filters
// mochi no longer holds for the session. A filter with no reference is
// skipped, so a second release of the same filter does nothing.
func (n *clusterNode) release(nsID, tenantID string, filters []string) {
	n.routesMu.Lock()

	var drop []string

	for _, f := range filters {
		ref, ok := n.routes[nsID][f]
		if !ok {
			continue
		}

		n.forget(nsID, f)

		if ref.active {
			drop = append(drop, f)
		}
	}

	n.routesMu.Unlock()

	for _, f := range drop {
		n.unsubscribe(tenantID, f)
	}
}

// releaseAll drops every reference nsID holds, and ends its shared group
// memberships (endShared): the session is over on this node.
func (n *clusterNode) releaseAll(nsID, tenantID string) {
	defer n.endShared(nsID)

	n.routesMu.Lock()

	filters := make([]string, 0, len(n.routes[nsID]))
	for f := range n.routes[nsID] {
		filters = append(filters, f)
	}

	n.routesMu.Unlock()

	n.release(nsID, tenantID, filters)
}

// forget removes one entry. routesMu must be held.
func (n *clusterNode) forget(nsID, filter string) {
	set := n.routes[nsID]
	delete(set, filter)

	if len(set) == 0 {
		delete(n.routes, nsID)
	}
}

// unsubscribe drops one Router reference, logging and counting a failure.
func (n *clusterNode) unsubscribe(tenantID, filter string) {
	if err := n.router.Unsubscribe(tenantID, filter); err != nil {
		n.routeErrors.Add(1)
		n.log.Warn("tether: route not released", slog.String("tenant", tenantID), slog.String("filter", filter), slog.Any("error", err))
	}
}

// routed returns the tenant-relative filters nsID holds references on, for
// tests.
func (n *clusterNode) routed(nsID string) []string {
	n.routesMu.Lock()
	defer n.routesMu.Unlock()

	out := make([]string, 0, len(n.routes[nsID]))
	for f := range n.routes[nsID] {
		out = append(out, f)
	}

	return out
}

// routedFilter returns the tenant-relative form of a namespaced filter of
// tenantID that the Router carries, and false for one it does not: a shared
// filter (its group carries those, clustershared.go) or one outside the
// tenant.
func routedFilter(tenantID, filter string) (string, bool) {
	rel, ok := topic.Strip(tenantID, filter)
	if !ok || rel == "" || withoutShare(rel) != rel {
		return "", false
	}

	return rel, true
}

// tenantOfName returns the tenant of a namespaced topic name: its first
// level, or its second one for a name whose first level starts with "$".
func tenantOfName(name string) (string, bool) {
	first, rest, ok := strings.Cut(name, "/")
	if !ok {
		return "", false
	}

	if strings.HasPrefix(first, "$") {
		first, _, _ = strings.Cut(rest, "/")
	}

	if !validTenant(first) {
		return "", false
	}

	return first, true
}

// hasSysPrefix reports whether a tenant-relative name starts with $SYS in
// any casing, the names neither a client nor the host may publish.
func hasSysPrefix(name string) bool {
	return len(name) >= len(sysPrefix) && strings.EqualFold(name[:len(sysPrefix)], sysPrefix)
}

// clusterProps copies the MQTT 5 publish properties that travel with a
// message from mochi's form into the cluster's.
func clusterProps(p packets.Properties) cluster.Props {
	out := cluster.Props{
		ContentType:     p.ContentType,
		ResponseTopic:   p.ResponseTopic,
		CorrelationData: p.CorrelationData,
		MessageExpiry:   p.MessageExpiryInterval,
	}

	if len(p.User) > 0 {
		out.User = make([]cluster.UserProperty, len(p.User))
		for i, u := range p.User {
			out.User[i] = cluster.UserProperty{Key: u.Key, Value: u.Val}
		}
	}

	if p.PayloadFormatFlag {
		f := p.PayloadFormat
		out.PayloadFormat = &f
	}

	return out
}

// packetProps is the inverse of clusterProps.
func packetProps(p cluster.Props) packets.Properties {
	out := packets.Properties{
		ContentType:           p.ContentType,
		ResponseTopic:         p.ResponseTopic,
		CorrelationData:       p.CorrelationData,
		MessageExpiryInterval: p.MessageExpiry,
	}

	if len(p.User) > 0 {
		out.User = make([]packets.UserProperty, len(p.User))
		for i, u := range p.User {
			out.User[i] = packets.UserProperty{Key: u.Key, Val: u.Value}
		}
	}

	if p.PayloadFormat != nil {
		out.PayloadFormat = *p.PayloadFormat
		out.PayloadFormatFlag = true
	}

	return out
}
