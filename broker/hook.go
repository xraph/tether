package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/topic"
)

// connectTimeout bounds the authentication and permission lookups of one
// CONNECT. Mochi gives the hook no context.
const connectTimeout = 10 * time.Second

// sysPrefix is mochi's own topic prefix. Mochi drops client publishes to any
// name starting with it (in any casing) before the ACL check.
const sysPrefix = "$SYS"

// hook is Tether's mochi hook. It must stay the only hook that provides
// OnConnectAuthenticate and OnACLCheck: mochi ORs those across hooks and stops
// at the first true, so a second provider would widen access.
type hook struct {
	mqtt.HookBase

	b      *Broker
	server *mqtt.Server
}

// ID implements mqtt.Hook.
func (h *hook) ID() string { return "tether" }

// provided lists the mochi hook methods the Tether hook implements.
var provided = []byte{
	mqtt.OnConnectAuthenticate,
	mqtt.OnACLCheck,
	mqtt.OnSessionEstablish,
	mqtt.OnPacketRead,
	mqtt.OnPacketEncode,
	mqtt.OnPublish,
	mqtt.OnWill,
}

// Provides implements mqtt.Hook.
func (h *hook) Provides(b byte) bool {
	return bytes.IndexByte(provided, b) >= 0
}

// OnConnectAuthenticate authenticates the client, loads and compiles its
// grants, checks its will against them and takes a connection slot. On any
// failure it records the CONNACK reason for OnPacketEncode and returns false.
//
// A key, role or tenant change announced while this runs (Broker.changes
// moved) may have been handled before this connection's state was in the
// registry, so the reaction never saw it. The connection then authorizes a
// second time, now visible to every later reaction. Credentials that expire
// start a timer that disconnects the client at that moment.
func (h *hook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	log := h.b.log.With(slog.String("client", cl.ID), slog.String("remote", cl.Net.Remote))

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	gen := h.b.changes.Load()

	ident, g, ok := h.authorize(ctx, cl, pk, log)
	if !ok {
		return false
	}

	tenantID := ident.Tenant.ID.String()
	log = log.With(slog.String("tenant", tenantID))

	if !h.acquireSlot(tenantID, cl.ID, ident.Tenant.Limits.MaxConnections) {
		return h.refuse(cl, log, packets.ErrQuotaExceeded, "tenant connection limit reached", nil)
	}

	st := newClientState(cl, cl.ID, string(pk.Connect.Username), ident, ident.Tenant, g, h.b.deps.Limits, h.b.countersFor(tenantID))
	st.metrics = h.b.deps.Metrics
	st.sessionExpiry = sessionExpiry(cl, h.b.cfg.sessionExpirySeconds())

	if !h.b.state.addUnlessDeleting(st, namespacedID(tenantID, cl.ID)) {
		st.releaseSlot()

		return h.refuse(cl, log, packets.ErrServerUnavailable, "session is being deleted", nil)
	}

	if h.b.changes.Load() != gen && !h.reauthorize(ctx, cl, pk, st, log) {
		h.b.state.remove(cl)
		st.releaseSlot()

		return false
	}

	h.startExpiry(st)

	return true
}

// authorize authenticates the CONNECT, refuses a tenant that is missing or
// disabled and credentials that have already expired, loads and compiles the
// grants and checks the will against them. On failure it records the
// refusal and returns false.
func (h *hook) authorize(ctx context.Context, cl *mqtt.Client, pk packets.Packet, log *slog.Logger) (*auth.Identity, *grants, bool) {
	username := string(pk.Connect.Username)

	ident, err := h.b.deps.Auth.Authenticate(ctx, auth.Credentials{
		ClientID: cl.ID,
		Username: username,
		Password: pk.Connect.Password,
		Remote:   cl.Net.Remote,
	})
	if err != nil {
		return nil, nil, h.refuse(cl, log, connackCode(err), "authentication failed", err)
	}

	if ident == nil || ident.Tenant == nil || ident.Tenant.ID.IsNil() {
		return nil, nil, h.refuse(cl, log, packets.ErrServerUnavailable, "authenticator returned no tenant", nil)
	}

	log = log.With(slog.String("tenant", ident.Tenant.ID.String()))

	if !ident.Tenant.Enabled {
		return nil, nil, h.refuse(cl, log, packets.ErrNotAuthorized, "tenant disabled", nil)
	}

	if !ident.ExpiresAt.IsZero() && !time.Now().Before(ident.ExpiresAt) {
		return nil, nil, h.refuse(cl, log, packets.ErrBadUsernameOrPassword, "credentials expired", nil)
	}

	rules, roleIDs, err := h.b.deps.Perms.Load(ctx, ident)
	if err != nil {
		return nil, nil, h.refuse(cl, log, packets.ErrServerUnavailable, "loading permissions failed", err)
	}

	g := h.b.compile(rules, roleIDs, cl.ID, username, log)

	// Mochi publishes a will without a publish ACL check, so it is checked
	// here, the same way OnACLCheck would check a PUBLISH to its topic.
	if atomic.LoadUint32(&cl.Properties.Will.Flag) == 1 {
		will := cl.Properties.Will.TopicName
		if topic.Valid(will, false, h.b.cfg.MaxTopicLength) != nil {
			return nil, nil, h.refuse(cl, log, packets.ErrTopicNameInvalid, "invalid will topic", nil)
		}

		if !canPublish(g.acl, will) {
			return nil, nil, h.refuse(cl, log, packets.ErrNotAuthorized, "will topic not granted", nil)
		}
	}

	return ident, g, true
}

// reauthorize runs authorize again for a connection whose state is already
// in the registry and replaces its identity and grants with the result. It
// holds reloadMu, so a reload that found the state meanwhile applies after
// this one and never loses to it.
func (h *hook) reauthorize(ctx context.Context, cl *mqtt.Client, pk packets.Packet, st *clientState, log *slog.Logger) bool {
	st.reloadMu.Lock()
	defer st.reloadMu.Unlock()

	ident, g, ok := h.authorize(ctx, cl, pk, log)
	if !ok {
		return false
	}

	if ident.Tenant.ID.String() != st.tenantID {
		return h.refuse(cl, log, packets.ErrNotAuthorized, "tenant changed during connect", nil)
	}

	st.ident.Store(ident)
	st.tenant.Store(ident.Tenant)
	st.grants.Store(g)

	return true
}

// startExpiry starts the timer that disconnects the client when its
// credentials expire (reason 0xA0, maximum connect time). The timer is
// stopped when the connection's state is dropped and when another
// connection takes its session over.
func (h *hook) startExpiry(st *clientState) {
	exp := st.ident.Load().ExpiresAt
	if exp.IsZero() {
		return
	}

	st.setExpiry(time.AfterFunc(time.Until(exp), func() { h.expire(st) }))
}

// expire disconnects a connection whose credentials expired, unless it is
// already gone.
func (h *hook) expire(st *clientState) {
	if h.b.state.get(st.cl) != st || st.cl.Closed() {
		return
	}

	h.b.log.Info("tether: credentials expired, disconnecting", slog.String("client", st.cl.ID), slog.String("tenant", st.tenantID))
	disconnect(h.server, h.b.log, st.cl, packets.ErrMaxConnectTime)
}

// acquireSlot takes a tenant connection slot. When the tenant is at its limit
// and a live connection already owns this client ID, the new connection is a
// takeover that mochi is about to disconnect the old one for, so it inherits
// the old connection's slot and needs no second one.
//
// Before that, it sweeps the tenant's closed connections. Mochi skips
// OnDisconnect when writing the success CONNACK or resending inflight
// messages fails, so their slots would otherwise stay taken for good.
//
// A slot taken here is reported as an opened connection; an inherited one is
// not, since the connection it came from never reports its close.
func (h *hook) acquireSlot(tenantID, rawID string, maxConns int) bool {
	if h.acquire(tenantID, maxConns) {
		return true
	}

	if h.b.state.sweepClosed(tenantID) > 0 && h.acquire(tenantID, maxConns) {
		return true
	}

	prev := h.b.state.owner(namespacedID(tenantID, rawID))
	if prev == nil {
		return false
	}

	if prev.transferSlot() {
		return true
	}

	// The old connection released its slot in the meantime.
	return h.acquire(tenantID, maxConns)
}

// acquire takes a fresh tenant connection slot and reports the connection
// as opened.
func (h *hook) acquire(tenantID string, maxConns int) bool {
	if !h.b.deps.Limits.AcquireConnection(tenantID, maxConns) {
		return false
	}

	h.b.deps.Metrics.ConnectionOpened()

	return true
}

func (h *hook) refuse(cl *mqtt.Client, log *slog.Logger, code packets.Code, msg string, err error) bool {
	h.b.state.refuse(cl, code)
	h.b.deps.Metrics.AuthFailure(authReason(code))
	log.Info("tether: connection refused", slog.String("reason", msg), slog.Int("code", int(code.Code)), slog.Any("error", err))

	return false
}

// connackCode maps an Authenticator error to a CONNACK reason.
func connackCode(err error) packets.Code {
	switch {
	case errors.Is(err, auth.ErrBadCredentials):
		return packets.ErrBadUsernameOrPassword
	case errors.Is(err, auth.ErrNotAuthorized):
		return packets.ErrNotAuthorized
	case errors.Is(err, auth.ErrRateLimited):
		return packets.ErrConnectionRateExceeded
	default:
		return packets.ErrServerUnavailable
	}
}

// v3ConnackCode is the MQTT 3.1.1 CONNACK return code for a 5.0 reason.
// 3.1.1 has no quota or rate codes, so those read as server unavailable.
func v3ConnackCode(code packets.Code) byte {
	switch code {
	case packets.ErrBadUsernameOrPassword:
		return packets.ErrMalformedUsernameOrPassword.Code // 0x04, bad user name or password
	case packets.ErrServerUnavailable, packets.ErrQuotaExceeded, packets.ErrConnectionRateExceeded:
		return packets.Err3ServerUnavailable.Code
	default:
		return packets.Err3NotAuthorized.Code
	}
}

// OnSessionEstablish namespaces the client ID and the will topic, turns off
// outbound topic aliases and indexes the state by the namespaced ID. It runs
// before mochi looks the session up, so the session, a takeover and storage
// all see the namespaced ID: two tenants can share a raw client ID, and no
// client can reach mochi's inline client.
func (h *hook) OnSessionEstablish(cl *mqtt.Client, _ packets.Packet) {
	st := h.b.state.get(cl)
	if st == nil {
		// Only this hook authenticates, so this cannot happen. If it does,
		// park the client on an ID no tenant and no inline client owns and
		// close it.
		h.b.log.Error("tether: session without authentication state", slog.String("client", cl.ID))
		cl.ID = fmt.Sprintf("tether-unbound/%p", cl)
		cl.Stop(packets.ErrNotAuthorized)

		return
	}

	cl.ID = namespacedID(st.tenantID, st.rawID)

	if atomic.LoadUint32(&cl.Properties.Will.Flag) == 1 {
		cl.Properties.Will.TopicName = topic.Namespace(st.tenantID, cl.Properties.Will.TopicName)
	}

	cl.Properties.Props.TopicAliasMaximum = 0

	h.b.state.bind(st)
}

// OnPacketRead namespaces every inbound topic. A PUBLISH with an empty topic
// (an alias-only publish), an invalid or an over-length topic closes the
// connection, with a DISCONNECT for 5.0 clients, before any ACL check sees it.
// SUBSCRIBE and UNSUBSCRIBE keep their filter count: an invalid filter becomes
// the empty filter, which mochi refuses (SUBACK 0x8F) before the ACL check.
func (h *hook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		return pk, nil
	}

	switch pk.FixedHeader.Type {
	case packets.Publish, packets.Subscribe, packets.Unsubscribe:
	default:
		return pk, nil
	}

	st := h.b.state.get(cl)
	if st == nil {
		return pk, packets.ErrRejectPacket
	}

	if pk.FixedHeader.Type == packets.Publish {
		switch {
		case pk.TopicName == "" && pk.Properties.TopicAliasFlag:
			return h.reject(cl, pk, packets.ErrTopicAliasInvalid)
		case pk.TopicName == "":
			return h.reject(cl, pk, packets.ErrProtocolViolationNoTopic)
		case topic.Valid(pk.TopicName, false, h.b.cfg.MaxTopicLength) != nil:
			return h.reject(cl, pk, packets.ErrTopicNameInvalid)
		}

		pk.TopicName = topic.Namespace(st.tenantID, pk.TopicName)

		return pk, nil
	}

	filters := make(packets.Subscriptions, len(pk.Filters))
	copy(filters, pk.Filters)

	for i := range filters {
		if topic.Valid(filters[i].Filter, true, h.b.cfg.MaxTopicLength) != nil {
			filters[i].Filter = ""

			continue
		}

		filters[i].Filter = topic.Namespace(st.tenantID, filters[i].Filter)
	}

	pk.Filters = filters

	return pk, nil
}

// reject closes the connection over a bad PUBLISH. A 5.0 client gets a
// DISCONNECT with the reason first.
func (h *hook) reject(cl *mqtt.Client, pk packets.Packet, code packets.Code) (packets.Packet, error) {
	h.b.log.Info("tether: publish rejected", slog.String("client", cl.ID), slog.Int("code", int(code.Code)))

	if cl.Properties.ProtocolVersion == 5 {
		disconnect(h.server, h.b.log, cl, code)
	}

	return pk, packets.ErrRejectPacket
}

// OnPacketEncode strips the tenant prefix from every PUBLISH a networked
// client receives and counts it as a message out for the receiver's tenant,
// and gives a refused CONNECT the CONNACK reason OnConnectAuthenticate chose
// (mochi always sends bad username or password).
//
// The success CONNACK of a connection that inherited a session is the one
// point between mochi's session takeover and its resend of the session's
// inflight messages, so that is where the inherited session is pruned to the
// connection's grants (pruneSession): a grant revoked while the device was
// offline loses its subscription and its queued messages.
func (h *hook) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	switch pk.FixedHeader.Type {
	case packets.Connack:
		code, ok := h.b.state.takeRefusal(cl)
		if !ok {
			// Mochi has just handed this connection the session it
			// inherited and will resend the session's inflight messages
			// right after this CONNACK.
			if pk.SessionPresent {
				if st := h.b.state.get(cl); st != nil {
					h.b.pruneSession(h.server, st)
				}
			}

			return pk
		}

		if cl.Properties.ProtocolVersion < 5 {
			pk.ReasonCode = v3ConnackCode(code)
		} else {
			pk.ReasonCode = code.Code
			pk.Properties.ReasonString = code.Reason
		}

		return pk
	case packets.Publish:
		if cl.Net.Inline {
			return pk
		}

		tenantID, counters, ok := h.tenantOf(cl)
		if !ok {
			return h.withhold(cl, pk)
		}

		name, ok := topic.Strip(tenantID, pk.TopicName)
		if !ok {
			return h.withhold(cl, pk)
		}

		pk.TopicName = name
		counters.out.Add(1)
		h.b.deps.Metrics.MessageOut()

		return pk
	default:
		return pk
	}
}

// withhold fails closed on an outbound PUBLISH whose topic is not in the
// receiver's namespace. Every delivery passed OnACLCheck, which only allows
// names in the client's namespace, so this is a bug; but the packet would
// carry another tenant's ID, topic and data, and OnPacketEncode cannot drop
// it. So the client is stopped (implementation specific error), which closes
// its connection before mochi writes, and the packet that mochi then fails to
// write is a blank one: a fixed topic naming no tenant, no payload and no
// properties. Nothing is counted.
func (h *hook) withhold(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	h.b.log.Error("tether: outbound publish outside the client's namespace, closing the client", slog.String("client", cl.ID))
	cl.Stop(packets.ErrImplementationSpecificError)

	return packets.Packet{
		FixedHeader:     pk.FixedHeader,
		PacketID:        pk.PacketID,
		ProtocolVersion: pk.ProtocolVersion,
		Mods:            pk.Mods,
		TopicName:       withheldTopic,
	}
}

// OnPublish runs after the ACL check. The topic is already namespaced; this
// re-checks it after mochi's alias handling, as a guard. Then it spends one
// message of the tenant's rate budget (limits.AllowMessage, per tenant on
// this node) and counts the message in. The inline client is never limited
// or counted.
//
// Over the budget, a 5.0 QoS 1 publish gets PUBACK 0x97 (quota exceeded):
// mochi writes a PUBACK with a code returned here, and it type-asserts the
// error, so the code is returned bare, never wrapped. Everything else is
// dropped with ErrRejectPacket: for 3.1.1 or QoS 0 mochi would publish the
// message anyway on any other error, and for QoS 2 it would answer with a
// PUBACK where a PUBREC belongs.
func (h *hook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		return pk, nil
	}

	st := h.b.state.get(cl)
	if st == nil {
		return pk, packets.ErrRejectPacket
	}

	if _, ok := topic.Strip(st.tenantID, pk.TopicName); !ok {
		return pk, packets.ErrRejectPacket
	}

	if !h.b.deps.Limits.AllowMessage(st.tenantID, st.tenant.Load().Limits) {
		h.b.log.Debug("tether: publish over the tenant's rate limit", slog.String("client", cl.ID), slog.String("tenant", st.tenantID))

		if cl.Properties.ProtocolVersion == 5 && pk.FixedHeader.Qos == 1 {
			return pk, packets.ErrQuotaExceeded
		}

		return pk, packets.ErrRejectPacket
	}

	st.counters.in.Add(1)
	h.b.deps.Metrics.MessageIn()

	return pk, nil
}

// lastHook is added after every other hook, storage included. Mochi runs
// a hook event on the hooks in the order they were added, so its
// OnDisconnect is the last hook mochi runs for a connection that ends: when
// it drops the connection's state, the storage hook has already written the
// session's final record. DeleteSession relies on that (clientState.ended)
// before it deletes the record.
type lastHook struct {
	mqtt.HookBase

	b *Broker
}

// ID implements mqtt.Hook.
func (h *lastHook) ID() string { return "tether-last" }

// Provides implements mqtt.Hook.
func (h *lastHook) Provides(b byte) bool { return b == mqtt.OnDisconnect }

// OnDisconnect releases the connection slot and drops the state.
func (h *lastHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	if st := h.b.state.remove(cl); st != nil {
		st.releaseSlot()
	}
}

// sessionExpiry is how long, in seconds, mochi keeps cl's session after its
// connection ends: for 5.0 the interval the CONNECT asked for, capped at
// maxExpiry as mochi caps it in the CONNACK, and for 3.1.1 none for a clean
// session and maxExpiry otherwise (mochi expires those after its maximum).
// Read it only from the client's own goroutine or once the client is closed:
// mochi changes these properties while the connection is open.
func sessionExpiry(cl *mqtt.Client, maxExpiry uint32) uint32 {
	if cl.Properties.ProtocolVersion == 5 {
		return min(cl.Properties.Props.SessionExpiryInterval, maxExpiry)
	}

	if cl.Properties.Clean {
		return 0
	}

	return maxExpiry
}

// OnACLCheck decides PUBLISH (write) and SUBSCRIBE or delivery (read) access
// with the client's compiled grants. Names are namespaced; anything outside
// the client's tenant is denied, and so are names that fail topic.Valid.
//
// A disconnected client with a kept session has no connection state. For a
// read (mochi queueing a delivery for the offline session) its tenant comes
// from its namespaced ID and only the namespace is checked: the subscription
// itself passed the grants when it was made.
func (h *hook) OnACLCheck(cl *mqtt.Client, name string, write bool) bool {
	if obs := h.b.aclObserver; obs != nil {
		obs(cl.ID, name, write)
	}

	if cl.Net.Inline {
		return true
	}

	if h.aclCheck(cl, name, write) {
		return true
	}

	h.b.deps.Metrics.ACLDenied(aclOp(write))

	return false
}

// aclCheck is OnACLCheck for a network client.
func (h *hook) aclCheck(cl *mqtt.Client, name string, write bool) bool {
	st := h.b.state.get(cl)
	if st == nil {
		if write || !cl.Closed() {
			return false
		}

		tenantID, ok := tenantOfID(cl.ID)
		if !ok {
			return false
		}

		_, ok = h.b.relative(tenantID, name, true)

		return ok
	}

	if write {
		rel, ok := h.b.relative(st.tenantID, name, false)
		if !ok {
			// Another tenant's name, or mochi's own global $SYS/...
			return false
		}

		return canPublish(st.acl(), rel)
	}

	return h.b.canRead(st.tenantID, st.acl(), name)
}

// canRead is the read check of OnACLCheck for a live connection of tenantID
// with grants set: name (a namespaced filter, or the topic of a delivery)
// must be in the tenant's namespace and either be the tenant's own $SYS
// (namespaced "$SYS/<tenant>/...", readable by every client of the tenant,
// grants or not) or be allowed by the grants. Pruning a session uses the
// same check, so a subscription survives a reload exactly when a new
// SUBSCRIBE for it would pass.
func (b *Broker) canRead(tenantID string, set *acl.Set, name string) bool {
	rel, ok := b.relative(tenantID, name, true)
	if !ok {
		// Another tenant's name, or mochi's own global $SYS/...
		return false
	}

	if isSys(rel) {
		return true
	}

	return set.CanSubscribe(rel)
}

// OnWill checks a will against the client's current grants as mochi is about
// to publish it. The grants may have lost the will's topic since CONNECT
// (Broker.ReloadRole, ReloadPrincipal), and a will skips the publish ACL
// check. A will that no longer passes, or a client with no state, gets a
// will on withheldTopic with nothing in it, which no tenant's filter
// matches and no ACL check lets through: mochi ignores an error from this
// hook and cannot be told to drop the will.
func (h *hook) OnWill(cl *mqtt.Client, will mqtt.Will) (mqtt.Will, error) {
	st := h.b.state.get(cl)
	if st != nil {
		if rel, ok := topic.Strip(st.tenantID, will.TopicName); ok && canPublish(st.acl(), rel) {
			return will, nil
		}
	}

	h.b.log.Info("tether: will withheld, its topic is no longer granted", slog.String("client", cl.ID))

	return mqtt.Will{Flag: will.Flag, TopicName: withheldTopic}, nil
}

// isSys reports whether a tenant-relative name or filter is under "$SYS",
// exactly as the broker publishes it.
func isSys(rel string) bool {
	return rel == sysPrefix || strings.HasPrefix(rel, sysPrefix+"/")
}

// relative turns a namespaced name or filter back into the tenant-relative
// form the grants use: it strips the tenant, then for a filter the
// "$share/<group>/" prefix, and validates what is left.
func (b *Broker) relative(tenantID, name string, isFilter bool) (string, bool) {
	rel, ok := topic.Strip(tenantID, name)
	if !ok {
		return "", false
	}

	if isFilter {
		rel = withoutShare(rel)
	}

	if topic.Valid(rel, isFilter, b.cfg.MaxTopicLength) != nil {
		return "", false
	}

	return rel, true
}

// tenantOf returns the client's tenant and its counters, from the client's
// state or its namespaced ID.
func (h *hook) tenantOf(cl *mqtt.Client) (string, *tenantCounters, bool) {
	if st := h.b.state.get(cl); st != nil {
		return st.tenantID, st.counters, true
	}

	tenantID, ok := tenantOfID(cl.ID)
	if !ok {
		return "", nil, false
	}

	return tenantID, h.b.countersFor(tenantID), true
}

// withoutShare returns the filter part of "$share/<group>/<filter>", the
// empty string for a malformed shared filter, and anything else unchanged.
func withoutShare(filter string) string {
	first, rest, ok := strings.Cut(filter, "/")
	if !strings.EqualFold(first, "$share") {
		return filter
	}

	if !ok {
		return ""
	}

	_, f, ok := strings.Cut(rest, "/")
	if !ok {
		return ""
	}

	return f
}

// canPublish is the publish check shared by PUBLISH and the will: the grants
// allow the name and it is not one mochi reserves.
func canPublish(set *acl.Set, name string) bool {
	if len(name) >= len(sysPrefix) && strings.EqualFold(name[:len(sysPrefix)], sysPrefix) {
		return false
	}

	return set.CanPublish(name)
}
