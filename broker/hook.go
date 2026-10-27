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
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/tenant"
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

// clusterProvided lists the methods the hook implements only in cluster
// mode, where they keep the node's QoS 0 routes (cluster.go), its sessions'
// cursors (cursor.go), its retained cache (clusterretained.go) and its shared
// groups (clustershared.go). Alone, the broker provides exactly the methods
// it always did.
var clusterProvided = []byte{
	mqtt.OnSubscribe,
	mqtt.OnSubscribed,
	mqtt.OnUnsubscribed,
	mqtt.OnClientExpired,
	mqtt.OnSessionEstablished,
	mqtt.OnQosPublish,
	mqtt.OnQosDropped,
	mqtt.OnPublishDropped,
	mqtt.OnSelectSubscribers,
}

// Provides implements mqtt.Hook.
func (h *hook) Provides(b byte) bool {
	if bytes.IndexByte(provided, b) >= 0 {
		return true
	}

	return h.b.cluster != nil && bytes.IndexByte(clusterProvided, b) >= 0
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

	if h.b.cluster != nil {
		// Ruling R13: the cell keys sessions by client ID, which it caps.
		if err := checkClientID(cl.ID); err != nil {
			return h.refuse(cl, log, packets.ErrClientIdentifierNotValid, "client ID the cell cannot carry", err)
		}

		// A fenced node serves nobody until it can reach the cell again.
		if h.b.cluster.fenced.Load() {
			return h.refuse(cl, log, packets.ErrServerUnavailable, "node is fenced from the cell", cluster.ErrFenced)
		}
	}

	ident, g, ok := h.authorize(ctx, cl, pk, log)
	if !ok {
		return false
	}

	tenantID := ident.Tenant.ID.String()
	log = log.With(slog.String("tenant", tenantID))

	if !h.sessionRoom(tenantID, cl.ID, ident.Tenant.Limits.MaxSessions) {
		return h.refuse(cl, log, packets.ErrQuotaExceeded, "tenant session limit reached", nil)
	}

	from, ok := h.acquireSlot(tenantID, cl.ID, ident.Tenant.Limits.MaxConnections)
	if !ok {
		return h.refuse(cl, log, packets.ErrQuotaExceeded, "tenant connection limit reached", nil)
	}

	st := newClientState(cl, cl.ID, string(pk.Connect.Username), ident, ident.Tenant, g, h.b.deps.Limits, h.b.countersFor(tenantID))
	st.metrics = h.b.deps.Metrics
	st.expiryCap = h.b.expiryCap(ident.Tenant.Limits)
	st.sessionExpiry = sessionExpiry(cl, st.expiryCap)
	st.expiryShortened = cl.Properties.ProtocolVersion == 5 && cl.Properties.Props.SessionExpiryInterval > st.expiryCap

	if h.b.cluster != nil {
		st.pipe = newPubPipe(h.b, h.server, cl, st)
	}

	if !h.b.state.addUnlessDeleting(st, namespacedID(tenantID, cl.ID)) {
		st.returnSlot(from)

		return h.refuse(cl, log, packets.ErrServerUnavailable, "session is being deleted", nil)
	}

	if h.b.changes.Load() != gen && !h.reauthorize(ctx, cl, pk, st, log) {
		h.b.state.remove(cl)
		st.returnSlot(from)

		return false
	}

	// The tenant's log reader runs before this connection gets its CONNACK,
	// so none of its SUBSCRIBEs can come before the node's read position.
	if h.b.cluster != nil {
		if err := h.b.ensureReader(tenantID); err != nil {
			h.b.state.remove(cl)
			st.returnSlot(from)

			return h.refuse(cl, log, packets.ErrServerUnavailable, "cluster log unavailable", err)
		}

		// The tenant's retained messages start loading into this node's
		// cache; a SUBSCRIBE waits for them (OnSubscribe).
		h.b.cluster.ensureRetained(tenantID)

		// Last: once the key is this node's, the session's previous owner,
		// anywhere in the cell, is on its way out. The session's lock stays
		// held until OnSessionEstablish is done (st.sessUnlock).
		if !h.b.acquireSession(cl, st, log) {
			h.b.state.remove(cl)
			st.returnSlot(from)

			return h.refuse(cl, log, packets.ErrServerUnavailable, "session unavailable in the cell", nil)
		}
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

	st.setExpiry(time.AfterFunc(time.Until(exp), func() { h.b.expireConnection(h.server, st) }))
}

// sessionRoom reports whether a CONNECT for rawID may hold a session of
// tenantID under its MaxSessions limit (zero is unlimited). A client ID that
// already has a session, open or kept offline, is coming back to it and
// always may. Otherwise the tenant's sessions on this node, open connections
// plus kept offline sessions, must be under the limit.
//
// It walks mochi's clients, so it costs O(clients on the node) per CONNECT,
// and only for tenants that set the limit. Two CONNECTs racing for the last
// session can both pass: the count is read, not reserved.
func (h *hook) sessionRoom(tenantID, rawID string, limit int) bool {
	if limit <= 0 {
		return true
	}

	nsID := namespacedID(tenantID, rawID)
	if _, ok := h.server.Clients.Get(nsID); ok {
		return true
	}

	prefix := tenantID + "/"
	n := 0

	for id, cl := range h.server.Clients.GetAll() {
		if !cl.Net.Inline && strings.HasPrefix(id, prefix) {
			n++
		}
	}

	return n < limit
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
//
// from is the connection an inherited slot came from, so a CONNECT refused
// after this can give it back (clientState.returnSlot); nil for a fresh slot.
func (h *hook) acquireSlot(tenantID, rawID string, maxConns int) (from *clientState, ok bool) {
	if h.acquire(tenantID, maxConns) {
		return nil, true
	}

	if h.b.state.sweepClosed(tenantID) > 0 && h.acquire(tenantID, maxConns) {
		return nil, true
	}

	prev := h.b.state.owner(namespacedID(tenantID, rawID))
	if prev == nil {
		return nil, false
	}

	if prev.transferSlot() {
		return prev, true
	}

	// The old connection released its slot in the meantime.
	return nil, h.acquire(tenantID, maxConns)
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
	case packets.ErrClientIdentifierNotValid:
		return packets.Err3ClientIdentifierNotValid.Code
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

	if st.sessUnlock != nil {
		// Taken in OnConnectAuthenticate with the session's key (cluster
		// mode): held until the connection owns the ID here.
		defer func() {
			st.sessUnlock()
			st.sessUnlock = nil
		}()
	}

	cl.ID = namespacedID(st.tenantID, st.rawID)

	if atomic.LoadUint32(&cl.Properties.Will.Flag) == 1 {
		cl.Properties.Will.TopicName = topic.Namespace(st.tenantID, cl.Properties.Will.TopicName)
	}

	cl.Properties.Props.TopicAliasMaximum = 0

	// A clean start ends the previous session, its QoS 2 state included.
	if h.b.cluster != nil && cl.Properties.Clean {
		h.b.cluster.dropQoS2(cl.ID)
	}

	h.capSessionExpiry(cl, st)

	// bind also abandons the pipe of a connection this one takes over
	// (clientState.closePipe), before mochi clones its inflight map.
	h.b.state.bind(st)
}

// capSessionExpiry holds the session to the connection's expiry cap, the
// lower of the broker's and the tenant's. Mochi caps a 5.0 session at the
// broker's cap alone, so a longer request is lowered here, before mochi
// builds the CONNACK; OnPacketEncode then tells the client. A 3.1.1
// persistent session gets the cap as its expiry (mochi would keep it for the
// broker's cap), and Broker.expireSessions ends it when that runs out: mochi
// only reads the expiry property of 5.0 sessions.
func (h *hook) capSessionExpiry(cl *mqtt.Client, st *clientState) {
	props := &cl.Properties.Props

	switch {
	case cl.Properties.ProtocolVersion == 5:
		if props.SessionExpiryInterval > st.expiryCap {
			props.SessionExpiryInterval = st.expiryCap
			props.SessionExpiryIntervalFlag = true
		}
	case !cl.Properties.Clean:
		props.SessionExpiryInterval = st.expiryCap
		props.SessionExpiryIntervalFlag = true
	}
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

	if n := h.b.cluster; n != nil {
		switch pk.FixedHeader.Type {
		case packets.Pubrel:
			// The end of a QoS 2 publish the pipe acked: its receive slot
			// frees and its pending entry goes. Mochi answers PUBCOMP.
			h.b.pubrel(cl, pk.PacketID)

			return pk, nil
		case packets.Puback, packets.Pubcomp:
			// One of the session's deliveries is complete (cursor.go), or
			// one of its shared ones (clustershared.go).
			n.onAck(cl, pk.PacketID, pk.FixedHeader.Type == packets.Pubcomp)
			n.shareAcked(cl.ID, pk.PacketID, pk.FixedHeader.Type)

			return pk, nil
		case packets.Pubrec:
			n.sharePubrec(cl.ID, pk.PacketID, pk.ReasonCode)

			// The record says PUBREL before mochi sends it.
			if !n.onPubrec(cl, pk) {
				return pk, packets.ErrRejectPacket
			}

			return pk, nil
		}
	}

	switch pk.FixedHeader.Type {
	case packets.Publish, packets.Subscribe, packets.Unsubscribe, packets.Disconnect:
	default:
		return pk, nil
	}

	st := h.b.state.get(cl)
	if st == nil {
		// A clean DISCONNECT still has to reach mochi, or the read ends
		// with an error and mochi publishes the will the client asked it
		// to drop.
		if pk.FixedHeader.Type == packets.Disconnect {
			return pk, nil
		}

		return pk, packets.ErrRejectPacket
	}

	if pk.FixedHeader.Type == packets.Disconnect {
		// A 5.0 client may change its session expiry on the way out. Mochi
		// takes the new value as is, so it is held to the cap here.
		if pk.Properties.SessionExpiryIntervalFlag && pk.Properties.SessionExpiryInterval > st.expiryCap {
			pk.Properties.SessionExpiryInterval = st.expiryCap
		}

		return pk, nil
	}

	if pk.FixedHeader.Type == packets.Publish {
		switch {
		case pk.TopicName == "" && pk.Properties.TopicAliasFlag:
			return h.reject(cl, pk, packets.ErrTopicAliasInvalid)
		case pk.TopicName == "":
			return h.reject(cl, pk, packets.ErrProtocolViolationNoTopic)
		case topic.Valid(pk.TopicName, false, h.b.cfg.MaxTopicLength) != nil:
			return h.reject(cl, pk, packets.ErrTopicNameInvalid)
		case h.b.cluster != nil && subject.CheckTopic(pk.TopicName) != nil:
			// The cell carries topics of at most 1024 bytes, whatever
			// MaxTopicLength allows.
			return h.reject(cl, pk, packets.ErrTopicNameInvalid)
		}

		pk.TopicName = topic.Namespace(st.tenantID, pk.TopicName)

		return pk, nil
	}

	filters := make(packets.Subscriptions, len(pk.Filters))
	copy(filters, pk.Filters)

	for i := range filters {
		if topic.Valid(filters[i].Filter, true, h.b.cfg.MaxTopicLength) != nil ||
			h.b.cluster != nil && subject.CheckTopic(filters[i].Filter) != nil {
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
// (mochi always sends bad username or password). It also keeps the user
// properties on a PUBLISH for a client that asked for no problem information.
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
			st := h.b.state.get(cl)
			if st == nil {
				return pk
			}

			if h.b.cluster != nil && h.b.state.owner(cl.ID) != st {
				// A concurrent CONNECT on this node took the ID after this
				// one inherited (OnSessionEstablished disconnects it): what
				// mochi cloned into it belongs to the other connection's
				// flows, and none of it may be resent from here.
				for _, p := range cl.State.Inflight.GetAll(false) {
					cl.State.Inflight.Delete(p.PacketID)
				}

				return pk
			}

			if h.b.cluster != nil {
				orderResend(cl)
			}

			if n := h.b.cluster; n != nil && !n.mayResend(st) {
				// Another node took the session while this CONNECT was
				// loading it: what it restored must not be resent from here
				// (OnSessionEstablished disconnects it with 0x8E).
				for _, p := range cl.State.Inflight.GetAll(false) {
					cl.State.Inflight.Delete(p.PacketID)
				}

				return pk
			}

			if pk.SessionPresent {
				h.b.pruneSession(h.server, st)
			}

			// capSessionExpiry lowered a 5.0 request below the broker's cap,
			// where mochi says nothing: the client must learn the expiry it
			// actually got.
			if st.expiryShortened {
				pk.Properties.SessionExpiryInterval = st.sessionExpiry
				pk.Properties.SessionExpiryIntervalFlag = true
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

		// A retained message the hook took the flag off (cluster mode)
		// carries it again to a 5.0 subscription with Retain As Published.
		if retainedTag(pk) && cl.Properties.ProtocolVersion == 5 && retainAsPublished(cl, pk.TopicName) {
			pk.FixedHeader.Retain = true
		}

		if n := h.b.cluster; n != nil && pk.FixedHeader.Qos == 2 && !n.beforeQoS2Send(cl, pk) {
			// The delivery is not in the session's record, so it must not
			// go out; beforeQoS2Send stopped the client.
			return blankPublish(pk)
		}

		pk.TopicName = name
		counters.out.Add(1)
		h.b.deps.Metrics.MessageOut()

		// Mochi v2.7.9 sets DisallowProblemInfo for a client that connected
		// with Request Problem Information 0, and then drops the user
		// properties from every packet it sends it, PUBLISH included.
		// paho.golang sends 0 unless you ask for 1. MQTT 5 ([MQTT-3.1.2-29])
		// only restricts the packets other than PUBLISH, CONNACK and
		// DISCONNECT, so the publisher's user properties stay.
		pk.Mods.DisallowProblemInfo = false

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

	return blankPublish(pk)
}

// blankPublish is what a PUBLISH that must not go out becomes once its client
// is stopped: mochi's write of it then fails, and it carries no tenant, no
// topic of one, no payload and no properties.
func blankPublish(pk packets.Packet) packets.Packet {
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
//
// In cluster mode a QoS 0 publish also goes to the Router, and a QoS 1 or 2
// publish goes to the log instead of mochi (clusterlog.go). A retained one is
// written to the cell's retained store and this node's cache, never by mochi
// (clusterretained.go): mochi gets it with its retain flag clear and tagged
// for Retain As Published. A QoS 1 or 2 publish must first fit
// under the server's receive maximum (pubPipe.admit), then the connection's
// pipe appends it and writes the ack once it is committed, and this returns
// ErrRejectPacket so mochi neither acks nor delivers it. The 0x97 PUBACK
// then goes through the pipe too, to keep the acks in order. What the
// injector publishes keeps the publisher's client ID as its origin.
func (h *hook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.Net.Inline {
		// The host's QoS 0 publishes reach the other nodes too. What the
		// injector publishes came from the cell and never goes back.
		if n := h.b.cluster; n != nil {
			if n.isInjector(cl) {
				injectedOrigin(&pk)
			} else {
				if pk.FixedHeader.Retain {
					// The host's publish wrote the cell's store itself
					// (admin.go).
					pk.FixedHeader.Retain = false
					tagRetained(&pk)
				}

				if pk.FixedHeader.Qos == 0 {
					n.publishInline(&pk)
				}
			}
		}

		return pk, nil
	}

	st := h.b.state.get(cl)
	if st == nil {
		return pk, packets.ErrRejectPacket
	}

	rel, ok := topic.Strip(st.tenantID, pk.TopicName)
	if !ok {
		return pk, packets.ErrRejectPacket
	}

	n := h.b.cluster
	qos := pk.FixedHeader.Qos
	logged := n != nil && qos > 0
	v5 := cl.Properties.ProtocolVersion == 5

	if n != nil && !h.aclCheck(cl, pk.TopicName, true) {
		// OnACLCheck let it through (cluster mode), so the refusal is
		// made here, where its ack can wait its turn in the pipe.
		h.b.deps.Metrics.ACLDenied(ACLOpPublish)

		switch {
		case qos == 0:
		case !v5:
			disconnect(h.server, h.b.log, cl, packets.ErrNotAuthorized)
		case st.pipe.admit():
			st.pipe.ackNow(pk.PacketID, qos, packets.ErrNotAuthorized)
		}

		return pk, packets.ErrRejectPacket
	}

	if logged && !st.pipe.admit() {
		return pk, packets.ErrRejectPacket
	}

	if !h.b.deps.Limits.AllowMessage(st.tenantID, st.tenant.Load().Limits) {
		h.b.log.Debug("tether: publish over the tenant's rate limit", slog.String("client", cl.ID), slog.String("tenant", st.tenantID))

		switch {
		case logged && v5:
			// PUBACK or PUBREC 0x97, in order.
			st.pipe.ackNow(pk.PacketID, qos, packets.ErrQuotaExceeded)
		case v5 && qos == 1:
			return pk, packets.ErrQuotaExceeded
		}

		return pk, packets.ErrRejectPacket
	}

	st.counters.in.Add(1)
	h.b.deps.Metrics.MessageIn()

	switch {
	case logged:
		h.b.commitPublish(cl, st, rel, &pk)

		return pk, packets.ErrRejectPacket
	case n != nil:
		// Mochi delivers to this node's subscribers once this returns; the
		// Router takes the message to every other node with interest.
		n.publish(st.tenantID, rel, &pk)

		if pk.FixedHeader.Retain {
			n.retainPublish(st, rel, &pk)
		}
	}

	return pk, nil
}

// OnSubscribe holds a network client's SUBSCRIBE (cluster mode only) until
// its tenant's retained messages are loaded into this node's cache, for up
// to retainedLoadWait, so mochi's retained delivery after the SUBACK reads
// them (awaitRetained). It holds no lock while it waits.
func (h *hook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	if n := h.b.cluster; n != nil && !cl.Net.Inline {
		n.awaitRetained(cl, pk.Filters)
	}

	return pk
}

// OnSubscribed takes a Router reference for each filter mochi accepted from
// a network client's SUBSCRIBE (cluster mode only), with the client's tenant
// and the tenant-relative filter, so QoS 0 messages for it published on
// other nodes come here. A shared filter joins its group instead
// (joinShared, clustershared.go).
//
// Mochi has stored the subscriptions when this runs and writes the SUBACK
// right after, from the same reasonCodes slice. A filter whose Router
// subscribe or group join fails is refused there: its code becomes 0x80
// (unspecified error, also the 3.1.1 failure code), mochi's subscription is
// removed, and mochi skips its retained messages. The failure is logged at
// Warn and counted (routeErrors, shareErrors). A filter the session no
// longer holds once its reference is taken (a prune got there first) gives
// the reference back.
func (h *hook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	n := h.b.cluster
	if n == nil || cl.Net.Inline {
		return
	}

	tenantID, ok := tenantOfID(cl.ID)
	if !ok {
		return
	}

	for i, code := range reasonCodes {
		if i >= len(pk.Filters) || code >= packets.ErrUnspecifiedError.Code {
			continue
		}

		sub := pk.Filters[i]

		rel, ok := routedFilter(tenantID, sub.Filter)
		if !ok {
			if mqtt.IsSharedFilter(sub.Filter) && !h.joinShared(cl, tenantID, sub) {
				reasonCodes[i] = packets.ErrUnspecifiedError.Code
			}

			continue
		}

		err := n.subscribe(cl.ID, tenantID, rel)
		if err == nil {
			// Mochi stored the subscription before calling this, so a prune
			// on another goroutine (a reload) can have removed it in
			// between and found no reference to release. The reference is
			// registered now, so a later prune finds it; this catches an
			// earlier one. release is idempotent, so both may run.
			if _, held := cl.State.Subscriptions.Get(sub.Filter); !held {
				n.release(cl.ID, tenantID, []string{rel})
			}

			continue
		}

		n.routeErrors.Add(1)
		n.log.Warn("tether: subscription refused, its route failed", slog.String("client", cl.ID),
			slog.String("tenant", tenantID), slog.String("filter", rel), slog.Any("error", err))

		reasonCodes[i] = packets.ErrUnspecifiedError.Code

		if h.server.Topics.Unsubscribe(sub.Filter, cl.ID) {
			atomic.AddInt64(&h.server.Info.Subscriptions, -1)
		}

		cl.State.Subscriptions.Delete(sub.Filter)
	}

	if s := n.sessionOf(cl); s != nil {
		s.touch()

		if s.heldOut() {
			// The session is still replaying: its new subscriptions wait
			// out of the index with the rest (cursor.go).
			var held []packets.Subscription

			for i, code := range reasonCodes {
				if i < len(pk.Filters) && code < packets.ErrUnspecifiedError.Code {
					held = append(held, pk.Filters[i])
				}
			}

			n.holdOut(s, cl, held)
		}
	}
}

// OnUnsubscribed drops the Router references and shared group memberships
// of the filters a network client's session no longer holds (cluster mode
// only): after an UNSUBSCRIBE, and when mochi unsubscribes a clean session that ended or was
// taken over. Mochi also calls it for an UNSUBSCRIBE it refused (packet
// identifier in use) and for filters the client never had, so only filters
// gone from the session are released.
func (h *hook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	n := h.b.cluster
	if n == nil || cl.Net.Inline {
		return
	}

	tenantID, ok := tenantOfID(cl.ID)
	if !ok {
		return
	}

	var gone, goneShared []string

	for _, sub := range pk.Filters {
		if _, held := cl.State.Subscriptions.Get(sub.Filter); held {
			continue
		}

		if rel, ok := routedFilter(tenantID, sub.Filter); ok {
			gone = append(gone, rel)
		} else if mqtt.IsSharedFilter(sub.Filter) {
			goneShared = append(goneShared, sub.Filter)
		}
	}

	n.release(cl.ID, tenantID, gone)
	n.leaveShared(cl.ID, goneShared)

	if s := n.sessionOf(cl); s != nil {
		s.unloaded(pk.Filters)
	}
}

// OnSessionEstablished runs after the CONNACK and the resend of the
// session's inflight messages (cluster mode): see Broker.established.
func (h *hook) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	if h.b.cluster == nil || cl.Net.Inline {
		return
	}

	if st := h.b.state.get(cl); st != nil {
		h.b.established(cl, st)
	}
}

// OnQosPublish records the log sequence of a QoS 1 or 2 delivery (cluster
// mode, cursor.go).
func (h *hook) OnQosPublish(cl *mqtt.Client, pk packets.Packet, _ int64, _ int) {
	if n := h.b.cluster; n != nil && !cl.Net.Inline {
		n.onQosPublish(cl, pk)
	}
}

// OnQosDropped forgets a delivery mochi gave up on (cluster mode); a shared
// one is finished (its message expired).
func (h *hook) OnQosDropped(cl *mqtt.Client, pk packets.Packet) {
	if n := h.b.cluster; n != nil && !cl.Net.Inline {
		n.onQosDropped(cl, pk)
		n.shareDropped(cl.ID, pk, false)
	}
}

// OnSelectSubscribers empties mochi's shared selection in cluster mode
// (ruling R35): a shared subscription receives only from its group
// (clustershared.go), never from a message this node injects or a client of
// this node publishes. mochi calls it only when the topic has shared
// subscribers here, and then selects from what this returns.
func (h *hook) OnSelectSubscribers(subs *mqtt.Subscribers, _ packets.Packet) *mqtt.Subscribers {
	if h.b.cluster != nil && subs != nil {
		subs.Shared = map[string]map[string]packets.Subscription{}
		subs.SharedSelected = map[string]packets.Subscription{}
	}

	return subs
}

// OnPublishDropped holds the cursor below a delivery mochi could not queue
// (cluster mode).
func (h *hook) OnPublishDropped(cl *mqtt.Client, pk packets.Packet) {
	if n := h.b.cluster; n != nil && !cl.Net.Inline {
		n.onPublishDropped(cl, pk)
	}
}

// OnClientExpired drops every Router reference of a session mochi expired
// (cluster mode only). Mochi deletes the session from its clients without
// unsubscribing it, so OnUnsubscribed never runs for it.
func (h *hook) OnClientExpired(cl *mqtt.Client) {
	n := h.b.cluster
	if n == nil || cl.Net.Inline {
		return
	}

	if cur, ok := h.server.Clients.Get(cl.ID); ok && cur != cl {
		// Another connection holds the ID now, with its own subscriptions.
		return
	}

	if tenantID, ok := tenantOfID(cl.ID); ok {
		n.releaseAll(cl.ID, tenantID)
	}

	n.dropQoS2(cl.ID)
}

// lastHook is added after every other hook, storage included. Mochi runs
// a hook event on the hooks in the order they were added, so its
// OnDisconnect is the last hook mochi runs for a connection that ends: when
// it drops the connection's state, the storage hook has already written the
// session's final record. DeleteSession relies on that (clientState.ended)
// before it deletes the record.
type lastHook struct {
	mqtt.HookBase

	b      *Broker
	server *mqtt.Server
}

// ID implements mqtt.Hook.
func (h *lastHook) ID() string { return "tether-last" }

// Provides implements mqtt.Hook.
func (h *lastHook) Provides(b byte) bool { return b == mqtt.OnDisconnect }

// OnDisconnect releases the connection slot and drops the state. In cluster
// mode it also abandons the connection's pending QoS 1 and 2 publishes
// (unacked; the client sends them again) and, for a session that ends with
// the connection, drops its pending QoS 2 entries, unless another connection
// holds the session now.
func (h *lastHook) OnDisconnect(cl *mqtt.Client, _ error, expire bool) {
	if hold := h.b.disconnectHold; hold != nil {
		hold(cl, false)
	}

	// remove closes the pipe before the state leaves the ID's ownership
	// (clientState.closePipe).
	st := h.b.state.remove(cl)

	if hold := h.b.disconnectHold; hold != nil {
		hold(cl, true)
	}

	if st != nil {
		st.releaseSlot()
	}

	if n := h.b.cluster; n != nil && !cl.Net.Inline {
		if expire {
			if cur, ok := h.server.Clients.Get(cl.ID); !ok || cur == cl {
				n.dropQoS2(cl.ID)
			}
		}

		// The session ends here, unless another connection holds it now:
		// deleted, written one last time and evicted, or left alone when
		// another node took it (clustersession.go).
		h.b.endSession(cl, expire)
	}
}

// sessionExpiry is how long, in seconds, cl's session is kept after its
// connection ends: for 5.0 the interval the client asked for, capped at
// maxExpiry, and for 3.1.1 none for a clean session and otherwise the
// expiry capSessionExpiry gave it, or maxExpiry for a session it never saw.
// Read it only from the client's own goroutine or once the client is closed:
// mochi changes these properties while the connection is open.
func sessionExpiry(cl *mqtt.Client, maxExpiry uint32) uint32 {
	props := cl.Properties.Props

	switch {
	case cl.Properties.ProtocolVersion == 5:
		return min(props.SessionExpiryInterval, maxExpiry)
	case cl.Properties.Clean:
		return 0
	case props.SessionExpiryIntervalFlag:
		return min(props.SessionExpiryInterval, maxExpiry)
	default:
		return maxExpiry
	}
}

// expiryCap is the longest session, in seconds, a connection of a tenant
// with limits l may keep: the broker's cap, or the tenant's when it is set
// and lower.
func (b *Broker) expiryCap(l tenant.Limits) uint32 {
	capped := b.cfg.sessionExpirySeconds()

	if l.MaxSessionExpiry > 0 {
		capped = min(capped, durationSeconds(l.MaxSessionExpiry))
	}

	return capped
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

	// In cluster mode OnPublish decides a network client's PUBLISH, so a
	// refused one is acked in order through the pipe (mochi would write
	// its 0x87 at once). Mochi asks for write access only in
	// processPublish, right before OnPublish.
	if write && h.b.cluster != nil {
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
//
// In cluster mode a will that passes goes to the cell (Broker.clusterWill,
// ruling R30), and mochi gets back either the will, for its local delivery
// of a QoS 0 one, or the withheld will.
func (h *hook) OnWill(cl *mqtt.Client, will mqtt.Will) (mqtt.Will, error) {
	st := h.b.state.get(cl)
	if st != nil {
		if rel, ok := topic.Strip(st.tenantID, will.TopicName); ok && canPublish(st.acl(), rel) {
			if h.b.cluster != nil && !cl.Net.Inline {
				return h.b.clusterWill(cl, st, rel, will), nil
			}

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
