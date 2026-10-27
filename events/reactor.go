package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/xraph/warden"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
)

// BrokerControl is the part of *broker.Broker the reactor drives.
type BrokerControl interface {
	DisconnectPrincipal(principalID string, code packets.Code) int
	ExpirePrincipal(principalID string, connectedBefore, at time.Time) int
	ReloadRole(ctx context.Context, roleID string)
	ReloadPrincipal(ctx context.Context, principalID string)
}

// Bus is the cell's control bus a clustered reactor publishes its events to
// and applies them from. *cluster.Control satisfies it.
type Bus interface {
	Publish(ctx context.Context, kind string, v any) error
	Subscribe(kind string, fn func([]byte)) error
}

const (
	// publishWait bounds a publish to the bus. The plugins call the reactor
	// from inside keysmith's and warden's admin calls, which wait for it.
	publishWait = 5 * time.Second

	// applyWait bounds a reload an event from the bus runs (the PermLoader
	// calls); a reload that fails on an outage retries in the broker.
	applyWait = 30 * time.Second
)

// reactor is the Handler NewReactor and NewClusterReactor return.
type reactor struct {
	evict func(keyID string)
	b     BrokerControl

	// bus is nil for NewReactor: every change is applied here at once.
	bus Bus
	log *slog.Logger
}

// NewReactor returns the Handler that applies key and role changes to a
// broker. evict is the authenticator's cache eviction ((*auth.Keysmith).Evict)
// and may be nil.
//
//   - KeyInvalidated with disconnect: evict the key, then disconnect every
//     connection authenticated with it (0x98 administrative action).
//   - KeyInvalidated without disconnect: evict the key only.
//   - KeyRotated (a rotation with a grace window): evict the key, then end
//     every connection made before rotatedAt, which covers all that used
//     the previous secret, when the grace window closes (ExpirePrincipal;
//     0xA0 then).
//     Connections made with the new secret stay, and one made with the
//     previous secret during the window already expires with it, because
//     keysmith says so when it validates (auth.Keysmith).
//   - RolesChanged: reload the connections whose grants came from the role.
//   - SubjectChanged for an "api_key", "user" or "service_acct" subject:
//     reload that principal's connections. Other subject kinds are ignored,
//     since only those connect.
//
// Eviction comes first, so a client that reconnects after the disconnect is
// validated against keysmith, not the cache. Empty IDs are ignored.
func NewReactor(evict func(keyID string), b BrokerControl) Handler {
	return &reactor{evict: evict, b: b}
}

// NewClusterReactor is NewReactor for a broker that is one node of a cell:
// every change goes to every node through bus, and each node, this one
// included, applies it once when the event comes back (ruling R41). The
// change reaches the clients of every node, and every node's authenticator
// cache drops the key.
//
// The Handler methods publish the change as a cluster event (key.invalidated,
// role.changed, subject.changed) and return once the bus has stored it,
// within publishWait (5 s). If the publish fails, the change is applied on
// this node as NewReactor would, logged at Warn on log (slog.Default when
// nil), and sent again in the background until the bus takes it (ruling
// R51; cluster.Republish: until it lands, the bus stops, or an hour has
// passed), so the other nodes get it once the bus is back. This node then
// applies it a second time, which is harmless.
//
// NewClusterReactor subscribes its handlers on bus, so call it before the
// bus starts. An applied event evicts the key at once, on the bus's
// goroutine, and runs the broker call (disconnect, expiry, reload) on a
// goroutine of its own, so a slow reload never holds up the bus. A
// key.invalidated event with Disconnect is a revocation; one with RotatedAt
// or GraceEnds set is a rotation with a grace window; one with neither only
// evicts. The error is the bus's Subscribe error.
func NewClusterReactor(evict func(keyID string), b BrokerControl, bus Bus, log *slog.Logger) (Handler, error) {
	if bus == nil {
		return nil, fmt.Errorf("tether/events: cluster reactor: nil bus: %w", tether.ErrInvalidInput)
	}

	if log == nil {
		log = slog.Default()
	}

	r := &reactor{evict: evict, b: b, bus: bus, log: log}

	subs := []struct {
		kind string
		fn   func([]byte)
	}{
		{cluster.KindKeyInvalidated, r.onKey},
		{cluster.KindRoleChanged, r.onRole},
		{cluster.KindSubjectChanged, r.onSubject},
	}

	for _, s := range subs {
		if err := bus.Subscribe(s.kind, s.fn); err != nil {
			return nil, fmt.Errorf("tether/events: cluster reactor: %w", err)
		}
	}

	return r, nil
}

// EvictTenantsOnUpdate subscribes evict to the bus's tenant.updated events,
// so every node of the cell drops its cached copy of a tenant that changed
// (service.Tenants.Evict). The broker applies the change itself
// (broker.Cluster.Control); subscribe this before the broker's New, so the
// eviction runs first, and a client the change disconnects cannot reconnect
// with the old tenant from the cache. evict runs on the bus's goroutine and
// must be quick.
func EvictTenantsOnUpdate(bus Bus, evict func(tenantID string)) error {
	if bus == nil || evict == nil {
		return fmt.Errorf("tether/events: tenant eviction: nil bus or func: %w", tether.ErrInvalidInput)
	}

	return bus.Subscribe(cluster.KindTenantUpdated, func(data []byte) {
		var ev cluster.TenantUpdated
		if err := json.Unmarshal(data, &ev); err != nil || ev.TenantID == "" {
			return
		}

		evict(ev.TenantID)
	})
}

func (r *reactor) KeyInvalidated(ctx context.Context, keyID string, disconnect bool) {
	if keyID == "" {
		return
	}

	if r.publish(ctx, cluster.KindKeyInvalidated, cluster.KeyInvalidated{KeyID: keyID, Disconnect: disconnect}) {
		return
	}

	if r.evict != nil {
		r.evict(keyID)
	}

	if disconnect {
		r.b.DisconnectPrincipal(keyID, packets.ErrAdministrativeAction)
	}
}

func (r *reactor) KeyRotated(ctx context.Context, keyID string, rotatedAt, graceEnds time.Time) {
	if keyID == "" {
		return
	}

	if r.publish(ctx, cluster.KindKeyInvalidated, cluster.KeyInvalidated{KeyID: keyID, RotatedAt: rotatedAt, GraceEnds: graceEnds}) {
		return
	}

	if r.evict != nil {
		r.evict(keyID)
	}

	r.b.ExpirePrincipal(keyID, rotatedAt, graceEnds)
}

func (r *reactor) RolesChanged(ctx context.Context, roleID string) {
	if roleID == "" {
		return
	}

	if r.publish(ctx, cluster.KindRoleChanged, cluster.RoleChanged{RoleID: roleID}) {
		return
	}

	r.b.ReloadRole(ctx, roleID)
}

func (r *reactor) SubjectChanged(ctx context.Context, subjectKind, subjectID string) {
	if subjectID == "" || !connects(subjectKind) {
		return
	}

	if r.publish(ctx, cluster.KindSubjectChanged, cluster.SubjectChanged{Kind: subjectKind, ID: subjectID}) {
		return
	}

	r.b.ReloadPrincipal(ctx, subjectID)
}

// publish sends v to the bus and reports whether it did. Without a bus it
// reports false; on an error it logs at Warn and reports false, and the
// caller applies the change here. ctx's cancellation does not stop the
// publish: the change is stored already.
//
// Ruling R51: a publish that failed is also sent again in the background
// (republish) until the bus takes it, so the rest of the cell gets the
// change once the bus is back. A control stream election (4 to 9 s) outlasts
// nats.go's own retries, and without this a revocation made during one
// reached only this node.
func (r *reactor) publish(ctx context.Context, kind string, v any) bool {
	if r.bus == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishWait)
	defer cancel()

	err := r.bus.Publish(ctx, kind, v)
	if err == nil {
		return true
	}

	r.log.Warn("tether: could not send a change to the cell, applying it on this node and sending it again in the background",
		slog.String("kind", kind), slog.Any("error", err))

	if !errors.Is(err, tether.ErrInvalidInput) {
		go r.republish(kind, v)
	}

	return false
}

// republish sends an event whose publish failed again until the bus takes
// it (cluster.Republish): until it lands, the bus stops (a bus with a
// Done channel, as *cluster.Control has), or the control stream's hour has
// passed. This node applies the event a second time when it comes back;
// events are idempotent.
func (r *reactor) republish(kind string, v any) {
	var done <-chan struct{}
	if d, ok := r.bus.(interface{ Done() <-chan struct{} }); ok {
		done = d.Done()
	}

	err := cluster.Republish(done, func(ctx context.Context) error { return r.bus.Publish(ctx, kind, v) })
	if err != nil {
		r.log.Warn("tether: gave up sending a change to the cell; other nodes' clients get it when they reconnect",
			slog.String("kind", kind), slog.Any("error", err))

		return
	}

	r.log.Info("tether: change sent to the cell after the bus came back", slog.String("kind", kind))
}

// decode decodes an event from the bus, logging one it cannot read.
func (r *reactor) decode(kind string, data []byte, v any) bool {
	if err := json.Unmarshal(data, v); err != nil {
		r.log.Warn("tether: undecodable control event", slog.String("kind", kind), slog.Any("error", err))

		return false
	}

	return true
}

// onKey applies a key.invalidated event: evict now, then the broker call in
// the background.
func (r *reactor) onKey(data []byte) {
	var ev cluster.KeyInvalidated
	if !r.decode(cluster.KindKeyInvalidated, data, &ev) || ev.KeyID == "" {
		return
	}

	if r.evict != nil {
		r.evict(ev.KeyID)
	}

	switch {
	case ev.Disconnect:
		go r.b.DisconnectPrincipal(ev.KeyID, packets.ErrAdministrativeAction)
	case !ev.RotatedAt.IsZero() || !ev.GraceEnds.IsZero():
		go r.b.ExpirePrincipal(ev.KeyID, ev.RotatedAt, ev.GraceEnds)
	}
}

// onRole applies a role.changed event in the background.
func (r *reactor) onRole(data []byte) {
	var ev cluster.RoleChanged
	if !r.decode(cluster.KindRoleChanged, data, &ev) || ev.RoleID == "" {
		return
	}

	go r.apply(func(ctx context.Context) { r.b.ReloadRole(ctx, ev.RoleID) })
}

// onSubject applies a subject.changed event in the background.
func (r *reactor) onSubject(data []byte) {
	var ev cluster.SubjectChanged
	if !r.decode(cluster.KindSubjectChanged, data, &ev) || ev.ID == "" || !connects(ev.Kind) {
		return
	}

	go r.apply(func(ctx context.Context) { r.b.ReloadPrincipal(ctx, ev.ID) })
}

// apply runs a reload an event asked for, bounded by applyWait.
func (r *reactor) apply(fn func(ctx context.Context)) {
	ctx, cancel := context.WithTimeout(context.Background(), applyWait)
	defer cancel()

	fn(ctx)
}

// connects reports whether warden subjects of this kind can hold an MQTT
// connection: API keys (devices), users and service accounts (JWT clients).
// It names warden's kinds, which are the ones assignment events carry; see
// perm.SubjectKind for the mapping from Identity.PrincipalKind.
func connects(subjectKind string) bool {
	switch warden.SubjectKind(subjectKind) {
	case warden.SubjectAPIKey, warden.SubjectUser, warden.SubjectServiceAcct:
		return true
	default:
		return false
	}
}
