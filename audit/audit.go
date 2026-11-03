// Package audit records what happens to MQTT connections and admin state in
// an audit trail: connects, disconnects, refused CONNECTs, ACL denials,
// session takeovers and every admin change. Never single messages.
//
// The broker and the REST and dashboard layers call an Emitter, which must
// never block them. Recorder is that Emitter: a bounded queue in front of a
// few writers that hand each event to a Sink (chronicle, in the extension).
// A full queue drops the event and counts it, so a tenant that floods the
// broker with refused CONNECTs or denied PUBLISHes cannot slow anyone down.
//
// The package imports no other Tether package: the broker imports it, and
// metering imports the broker.
package audit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Outcomes of an event.
const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
	OutcomeDenied  = "denied"
)

// MQTT actions.
const (
	ActionConnect    = "mqtt.connect"
	ActionDisconnect = "mqtt.disconnect"
	ActionAuthFailed = "mqtt.auth.failed"
	ActionACLDenied  = "mqtt.acl.denied"
	ActionTakeover   = "mqtt.session.takeover"
)

// Admin actions, one per state-changing REST route and dashboard command:
// admin.<resource>.<verb>.
const (
	ActionTenantCreate    = "admin.tenant.create"
	ActionTenantUpdate    = "admin.tenant.update"
	ActionTenantDelete    = "admin.tenant.delete"
	ActionDeviceProvision = "admin.device.provision"
	ActionDeviceRotate    = "admin.device.rotate"
	ActionDeviceRevoke    = "admin.device.revoke"
	ActionDeviceDelete    = "admin.device.delete"
	ActionDeviceRoles     = "admin.device.roles"
	ActionRoleCreate      = "admin.role.create"
	ActionRoleDelete      = "admin.role.delete"
	ActionRoleTopics      = "admin.role.topics"
	ActionClientKick      = "admin.client.kick"
	ActionSessionDelete   = "admin.session.delete"
	ActionRetainedDelete  = "admin.retained.delete"
)

// Event kinds, what Config.Events lists.
const (
	KindConnect     = "connect"
	KindDisconnect  = "disconnect"
	KindAuthFailure = "auth_failure"
	KindACLDeny     = "acl_deny"
	KindTakeover    = "takeover"
	KindAdmin       = "admin"
)

// Meta keys the emitters and the Recorder share.
const (
	// MetaOp is the operation of an ACL denial: "publish" or "subscribe".
	MetaOp = "op"
	// MetaTopic is the tenant-relative topic of a denied PUBLISH or the
	// filter of a denied SUBSCRIBE.
	MetaTopic = "topic"
	// MetaSuppressed is the number of identical ACL denials the Recorder
	// counted, and did not record, within one DenyWindow.
	MetaSuppressed = "suppressed"
	// MetaTarget is what an admin change acted on, when the event's
	// ClientID does not say: a tenant, device or role ID, or a topic.
	MetaTarget = "target"
	// MetaAppID and MetaExternalID carry the tenant's host pair on an
	// event whose tenant may be gone, or not yet in the cache, when a
	// writer looks it up (admin.tenant.create and admin.tenant.delete).
	// With both set the Recorder records them and skips the lookup.
	MetaAppID      = "app_id"
	MetaExternalID = "external_id"
)

// Target is the Meta of an admin event whose target is not a client: a
// tenant, device or role ID, or a topic.
func Target(target string) map[string]any {
	return map[string]any{MetaTarget: target}
}

// Event is one thing that happened.
type Event struct {
	// Action is one of the Action constants.
	Action string
	// Outcome is OutcomeSuccess, OutcomeFailure or OutcomeDenied.
	Outcome string
	// TenantID is the Tether tenant the event belongs to.
	TenantID string
	// ClientID is the MQTT client ID as the client sent it, without the
	// tenant prefix.
	ClientID string
	// Subject is who the event is about: the MQTT principal (keysmith key
	// ID or token subject), or the device an admin change acted on.
	Subject string
	// Actor is who made an admin change: the subject of the REST caller's
	// auth context, or the dashboard user.
	Actor string
	// Reason says why: the AuthReason of a refused CONNECT, the error of a
	// failed admin change.
	Reason string
	// Node is the broker node the event happened on.
	Node string
	// Meta carries the details of each action.
	Meta map[string]any
	// At is when it happened. Emit stamps it when it is zero.
	At time.Time
}

// Emitter takes events. Emit must never block and never wait on the sink.
type Emitter interface {
	Emit(ev Event)
}

// Tenant is the part of a tenant the sink records: the Tether ID and the
// host application's (app, external) pair.
type Tenant struct {
	ID         string
	AppID      string
	ExternalID string
}

// Tenants looks a tenant up by its Tether ID. The extension serves it from
// the same 30 s cache metering uses.
type Tenants interface {
	Get(ctx context.Context, tenantID string) (Tenant, error)
}

// Sink records one event of tenant t. A Recorder writer calls it, never the
// broker.
type Sink interface {
	Record(ctx context.Context, ev Event, t Tenant) error
}

// Kind is the Config.Events kind of action, or "" for an unknown action.
func Kind(action string) string {
	switch action {
	case ActionConnect:
		return KindConnect
	case ActionDisconnect:
		return KindDisconnect
	case ActionAuthFailed:
		return KindAuthFailure
	case ActionACLDenied:
		return KindACLDeny
	case ActionTakeover:
		return KindTakeover
	}

	if strings.HasPrefix(action, "admin.") {
		return KindAdmin
	}

	return ""
}

// kinds lists every kind Config.Events may name.
var kinds = []string{KindConnect, KindDisconnect, KindAuthFailure, KindACLDeny, KindTakeover, KindAdmin}

// Config configures a Recorder.
type Config struct {
	// Enabled turns auditing on. Off, Emit does nothing.
	Enabled bool `json:"enabled" yaml:"enabled"`
	// Events are the kinds recorded. Empty means every kind.
	Events []string `json:"events" yaml:"events"`
	// QueueSize is how many events wait for a writer before Emit drops.
	QueueSize int `json:"queue_size" yaml:"queue_size"`
	// Writers is how many goroutines hand events to the sink.
	Writers int `json:"writers" yaml:"writers"`
	// DenyWindow is how long one ACL denial of a client, operation and
	// topic is recorded only once.
	DenyWindow time.Duration `json:"deny_window" yaml:"deny_window"`
}

// Defaults.
const (
	DefaultQueueSize  = 10000
	DefaultWriters    = 4
	DefaultDenyWindow = time.Minute
)

// DefaultConfig is off, every kind, a queue of 10000, 4 writers and a deny
// window of a minute.
func DefaultConfig() Config {
	return Config{
		Events:     append([]string(nil), kinds...),
		QueueSize:  DefaultQueueSize,
		Writers:    DefaultWriters,
		DenyWindow: DefaultDenyWindow,
	}
}

// Validate refuses an unknown kind and negative sizes.
func (c Config) Validate() error {
	for _, k := range c.Events {
		known := false

		for _, want := range kinds {
			if k == want {
				known = true

				break
			}
		}

		if !known {
			return fmt.Errorf("tether/audit: unknown event kind %q (want one of %s)", k, strings.Join(kinds, ", "))
		}
	}

	switch {
	case c.QueueSize < 0:
		return errors.New("tether/audit: queue_size must not be negative")
	case c.Writers < 0:
		return errors.New("tether/audit: writers must not be negative")
	case c.DenyWindow < 0:
		return errors.New("tether/audit: deny_window must not be negative")
	}

	return nil
}

// withDefaults fills the zero values.
func (c Config) withDefaults() Config {
	if len(c.Events) == 0 {
		c.Events = append([]string(nil), kinds...)
	}

	if c.QueueSize <= 0 {
		c.QueueSize = DefaultQueueSize
	}

	if c.Writers <= 0 {
		c.Writers = DefaultWriters
	}

	if c.DenyWindow <= 0 {
		c.DenyWindow = DefaultDenyWindow
	}

	return c
}
