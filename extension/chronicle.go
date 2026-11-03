package extension

import (
	"context"
	"maps"
	"strings"

	"github.com/xraph/chronicle"
	chronicleaudit "github.com/xraph/chronicle/audit"

	"github.com/xraph/tether/audit"
)

// auditCategory is the chronicle category of every Tether event.
const auditCategory = "tether"

// mqttResource is the chronicle resource of the mqtt.* events.
const mqttResource = "mqtt.client"

// chronicleSink records audit events in chronicle, one chronicle event per
// Tether event. chronicle's Record is synchronous (one hash-chained store
// write), so only the audit.Recorder's writers call it.
type chronicleSink struct {
	e chronicle.Emitter
}

var _ audit.Sink = chronicleSink{}

// Record writes ev under the tenant's host pair: TenantID is its ExternalID
// and AppID its app, the same pair ledger bills. UserID is who acted (the
// admin caller), SubjectID who the event is about.
func (s chronicleSink) Record(ctx context.Context, ev audit.Event, t audit.Tenant) error {
	return s.e.Record(ctx, toChronicle(ev, t))
}

// toChronicle maps a Tether event to chronicle's.
func toChronicle(ev audit.Event, t audit.Tenant) *chronicleaudit.Event {
	meta := make(map[string]any, len(ev.Meta)+2)
	maps.Copy(meta, ev.Meta)
	meta["tether_tenant"] = ev.TenantID
	meta["node"] = ev.Node

	resourceID := ev.ClientID
	if target, ok := ev.Meta[audit.MetaTarget].(string); ok && resourceID == "" {
		resourceID = target
	}

	return &chronicleaudit.Event{
		Timestamp:  ev.At,
		AppID:      t.AppID,
		TenantID:   t.ExternalID,
		UserID:     ev.Actor,
		SubjectID:  ev.Subject,
		Action:     ev.Action,
		Resource:   resourceOf(ev.Action),
		Category:   auditCategory,
		ResourceID: resourceID,
		Metadata:   meta,
		Outcome:    ev.Outcome,
		Severity:   severityOf(ev.Outcome),
		Reason:     ev.Reason,
	}
}

// resourceOf is mqtt.client for an mqtt.* action and tether.<resource> for
// admin.<resource>.<verb>.
func resourceOf(action string) string {
	rest, ok := strings.CutPrefix(action, "admin.")
	if !ok {
		return mqttResource
	}

	res, _, _ := strings.Cut(rest, ".")

	return "tether." + res
}

// severityOf is warning for a failure or a denial, info otherwise.
func severityOf(outcome string) string {
	if outcome == audit.OutcomeFailure || outcome == audit.OutcomeDenied {
		return chronicleaudit.SeverityWarning
	}

	return chronicleaudit.SeverityInfo
}

// auditTenants adapts the shared tenant cache to audit.Tenants. One cache
// serves metering (meteringTenants) and audit; one type cannot have both
// Get methods.
type auditTenants struct {
	c *tenantCache
}

var _ audit.Tenants = auditTenants{}

// Get returns the tenant's Tether ID and host pair.
func (a auditTenants) Get(ctx context.Context, tenantID string) (audit.Tenant, error) {
	ref, err := a.c.lookup(ctx, tenantID)
	if err != nil {
		return audit.Tenant{}, err
	}

	return audit.Tenant{ID: ref.ID, AppID: ref.AppID, ExternalID: ref.ExternalID}, nil
}
