package extension

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/chronicle"
	chronicleaudit "github.com/xraph/chronicle/audit"
	chroniclestore "github.com/xraph/chronicle/store"
	"github.com/xraph/chronicle/store/memory"

	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

func TestChronicleSinkRecords(t *testing.T) {
	c, err := chronicle.New(chronicle.WithStore(chroniclestore.NewAdapter(memory.New())))
	if err != nil {
		t.Fatal(err)
	}

	sink := chronicleSink{e: c}
	tn := audit.Tenant{ID: "ttnt_x", AppID: "app_1", ExternalID: "org_1"}
	at := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

	err = sink.Record(t.Context(), audit.Event{
		Action: audit.ActionACLDenied, Outcome: audit.OutcomeDenied, TenantID: "ttnt_x", ClientID: "c1",
		Subject: "akey_1", Node: "n1", At: at,
		Meta: map[string]any{audit.MetaOp: "publish", audit.MetaTopic: "a/b"},
	}, tn)
	if err != nil {
		t.Fatal(err)
	}

	err = sink.Record(t.Context(), audit.Event{
		Action: audit.ActionDeviceRevoke, Outcome: audit.OutcomeSuccess, TenantID: "ttnt_x", Actor: "op-1",
		Subject: "tdev_1", Node: "single", Meta: map[string]any{audit.MetaTarget: "tdev_1"},
	}, tn)
	if err != nil {
		t.Fatal(err)
	}

	res, err := c.Query(t.Context(), &chronicleaudit.Query{AppID: "app_1", TenantID: "org_1", Order: "asc", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Events) != 2 {
		t.Fatalf("got %d events, want 2", len(res.Events))
	}

	deny, admin := res.Events[0], res.Events[1]

	checks := []struct{ got, want any }{
		{deny.Action, audit.ActionACLDenied},
		{deny.Resource, "mqtt.client"},
		{deny.Category, "tether"},
		{deny.ResourceID, "c1"},
		{deny.AppID, "app_1"},
		{deny.TenantID, "org_1"},
		{deny.SubjectID, "akey_1"},
		{deny.Outcome, audit.OutcomeDenied},
		{deny.Severity, chronicleaudit.SeverityWarning},
		{deny.Metadata["tether_tenant"], "ttnt_x"},
		{deny.Metadata["node"], "n1"},
		{deny.Metadata["topic"], "a/b"},
		{deny.Timestamp.Equal(at), true},
		{admin.Resource, "tether.device"},
		{admin.ResourceID, "tdev_1"},
		{admin.UserID, "op-1"},
		{admin.Severity, chronicleaudit.SeverityInfo},
	}

	for i, c := range checks {
		if c.got != c.want {
			t.Errorf("check %d: got %v, want %v", i, c.got, c.want)
		}
	}
}

func TestResourceOf(t *testing.T) {
	for action, want := range map[string]string{
		audit.ActionConnect:        "mqtt.client",
		audit.ActionTakeover:       "mqtt.client",
		audit.ActionTenantCreate:   "tether.tenant",
		audit.ActionRoleTopics:     "tether.role",
		audit.ActionSessionDelete:  "tether.session",
		audit.ActionRetainedDelete: "tether.retained",
	} {
		if got := resourceOf(action); got != want {
			t.Errorf("resourceOf(%q) = %q, want %q", action, got, want)
		}
	}
}

func TestAuditTenantsFromCache(t *testing.T) {
	tn := &tenant.Tenant{ID: id.NewTenantID(), AppID: "app_1", ExternalID: "org_1"}
	c := newTenantCache(func(_ context.Context, tid id.TenantID) (*tenant.Tenant, error) {
		if tid != tn.ID {
			t.Fatalf("lookup of %s", tid)
		}

		return tn, nil
	})

	got, err := auditTenants{c: c}.Get(t.Context(), tn.ID.String())
	if err != nil {
		t.Fatal(err)
	}

	want := audit.Tenant{ID: tn.ID.String(), AppID: "app_1", ExternalID: "org_1"}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
