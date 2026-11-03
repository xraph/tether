// Package contract publishes Tether's admin operations to the Forge
// dashboard as a contract contributor: a declarative manifest plus typed
// handlers that call the same services the REST API in package api does.
//
// The extension registers it from RegisterContractContributor. Every intent
// is admitted only for a principal with an authenticated user (set
// Deps.AllowAnonymous to turn that off), because the dashboard's auth is off
// unless the host enables it.
//
// A signed-in user is then confined to their own tenant: the one their
// session names, through a tenant_id claim or the org of the forge Scope the
// host's auth middleware sets, mapped to a Tether tenant. Naming another
// tenant is PERMISSION_DENIED, leaving tenantId out means their own, and a
// user with no tenant is refused. Deps.Operators lifts that: an operator may
// act on any tenant and manage tenants themselves. session.info tells the
// plugin which of the two the caller is.
package contract

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"slices"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/loader"

	"github.com/xraph/tether/api"
)

// ContributorName is the contributor name in the manifest and on the wire.
const ContributorName = "tether"

const manifestSource = "tether/contract/manifest.yaml"

//go:embed manifest.yaml
var manifestYAML []byte

// ServicesProvider returns the services the handlers run on, or nil while
// Tether has not started. It is called on every request, so a contributor
// registered by the dashboard before Tether's Start still works afterwards.
type ServicesProvider func() *api.Services

// Deps are the data sources the handlers need.
type Deps struct {
	Services ServicesProvider

	// AllowAnonymous serves requests whose principal has no authenticated
	// user. The default refuses them with UNAUTHENTICATED, before any
	// handler runs: the dashboard's auth is off unless the host turns it on,
	// and these intents provision device keys and delete tenants. A
	// principal has no user when the dashboard's auth is off, and also when
	// it is on but the request did not authenticate.
	AllowAnonymous bool

	// Operators makes every caller an operator, who may name any tenant and
	// create, change and delete tenants. Without it a caller is confined to
	// their own tenant (see the package doc). AllowAnonymous implies it: an
	// anonymous caller has no session to take a tenant from.
	Operators bool
}

// handlerFunc is the shape of a typed intent handler.
type handlerFunc[I, O any] = func(ctx context.Context, in I, p dashcontract.Principal) (O, error)

// Register loads the embedded manifest, validates it, registers it with the
// dashboard's contract registry and wires one handler per intent into disp.
func Register(disp *dispatcher.Dispatcher, reg dashcontract.Registry, wreg dashcontract.WardenRegistry, deps Deps) error {
	if deps.Services == nil {
		return errors.New("tether/contract: Deps.Services is required")
	}

	m, err := loader.Load(bytes.NewReader(manifestYAML), manifestSource)
	if err != nil {
		return fmt.Errorf("tether/contract: load manifest: %w", err)
	}

	if err := loader.Validate(m, wreg); err != nil {
		return fmt.Errorf("tether/contract: validate manifest: %w", err)
	}

	if err := reg.Register(m); err != nil {
		return fmt.Errorf("tether/contract: register manifest: %w", err)
	}

	g := newGate(deps)

	var opts []dispatcher.RegisterOption
	if !deps.AllowAnonymous {
		opts = append(opts, dispatcher.BeforeDispatch(requireUser))
	}

	secret := append(slices.Clone(opts), dispatcher.SecretResponse())
	sp := deps.Services

	return errors.Join(
		// Reads.
		dispatcher.RegisterQuery(disp, ContributorName, "session.info", 1, sessionInfoHandler(g), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "overview.stats", 1, overviewStatsHandler(g), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "tenants.list", 1, tenantsListHandler(g), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "tenants.get", 1, scoped(g, tenantsGetHandler(sp)), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "devices.list", 1, scoped(g, devicesListHandler(sp)), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "devices.get", 1, scoped(g, devicesGetHandler(sp)), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "roles.list", 1, scoped(g, rolesListHandler(sp)), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "roles.topics.get", 1, scoped(g, rolesTopicsGetHandler(sp)), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "clients.list", 1, scoped(g, clientsListHandler(sp)), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "retained.list", 1, scoped(g, retainedListHandler(sp)), opts...),

		// Commands. Tenants are the operators' to create, change and delete.
		// Every command is audited once it ran (audit.go).
		dispatcher.RegisterCommand(disp, ContributorName, "tenants.create", 1, operatorOnly(g, audited(sp, tenantCreated, tenantsCreateHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "tenants.update", 1, operatorOnly(g, scoped(g, audited(sp, tenantUpdated, tenantsUpdateHandler(sp)))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "tenants.delete", 1, operatorOnly(g, scoped(g, audited(sp, tenantDeleted, tenantsDeleteHandler(sp)))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.provision", 1, scoped(g, audited(sp, deviceProvisioned, devicesProvisionHandler(sp))), secret...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.rotate", 1, scoped(g, audited(sp, deviceRotated, devicesRotateHandler(sp))), secret...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.revoke", 1, scoped(g, audited(sp, deviceRevoked, devicesRevokeHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.delete", 1, scoped(g, audited(sp, deviceDeleted, devicesDeleteHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.roles.set", 1, scoped(g, audited(sp, deviceRolesSet, devicesRolesSetHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "roles.create", 1, scoped(g, audited(sp, roleCreated, rolesCreateHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "roles.delete", 1, scoped(g, audited(sp, roleDeleted, rolesDeleteHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "roles.topics.set", 1, scoped(g, audited(sp, roleTopicsSet, rolesTopicsSetHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "clients.kick", 1, scoped(g, audited(sp, clientKicked, clientsKickHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "sessions.delete", 1, scoped(g, audited(sp, sessionDeleted, sessionsDeleteHandler(sp))), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "retained.delete", 1, scoped(g, audited(sp, retainedDeleted, retainedDeleteHandler(sp))), opts...),
	)
}
