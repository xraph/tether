// Package contract publishes Tether's admin operations to the Forge
// dashboard as a contract contributor: a declarative manifest plus typed
// handlers that call the same services the REST API in package api does.
//
// The extension registers it from RegisterContractContributor. Every intent
// is admitted only for a principal with an authenticated user (set
// Deps.AllowAnonymous to turn that off), because the dashboard's auth is off
// unless the host enables it. Who may do what beyond that is the dashboard's
// policy. Its principals carry no tenant claims, so every tenant-scoped
// intent takes the tenant as an explicit tenantId and refuses without one.
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

	sp := deps.Services

	var opts []dispatcher.RegisterOption
	if !deps.AllowAnonymous {
		opts = append(opts, dispatcher.BeforeDispatch(requireUser))
	}

	secret := append(slices.Clone(opts), dispatcher.SecretResponse())

	return errors.Join(
		// Reads.
		dispatcher.RegisterQuery(disp, ContributorName, "overview.stats", 1, overviewStatsHandler(sp), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "tenants.list", 1, tenantsListHandler(sp), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "tenants.get", 1, tenantsGetHandler(sp), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "devices.list", 1, devicesListHandler(sp), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "devices.get", 1, devicesGetHandler(sp), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "roles.list", 1, rolesListHandler(sp), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "roles.topics.get", 1, rolesTopicsGetHandler(sp), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "clients.list", 1, clientsListHandler(sp), opts...),
		dispatcher.RegisterQuery(disp, ContributorName, "retained.list", 1, retainedListHandler(sp), opts...),

		// Commands.
		dispatcher.RegisterCommand(disp, ContributorName, "tenants.create", 1, tenantsCreateHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "tenants.update", 1, tenantsUpdateHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "tenants.delete", 1, tenantsDeleteHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.provision", 1, devicesProvisionHandler(sp), secret...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.rotate", 1, devicesRotateHandler(sp), secret...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.revoke", 1, devicesRevokeHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.delete", 1, devicesDeleteHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "devices.roles.set", 1, devicesRolesSetHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "roles.create", 1, rolesCreateHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "roles.delete", 1, rolesDeleteHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "roles.topics.set", 1, rolesTopicsSetHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "clients.kick", 1, clientsKickHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "sessions.delete", 1, sessionsDeleteHandler(sp), opts...),
		dispatcher.RegisterCommand(disp, ContributorName, "retained.delete", 1, retainedDeleteHandler(sp), opts...),
	)
}
