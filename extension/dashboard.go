package extension

import (
	dashboard "github.com/xraph/forge/extensions/dashboard"
	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"

	"github.com/xraph/tether/extension/contract"
)

var _ dashboard.ContractContributorAware = (*Extension)(nil)

// RegisterContractContributor implements dashboard.ContractContributorAware:
// the dashboard calls it during its own Start and gets Tether's admin
// intents (see package contract). The handlers read the services through
// Services at request time, so they answer UNAVAILABLE until Tether's Start
// has built them, whichever extension started first.
//
// The contributor adds no authentication of its own. The admin REST routes
// are guarded by WithAdminMiddleware, but the dashboard's intents are guarded
// by the dashboard: with its auth off (the default) they are open to anyone
// who can reach it.
func (e *Extension) RegisterContractContributor(disp *dispatcher.Dispatcher, reg dashcontract.Registry, wreg dashcontract.WardenRegistry) error {
	return contract.Register(disp, reg, wreg, contract.Deps{Services: e.Services})
}
