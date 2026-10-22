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
// Every intent refuses a request whose principal has no authenticated user
// (UNAUTHENTICATED), so a dashboard with its auth off serves nothing here
// unless you pass WithInsecureDashboard. The contributor stays registered
// either way, so the plugin still shows its pages and the refusal.
func (e *Extension) RegisterContractContributor(disp *dispatcher.Dispatcher, reg dashcontract.Registry, wreg dashcontract.WardenRegistry) error {
	return contract.Register(disp, reg, wreg, contract.Deps{Services: e.Services, AllowAnonymous: e.insecureDashboard})
}
