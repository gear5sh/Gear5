package syndicate

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/syndicate"
	"github.com/spf13/cobra"
)

var (
	globalDriver  syndicate.Driver
	globalAdapter syndicate.Adapter
)

func RegisterDriver(driver syndicate.Driver) (*cobra.Command, error) {
	if globalAdapter != nil {
		return nil, fmt.Errorf("adapter already registered: %s", globalAdapter.Type())
	}

	globalDriver = driver

	return syndicate.CreateRootCommand(true, driver), nil
}

func RegisterAdapter(adapter syndicate.Adapter) (*cobra.Command, error) {
	if globalDriver != nil {
		return nil, fmt.Errorf("driver alraedy registered: %s", globalDriver.Type())
	}

	globalAdapter = adapter

	return syndicate.CreateRootCommand(false, adapter), nil
}
