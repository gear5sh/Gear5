package syndicate

import (
	"fmt"

	protocol "github.com/piyushsingariya/syndicate/connector"
	"github.com/spf13/cobra"
)

var (
	globalDriver  protocol.Driver
	globalAdapter protocol.Adapter
)

func RegisterDriver(driver protocol.Driver) (*cobra.Command, error) {
	if globalAdapter != nil {
		return nil, fmt.Errorf("adapter already registered: %s", globalAdapter.Type())
	}

	globalDriver = driver

	return protocol.CreateRootCommand(true, driver), nil
}

func RegisterAdapter(adapter protocol.Adapter) (*cobra.Command, error) {
	if globalDriver != nil {
		return nil, fmt.Errorf("driver alraedy registered: %s", globalDriver.Type())
	}

	globalAdapter = adapter

	return protocol.CreateRootCommand(false, adapter), nil
}
