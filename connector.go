package synkit

import (
	"fmt"
	"os"

	"github.com/piyushsingariya/synkit/logger"
	protocol "github.com/piyushsingariya/synkit/protocol"
	"github.com/piyushsingariya/synkit/safego"
	"github.com/spf13/cobra"
)

var (
	globalDriver  protocol.Driver
	globalAdapter protocol.Adapter
)

func RegisterDriver(driver protocol.Driver) {
	defer safego.Recovery(true)

	if globalAdapter != nil {
		logger.Fatal(fmt.Errorf("adapter already registered: %s", globalAdapter.Type()))
	}

	globalDriver = driver

	// Execute the root command
	err := protocol.CreateRootCommand(true, driver).Execute()
	if err != nil {
		logger.Fatal(err)
	}

	os.Exit(0)
}

func RegisterAdapter(adapter protocol.Adapter) (*cobra.Command, error) {
	if globalDriver != nil {
		return nil, fmt.Errorf("driver alraedy registered: %s", globalDriver.Type())
	}

	globalAdapter = adapter

	return protocol.CreateRootCommand(false, adapter), nil
}
