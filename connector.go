package syndicate

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/syndicate"
	"github.com/spf13/cobra"
)

var (
	globalDriver  syndicate.Driver
	globalAdapter syndicate.Adapter
	rootCmd       *cobra.Command
)

func RegisterDriver(driver syndicate.Driver) error {
	if globalAdapter != nil {
		return fmt.Errorf("adapter already registered: %s", globalAdapter.Type())
	}

	globalDriver = driver

	return nil
}

func GetDriver() syndicate.Driver {
	return globalDriver
}

func RegisterAdapter(driver syndicate.Adapter) error {
	if globalDriver != nil {
		return fmt.Errorf("driver alraedy registered: %s", globalDriver.Type())
	}

	globalAdapter = driver

	return nil
}

func GetAdapter() syndicate.Adapter {
	return globalAdapter
}

func Root() *cobra.Command {
	return rootCmd
}
