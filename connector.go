package syndicate

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	globalDriver  Driver
	globalAdapter Adapter
	rootCmd       *cobra.Command
)

func RegisterDriver(driver Driver) error {
	if globalAdapter != nil {
		return fmt.Errorf("adapter already registered: %s", globalAdapter.Type())
	}

	globalDriver = driver

	return nil
}

func GetDriver() Driver {
	return globalDriver
}

func RegisterAdapter(driver Adapter) error {
	if globalDriver != nil {
		return fmt.Errorf("driver alraedy registered: %s", globalDriver.Type())
	}

	globalAdapter = driver

	return nil
}

func GetAdapter() Adapter {
	return globalAdapter
}

func Root() *cobra.Command {
	return rootCmd
}
