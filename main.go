package syndicate

import (
	"fmt"

	"github.com/piyushsingariya/syndicate/connector"
)

var (
	globalDriver  connector.Driver
	globalAdapter connector.Adapter
)

func RegisterDriver(driver connector.Driver) error {
	if globalAdapter != nil {
		return fmt.Errorf("adapter already registered: %s", globalAdapter.Type())
	}

	globalDriver = driver

	return nil
}

func RegisterAdapter(driver connector.Adapter) error {
	if globalDriver != nil {
		return fmt.Errorf("driver alraedy registered: %s", globalDriver.Type())
	}

	globalAdapter = driver

	return nil
}
