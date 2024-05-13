package base

import (
	"time"

	"github.com/piyushsingariya/shift/logger"
)

func RetryOnFailure(attempts int, sleep *time.Duration, f func() error) (err error) {
	for i := 0; i < attempts; i++ {
		if err = f(); err == nil {
			return nil
		}

		logger.Infof("Retrying after %v...", sleep)
		time.Sleep(*sleep)
	}

	return err
}
