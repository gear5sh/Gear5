package utils

import "errors"

var ErrServerTimeout = errors.New("the server did not respond within 10 minutes. Please try again. If you think this is a system error, write to us support@jitsu.com")
