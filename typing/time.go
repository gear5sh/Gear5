package typing

import (
	"fmt"
	"strings"
	"time"
)

type Time struct {
	time.Time
}

// UnmarshalJSON overrides the default unmarshalling for CustomTime
func (ct *Time) UnmarshalJSON(b []byte) error {
	// Remove the quotes around the date string
	str := strings.Trim(string(b), "\"")

	fmt.Println(str)

	time, err := parseStringTimestamp(str)
	if err != nil {
		return err
	}

	*ct = Time{time}
	return nil
}
