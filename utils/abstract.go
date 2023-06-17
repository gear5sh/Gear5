package utils

import "time"

func String(str string) *string {
	return &str
}

func Bool(b bool) *bool {
	return &b
}

func Int(i int) *int {
	return &i
}

func Duration(d time.Duration) *time.Duration {
	return &d
}

func Time(t time.Time) *time.Time {
	return &t
}

func Set[T any](array []T) []T {
	setArray := []T{}
	set := make(map[any]bool)
	for _, item := range array {
		if _, found := set[item]; found {
			continue
		}
		set[item] = true
		setArray = append(setArray, item)
	}

	return setArray
}

func Keys[T comparable](v map[T]any) []T {
	setArray := []T{}
	for key := range v {
		setArray = append(setArray, key)
	}

	return setArray
}

func IsSubset[T comparable](setArray, subsetArray []T) bool {
	set := make(map[T]bool)
	for _, item := range setArray {
		set[item] = true
	}

	for _, item := range subsetArray {
		if _, found := set[item]; !found {
			return false
		}
	}

	return true
}

func MaxDate(v1, v2 time.Time) time.Time {
	if v1.After(v2) {
		return v1
	}

	return v2
}

func ToChannel[T any](arr []T, buffer int64) <-chan T {
	var channel chan T
	if buffer > 0 {
		channel = make(chan T, buffer)
	} else {
		channel = make(chan T)
	}

	func() {
		defer func() {
			close(channel)
		}()

		for _, elem := range arr {
			channel <- elem
		}
	}()

	return channel
}
