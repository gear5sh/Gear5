package utils

func String(str string) *string {
	return &str
}

func Bool(b bool) *bool {
	return &b
}

func Int(i int) *int {
	return &i
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

func IsSubset[T any](setArray, subsetArray []T) bool {
	set := make(map[any]bool)
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
