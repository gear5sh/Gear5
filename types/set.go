package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type (
	Set[T comparable] struct {
		hash map[T]nothing
	}

	nothing struct{}
)

// Create a new set
func NewSet[T comparable](initial ...T) *Set[T] {
	s := &Set[T]{make(map[T]nothing)}

	for _, v := range initial {
		s.Insert(v)
	}

	return s
}

// Find the difference between two sets
func (this *Set[T]) Difference(set *Set[T]) *Set[T] {
	n := make(map[T]nothing)

	for k := range this.hash {
		if _, exists := set.hash[k]; !exists {
			n[k] = nothing{}
		}
	}

	return &Set[T]{n}
}

// Call f for each item in the set
func (this *Set[T]) Range(f func(T)) {
	if this == nil {
		*this = *NewSet[T]()
	}

	for k := range this.hash {
		f(k)
	}
}

// Test to see whether or not the element is in the set
func (this *Set[T]) Exists(element T) bool {
	if this == nil {
		*this = *NewSet[T]()
	}

	_, exists := this.hash[element]
	return exists
}

// Add an element to the set
func (this *Set[T]) Insert(element T) {
	if this == nil {
		*this = *NewSet[T]()
	}

	this.hash[element] = nothing{}
}

// Find the intersection of two sets
func (this *Set[T]) Intersection(set *Set[T]) *Set[T] {
	n := make(map[T]nothing)

	for k := range this.hash {
		if _, exists := set.hash[k]; exists {
			n[k] = nothing{}
		}
	}

	return &Set[T]{n}
}

// Return the number of items in the set
func (this *Set[T]) Len() int {
	if this == nil {
		*this = *NewSet[T]()
	}

	return len(this.hash)
}

// Test whether or not this set is a proper subset of "set"
func (this *Set[T]) ProperSubsetOf(set *Set[T]) bool {
	return this.SubsetOf(set) && this.Len() < set.Len()
}

// Remove an element from the set
func (this *Set[T]) Remove(element T) {
	delete(this.hash, element)
}

// Test whether or not this set is a subset of "set"
func (this *Set[T]) SubsetOf(set *Set[T]) bool {
	if this.Len() > set.Len() {
		return false
	}
	for k := range this.hash {
		if _, exists := set.hash[k]; !exists {
			return false
		}
	}
	return true
}

// Find the union of two sets
func (this *Set[T]) Union(set *Set[T]) *Set[T] {
	n := make(map[T]nothing)

	for k := range this.hash {
		n[k] = nothing{}
	}
	for k := range set.hash {
		n[k] = nothing{}
	}

	return &Set[T]{n}
}

func (this *Set[T]) String() string {
	keys := []string{}

	for key := range this.hash {
		keys = append(keys, fmt.Sprint(key))
	}

	return strings.Join(keys, " ")
}

func (this *Set[T]) Array() []T {
	arr := []T{}

	for item := range this.hash {
		arr = append(arr, item)
	}

	return arr
}

func (this *Set[T]) UnmarshalJSON(data []byte) error {
	*this = *NewSet[T]()

	arr := []T{}
	err := json.Unmarshal(data, &arr)
	if err != nil {
		return err
	}

	for _, item := range arr {
		this.Insert(item)
	}

	return nil
}

func (this *Set[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(this.Array())
}
