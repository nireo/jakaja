package util

import (
	"math/rand"
	"sync"
)

// thread-safe set for easier and cleaner usage of arrays in the code.
type Set[T comparable] struct {
	slice []T
	mu    sync.RWMutex
}

// Add adds a element into the set
func (s *Set[T]) Add(elem T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// check that item is unique
	for _, v := range s.slice {
		if v == elem {
			return
		}
	}

	s.slice = append(s.slice, elem)
}

// Len returns the length of the set
func (s *Set[T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.slice)
}

// Get all of the values in the slice
func (s *Set[T]) Get() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]T, 0)
	return append(res, s.slice...)
}

// Random item from the set
func (s *Set[T]) Random() T {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.slice[rand.Int()%len(s.slice)]
}

// Clear the items of the set
func (s *Set[T]) Clear() []T {
	s.mu.Lock()
	defer s.mu.Unlock()

	res := s.slice
	s.slice = make([]T, 0)
	return res
}
