package utils

// type Function[A, B any, f func() error | func(A) error | func(A, B) error] struct {
// 	exec f
// 	argA A
// 	arg  B
// }

// type Checker[A, B any, f func() error | func(A) error | func(A, B) error] struct {
// 	functions []Function
// }

// func NewChecker[A, B any, f func() error | func(A) error | func(A, B) error](initial ...f) *Checker[A, B, f] {
// 	return &Checker[A, B, f]{
// 		functions: initial,
// 	}
// }

// func (c *Checker[A, B, f]) Add(function func() error) *Checker[A, B, f] {
// 	c.functions = append(c.functions, function)
// 	return c
// }

// func (c *Checker[A, B, f]) Run() error {
// 	for _, function := range c.functions {
// 		err := function()
// 		return
// 	}
// }
