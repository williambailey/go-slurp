package slurp

// Describer is an interface that allows us to provide user friendly
// descriptions for a struct.
type Describer interface {
	Name() string
	Description() string
}
