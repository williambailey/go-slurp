package slurp

import (
	"time"
)

// Producer is the thing that generates production runs of items.
type Producer interface {
	Produce(from time.Time, until time.Time) ProductionRun
}

// ProductionRun is responsible for producing items and sending them
// to a channel.
// Items will be sent to the channel in time order with the
// oldest item going first. If two items have the same time
// value then the order is undefined.
type ProductionRun interface {
	SendItems(chan<- *Item)
}

// ProductionRunFunc is an adapter that allow you to use
// an ordinary function as an ProductionRun.
type ProductionRunFunc func(chan<- *Item)

// SendItems calls f(ch)
func (f ProductionRunFunc) SendItems(ch chan<- *Item) {
	f(ch)
}
