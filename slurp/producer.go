package slurp

import "time"

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

// CombinedProducer combines production runs from one or more producers.
type CombinedProducer struct {
	SendItemsBufferSize int
	Producers           []Producer
}

// Produce a production run that combines runs from other producers and
// sends items through in the correct order.
func (p *CombinedProducer) Produce(from time.Time, until time.Time) ProductionRun {
	var f ProductionRunFunc
	f = func(items chan<- *Item) {
		var ok bool
		run := make([]ProductionRun, len(p.Producers))
		ch := make([]chan *Item, len(p.Producers))
		nextItem := make([]*Item, len(p.Producers))
		for i := range p.Producers {
			run[i] = p.Producers[i].Produce(from, until)
			ch[i] = make(chan *Item, p.SendItemsBufferSize)
			go func(r ProductionRun, c chan<- *Item) {
				r.SendItems(c)
				close(c)
			}(run[i], ch[i])
			if nextItem[i], ok = <-ch[i]; !ok {
				nextItem[i] = nil
			}
		}
		for {
			next := -1
			for i := range nextItem {
				if nextItem[i] == nil {
					continue
				}
				if next == -1 || nextItem[i].At.Before(nextItem[next].At) {
					next = i
				}
			}
			if next < 0 {
				break
			}
			items <- nextItem[next]
			if nextItem[next], ok = <-ch[next]; !ok {
				nextItem[next] = nil
			}
		}
	}
	return f
}
