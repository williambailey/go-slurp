package slurp

import (
	"sync"
	"time"
)

// Item is nothing more then a time and map[string]interface{} pair.
type Item struct {
	At   time.Time
	Data map[string]interface{}
}

// NewItem creates a new Item and returns a pointer to it.
func NewItem(at time.Time) *Item {
	return &Item{
		At:   at,
		Data: make(map[string]interface{}),
	}
}

// ItemChannelStatWrapper is used to monitor the throughput rate.
type ItemChannelStatWrapper struct {
	in     <-chan *Item
	Out    chan *Item
	itemAt *time.Time
	rate   float64
	count  int64
	mutex  sync.RWMutex
}

// Stat returns information sbout the item channel.
func (c *ItemChannelStatWrapper) Stat() ItemChannelStat {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return ItemChannelStat{
		ItemAt:   c.itemAt,
		Rate:     c.rate,
		Count:    c.count,
		Length:   len(c.in),
		Capacity: cap(c.in),
	}
}

// ItemChannelStat provides statistic about the item channel.
type ItemChannelStat struct {
	ItemAt   *time.Time `json:"itemAt,omitempty"`
	Rate     float64    `json:"rate"`
	Count    int64      `json:"count"`
	Length   int        `json:"length"`
	Capacity int        `json:"capacity"`
}

// NewItemChannelStatWrapper creates a new ItemChannelStatWrapper
func NewItemChannelStatWrapper(items <-chan *Item) *ItemChannelStatWrapper {
	c := &ItemChannelStatWrapper{
		in:  items,
		Out: make(chan *Item, 0), // We want the Out chan to be unbuffered.
	}
	go func() {
		var (
			count  int64
			itemAt *time.Time
		)
		times := make([]int64, 100, 100)
		timesIdx := len(times)
		rateReport := time.NewTicker(200 * time.Millisecond)
		for {
			select {
			case <-rateReport.C:
				c.mutex.Lock()
				if timesIdx == len(times) {
					c.rate = 0
				} else {
					c.rate = 1000000000.0 * float64(len(times)-timesIdx) / float64(time.Now().UnixNano()-times[timesIdx])
				}
				c.count = count
				c.itemAt = itemAt
				c.mutex.Unlock()
			case i, ok := <-c.in:
				if !ok {
					rateReport.Stop()
					close(c.Out)
					c.mutex.Lock()
					c.rate = 0
					c.count = count
					c.itemAt = itemAt
					c.mutex.Unlock()
					return
				}
				c.Out <- i
				times = times[1:]
				times = append(times, time.Now().UnixNano())
				if timesIdx > 0 {
					timesIdx--
				}
				count++
				itemAt = &i.At
			}
		}
	}()
	return c
}

// Slurper is responsible for consuming a channel of items.
type Slurper interface {
	Slurp(<-chan *Item)
}

// SlurperFunc is an adapter that allow you to use
// an ordinary function as a Slurper.
type SlurperFunc func(<-chan *Item)

// Slurp calls f(items)
func (f SlurperFunc) Slurp(items <-chan *Item) {
	f(items)
}

// CompositionSlurper can be used to present multiple slurpers as a single
// slurper.
type CompositionSlurper struct {
	slurpers      []Slurper
	slurpFunction func(in <-chan *Item, out []chan *Item)
}

// Slurp coordinates the slurp for multiple slurpers. Responsability for the
// reading and writing to the channel is defered to the slurpFunction.
func (s *CompositionSlurper) Slurp(in <-chan *Item) {
	wg := sync.WaitGroup{}
	out := make([]chan *Item, len(s.slurpers))
	for i, slurper := range s.slurpers {
		out[i] = make(chan *Item, cap(in))
		wg.Add(1)
		go func(s Slurper, ch <-chan *Item) {
			defer wg.Done()
			s.Slurp(ch)
		}(slurper, out[i])
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.slurpFunction(in, out)
		// in has been consumed so lets ensure that
		// we close all of our out chans.
		for _, o := range out {
			close(o)
		}
	}()
	wg.Wait()
}

// NewFanOutSlurper will fan out all items to all slurpers.
func NewFanOutSlurper(slurpers ...Slurper) *CompositionSlurper {
	return &CompositionSlurper{
		slurpers: slurpers,
		slurpFunction: func(in <-chan *Item, out []chan *Item) {
			for i := range in {
				for _, o := range out {
					o <- i
				}
			}
		},
	}
}
