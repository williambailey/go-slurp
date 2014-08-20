package slurp

import (
	"testing"
	"time"
)

type simpleSlurper struct {
	count int
}

func (s *simpleSlurper) Slurp(items <-chan *Item) {
	for _ = range items {
		s.count++
	}
}

func createSimpleSlurpers(count int) []Slurper {
	a := make([]Slurper, count)
	for i := 0; i < count; i++ {
		a[i] = &simpleSlurper{}
	}
	return a
}

func TestFanOutSlurper(t *testing.T) {
	slurpers := createSimpleSlurpers(4)
	ch := make(chan *Item, 3)
	time := time.Now()
	s := NewFanOutSlurper(slurpers...)
	ch <- NewItem(time)
	ch <- NewItem(time)
	ch <- NewItem(time)
	close(ch)
	s.Slurp(ch)
	for k, v := range slurpers {
		s := v.(*simpleSlurper)
		if s.count != 3 {
			t.Errorf(
				"Expecting to have slurped 3 events for s%d but we managed %d.",
				k,
				s.count,
			)
		}
	}
}

func doBenchmarkSlurper(b *testing.B, s Slurper) {
	ch := make(chan *Item, 10000)
	time := time.Now()
	i := NewItem(time)
	go func() {
		for n := 0; n < b.N; n++ {
			ch <- i
		}
		close(ch)
	}()
	s.Slurp(ch)
}

func BenchmarkFanOutSlurper0(b *testing.B) {
	doBenchmarkSlurper(b, NewFanOutSlurper())
}

func BenchmarkFanOutSlurper1(b *testing.B) {
	doBenchmarkSlurper(b, NewFanOutSlurper(createSimpleSlurpers(1)...))
}

func BenchmarkFanOutSlurper10(b *testing.B) {
	doBenchmarkSlurper(b, NewFanOutSlurper(createSimpleSlurpers(10)...))
}
