package main

import (
	"math/rand"
	"time"

	"github.com/williambailey/go-slurp/slurp"
	"github.com/williambailey/go-slurp/slurpd"
)

func init() {
	loaderFunc = append(loaderFunc, func(s *slurpd.Slurpd) {
		l := slurp.NewDataLoaderStatWrapper(&exampleLoader{})
		s.RegisterDataLoader("ex", l)
		s.RegisterAnalyst("ex", &exampleAnalyst{
			dataLoaders: []slurp.DataLoader{
				l,
			},
		})
		s.RegisterProducer("ex", &exampleProducer{})
	})
}

type exampleAnalyst struct {
	dataLoaders []slurp.DataLoader
}

func (a *exampleAnalyst) Name() string {
	return "Example Analyst"
}

func (a *exampleAnalyst) Description() string {
	return "This is just an example."
}

func (a *exampleAnalyst) AnalysisRequest(pointInTime time.Time) *slurp.AnalysisRequest {
	from, until := a.AnalysisRange(pointInTime)
	return &slurp.AnalysisRequest{
		Analyst:    a,
		TimeFrom:   from,
		TimeUntil:  until,
		DataLoader: a.dataLoaders,
		SlurperFunc: func(items <-chan *slurp.Item) {
			for _ = range items {
				// blah...
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		},
	}
}

func (a *exampleAnalyst) AnalysisRange(pointInTime time.Time) (time.Time, time.Time) {
	return pointInTime, pointInTime.Add(24 * time.Hour)
}

type exampleProducer struct {
}

func (a *exampleProducer) Name() string {
	return "Example Producer"
}

func (a *exampleProducer) Description() string {
	return "This is just an example."
}

func (a *exampleProducer) Produce(from time.Time, until time.Time) slurp.ProductionRun {
	var f slurp.ProductionRunFunc
	f = func(ch chan<- *slurp.Item) {
		for i := from.UnixNano(); i < until.UnixNano(); i += int64(time.Second) {
			item := slurp.NewItem(time.Unix(0, i))
			ch <- item
		}
	}
	return f
}

type exampleLoader struct {
}

func (l *exampleLoader) Name() string {
	return "Example Loader"
}

func (l *exampleLoader) Description() string {
	return "This is just an example."
}

func (l *exampleLoader) LoadData(_ *slurp.Item) (string, interface{}) {
	i := rand.Intn(100)
	time.Sleep(time.Duration(i) * time.Nanosecond)
	if i < 50 {
		return "example", i
	}
	return "", nil
}
