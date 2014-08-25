package main

import (
	"math/rand"
	"net/http"
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
		slurpd.HTTPHandlerList = append(
			slurpd.HTTPHandlerList,
			&httpHandlerExample{},
		)
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

func (a *exampleAnalyst) slurpFunc(items <-chan *slurp.Item) {
	for _ = range items {
		// blah...
		time.Sleep(time.Duration(rand.Intn(1000000)) * time.Nanosecond)
	}
}

func (a *exampleAnalyst) AnalysisRequest(pointInTime time.Time) *slurp.AnalysisRequest {
	from, until := a.RangeForAnalysisRequest(pointInTime)
	return &slurp.AnalysisRequest{
		Analyst:     a,
		TimeFrom:    from,
		TimeUntil:   until,
		DataLoader:  a.dataLoaders,
		SlurperFunc: a.slurpFunc,
	}
}

func (a *exampleAnalyst) AnalysisRangeRequest(from time.Time, until time.Time) *slurp.AnalysisRequest {
	return &slurp.AnalysisRequest{
		Analyst:     a,
		TimeFrom:    from,
		TimeUntil:   until,
		DataLoader:  a.dataLoaders,
		SlurperFunc: a.slurpFunc,
	}
}

func (a *exampleAnalyst) RangeForAnalysisRequest(pointInTime time.Time) (time.Time, time.Time) {
	return pointInTime, pointInTime.Add(24 * time.Hour)
}

func (a *exampleAnalyst) RangeForAnalysisRangeRequest(from time.Time, until time.Time) (time.Time, time.Time) {
	return from, until
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
			time.Sleep(time.Duration(rand.Intn(1000000)) * time.Nanosecond)
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
	i := rand.Intn(1000000)
	time.Sleep(time.Duration(i) * time.Nanosecond)
	if i < 333333 {
		return "example", i
	} else if i < 666666 {
		return "example", nil
	}
	return "", nil
}

type httpHandlerExample struct{}

func (h *httpHandlerExample) Method() string {
	return "GET"
}

func (h *httpHandlerExample) Path() string {
	return "/example"
}

func (h *httpHandlerExample) Description() string {
	return "Sets off and example slurp."
}

func (h *httpHandlerExample) Readme() string {
	return "Saves having to use the other APIs :-)"
}

func (h *httpHandlerExample) HandlerFunc(s *slurpd.Slurpd) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, _ := s.Producer("ex")
		a, _ := s.Analyst("ex")
		go s.SlurpAnalysisRequest(
			p,
			a.AnalysisRequest(time.Now().Add(-24*time.Hour)),
			a.AnalysisRequest(time.Now().Add(-36*time.Hour)),
		)
		slurpd.WriteJSONResponse(w, "OK")
	}
}
