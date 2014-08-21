package slurpd

import (
	"log"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/williambailey/go-slurp/slurp"
)

type slurperMapItem struct {
	started time.Time
	slurper *slurp.AnalysisRequestSlurper
}

// Slurpd is our slurp daemon/http handler.
type Slurpd struct {
	analystMap    map[string]slurp.Analyst
	dataLoaderMap map[string]slurp.DataLoader
	producerMap   map[string]slurp.Producer
	slurperMap    map[string]slurperMapItem
	slurpBuffer   int
}

// NewSlurpd returns a pointer to a new Slurpd instance.
func NewSlurpd() *Slurpd {
	return &Slurpd{
		analystMap:    make(map[string]slurp.Analyst),
		dataLoaderMap: make(map[string]slurp.DataLoader),
		producerMap:   make(map[string]slurp.Producer),
		slurperMap:    make(map[string]slurperMapItem),
		slurpBuffer:   0,
	}
}

func (s *Slurpd) analystKey(a slurp.Analyst) string {
	for k, v := range s.analystMap {
		if a == v {
			return k
		}
	}
	return ""
}

func (s *Slurpd) dataLoaderKey(d slurp.DataLoader) string {
	for k, v := range s.dataLoaderMap {
		if d == v {
			return k
		}
	}
	return ""
}

func (s *Slurpd) producerKey(p slurp.Producer) string {
	for k, v := range s.producerMap {
		if p == v {
			return k
		}
	}
	return ""
}

func (s *Slurpd) slurperKey(rs *slurp.AnalysisRequestSlurper) string {
	for k, v := range s.slurperMap {
		if rs == v.slurper {
			return k
		}
	}
	return ""
}

// RegisterAnalyst registers an analyst to a key.
// Will panic if the analyst is not a slurp.Describer
func (s *Slurpd) RegisterAnalyst(k string, a slurp.Analyst) {
	if _, ok := a.(slurp.Describer); !ok {
		log.Panicf("Expecting analyst %q to be a slurp.Describer.\n", k)
	}
	s.analystMap[k] = a
}

// Analyst will try to get the instance at k
func (s *Slurpd) Analyst(k string) (slurp.Analyst, bool) {
	v, ok := s.analystMap[k]
	return v, ok
}

// RegisterDataLoader registers a loader to a key.
// Will panic if the loader is not a slurp.Describer
// Will panic if the loader is not a slurp.DataLoaderStatWrapper
func (s *Slurpd) RegisterDataLoader(k string, l slurp.DataLoader) {
	if _, ok := l.(slurp.Describer); !ok {
		log.Panicf("Expecting data loader %q to be a slurp.Describer.\n", k)
	}
	if _, ok := l.(*slurp.DataLoaderStatWrapper); !ok {
		log.Panicf("Expecting data loader %q to be a slurp.DataLoaderStatWrapper.\n", k)
	}
	s.dataLoaderMap[k] = l
}

// DataLoader will try to get the instance at k
func (s *Slurpd) DataLoader(k string) (slurp.DataLoader, bool) {
	v, ok := s.dataLoaderMap[k]
	return v, ok
}

// RegisterProducer registers a producer to a key.
// Will panic if the producer is not a slurp.Describer
func (s *Slurpd) RegisterProducer(k string, p slurp.Producer) {
	if _, ok := p.(slurp.Describer); !ok {
		log.Panicf("Expecting producer %q to be a slurp.Describer.\n", k)
	}
	s.producerMap[k] = p
}

// Producer will try to get the instance at k
func (s *Slurpd) Producer(k string) (slurp.Producer, bool) {
	v, ok := s.producerMap[k]
	return v, ok
}

// SlurpBuffer sets the default size of the buffer to use for slurping.
func (s *Slurpd) SlurpBuffer(size int) {
	s.slurpBuffer = size
}

// SlurpAnalysisRequest performs a slurp for the requests using data provided
// by the producer.
func (s *Slurpd) SlurpAnalysisRequest(producer slurp.Producer, analysisRequest []*slurp.AnalysisRequest) {
	// TODO: Better duplication and range checking. I.e. If we have a large
	//       range with a big time range that is not going to be analysed
	//       then it will most likely be better to split up the request
	//       in to many AnalysisRequestSlurper instances with the smaller
	//       time ranges that we then run concurrently.
	k := uuid.New()
	sl := slurp.NewAnalysisRequestSlurper(analysisRequest...)
	s.slurperMap[k] = slurperMapItem{
		started: time.Now(),
		slurper: sl,
	}
	defer delete(s.slurperMap, k)
	ch := make(chan *slurp.Item, s.slurpBuffer)
	go func() {
		producer.Produce(slurp.AnalysisRequestTimeRange(analysisRequest...)).SendItems(ch)
		close(ch)
	}()
	sl.Slurp(ch)
}
