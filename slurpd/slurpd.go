package slurpd

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/gorilla/mux"
	"github.com/williambailey/go-slurp/slurp"
)

type slurperMapItem struct {
	started time.Time
	slurper *slurp.AnalysisRequestSlurper
}

func writeJSONResponse(w http.ResponseWriter, response interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	data, _ := json.MarshalIndent(response, "", "  ")
	w.Write(data)
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

// RegisterProducer registers a producer to a key.
// Will panic if the producer is not a slurp.Describer
func (s *Slurpd) RegisterProducer(k string, p slurp.Producer) {
	if _, ok := p.(slurp.Describer); !ok {
		log.Panicf("Expecting producer %q to be a slurp.Describer.\n", k)
	}
	s.producerMap[k] = p
}

// SlurpBuffer sets the default size of the buffer to use for slurping.
func (s *Slurpd) SlurpBuffer(size int) {
	s.slurpBuffer = size
}

// ConfigureRouter will add our routes configuration to an existing router.
func (s *Slurpd) ConfigureRouter(r *mux.Router) *mux.Router {
	get := r.Methods("GET").Subrouter()
	get.HandleFunc("/analysts", s.apiAnalystsHandler)
	get.HandleFunc("/data-loaders", s.apiDataLoadersHandler)
	get.HandleFunc("/producers", s.apiProducersHandler)
	get.HandleFunc("/slurpers", s.apiSlurpersHandler)
	post := r.Methods("POST").MatcherFunc(
		func(r *http.Request, rm *mux.RouteMatch) bool {
			ct := r.Header.Get("Content-Type")
			return ct == "application/json" || ct == "application/json; charset=UTF-8"
		},
	).Subrouter()
	post.HandleFunc("/analysis-range", s.apiAnalysisRangeHandler)
	post.HandleFunc("/analysis-request", s.apiAnalysisRequestHandler)
	return r
}

func (s *Slurpd) apiAnalystsHandler(w http.ResponseWriter, r *http.Request) {
	type analyst struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	var response = make(map[string]analyst, len(s.analystMap))
	for k, v := range s.analystMap {
		d := v.(slurp.Describer)
		response[k] = analyst{
			Name:        d.Name(),
			Description: d.Description(),
		}
	}
	writeJSONResponse(w, response)
}

func (s *Slurpd) apiDataLoadersHandler(w http.ResponseWriter, r *http.Request) {
	type loader struct {
		Name        string                `json:"name"`
		Description string                `json:"description"`
		Stat        *slurp.DataLoaderStat `json:"stat,omitempty"`
	}
	var response = make(map[string]loader, len(s.dataLoaderMap))
	for k, v := range s.dataLoaderMap {
		d := v.(slurp.Describer)
		var stat *slurp.DataLoaderStat
		if s, ok := v.(*slurp.DataLoaderStatWrapper); ok {
			stat = s.Stat()
		}
		response[k] = loader{
			Name:        d.Name(),
			Description: d.Description(),
			Stat:        stat,
		}
	}
	writeJSONResponse(w, response)
}

func (s *Slurpd) apiProducersHandler(w http.ResponseWriter, r *http.Request) {
	type producer struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	var response = make(map[string]producer, len(s.producerMap))
	for k, v := range s.producerMap {
		d := v.(slurp.Describer)
		response[k] = producer{
			Name:        d.Name(),
			Description: d.Description(),
		}
	}
	writeJSONResponse(w, response)
}

func (s *Slurpd) apiSlurpersHandler(w http.ResponseWriter, r *http.Request) {
	type analysisRequest struct {
		Analyst   string                `json:"analyst"`
		TimeFrom  time.Time             `json:"timeFrom"`
		TimeUntil time.Time             `json:"timeUntil"`
		Stat      slurp.ItemChannelStat `json:"stat"`
	}
	type slurper struct {
		Started         time.Time             `json:"started"`
		Stat            slurp.ItemChannelStat `json:"stat"`
		AnalysisRequest []analysisRequest     `json:"analysisRequest"`
	}
	var response = make(map[string]slurper, len(s.slurperMap))
	for k, v := range s.slurperMap {
		rs := v.slurper.RequestStat()
		ar := make([]analysisRequest, len(rs))
		for i, st := range rs {
			ar[i] = analysisRequest{
				Analyst:   s.analystKey(v.slurper.Requests[i].Analyst),
				TimeFrom:  v.slurper.Requests[i].TimeFrom,
				TimeUntil: v.slurper.Requests[i].TimeUntil,
				Stat:      st,
			}
		}
		response[k] = slurper{
			Started:         v.started,
			Stat:            v.slurper.SlurpStat(),
			AnalysisRequest: ar,
		}
	}
	writeJSONResponse(w, response)
}

func (s *Slurpd) apiAnalysisRangeHandler(w http.ResponseWriter, r *http.Request) {
	type analysis struct {
		Analyst string    `json:"analyst"`
		Time    time.Time `json:"time"`
	}
	type analysisRange struct {
		TimeFrom  time.Time `json:"timeFrom"`
		TimeUntil time.Time `json:"timeUntil"`
	}
	var an analysis
	err := json.NewDecoder(r.Body).Decode(&an)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		log.Printf("JSON decode error: %s.\n", err)
		return
	}
	if _, ok := s.analystMap[an.Analyst]; !ok {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		log.Printf("Unknown analyst %q.\n", an.Analyst)
		return
	}
	timeFrom, timeUntil := s.analystMap[an.Analyst].AnalysisRange(an.Time)
	response := &analysisRange{
		TimeFrom:  timeFrom,
		TimeUntil: timeUntil,
	}
	writeJSONResponse(w, response)
}

func (s *Slurpd) apiAnalysisRequestHandler(w http.ResponseWriter, r *http.Request) {
	type analysis struct {
		Analyst string    `json:"analyst"`
		Time    time.Time `json:"time"`
	}
	type request struct {
		Producer string     `json:"producer"`
		Analysis []analysis `json:"analysis"`
	}

	var (
		err error
		req request
		ok  bool
		p   slurp.Producer
		ar  []*slurp.AnalysisRequest
	)

	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		log.Printf("JSON decode error: %s.\n", err)
		return
	}
	if p, ok = s.producerMap[req.Producer]; !ok {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		log.Printf("Unknown producer %q.\n", req.Producer)
		return
	}
	if len(req.Analysis) < 1 {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		log.Printf("Empty analysis slice.\n")
		return
	}
	ar = make([]*slurp.AnalysisRequest, len(req.Analysis))
	for i, a := range req.Analysis {
		if _, ok = s.analystMap[a.Analyst]; !ok {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			log.Printf("Unknown analyst %q.\n", a.Analyst)
			return
		}
		ar[i] = s.analystMap[a.Analyst].AnalysisRequest(a.Time)
	}

	// TODO: Better duplication and range checking. I.e. If we have a large
	//       range with a big time range that is not going to be analysed
	//       then it will most likely be better to split up the request
	//       and many NewAnalysisRequestSlurper instances with the smaller
	//       time ranges.

	k := uuid.New()
	sl := slurp.NewAnalysisRequestSlurper(ar...)

	go func() {
		s.slurperMap[k] = slurperMapItem{
			started: time.Now(),
			slurper: sl,
		}
		defer delete(s.slurperMap, k)
		ch := make(chan *slurp.Item, s.slurpBuffer)
		go func() {
			p.Produce(slurp.AnalysisRequestTimeRange(ar...)).SendItems(ch)
			close(ch)
		}()
		sl.Slurp(ch)
	}()

	writeJSONResponse(w, "OK")
}
