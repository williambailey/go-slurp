package slurpd

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/williambailey/go-slurp/slurp"
)

var (
	// HTTPHandlerList is a list of HTTPHandler instances that are used
	// to form the Slurpd HTTP API
	HTTPHandlerList = make([]HTTPHandler, 0)
)

func init() {
	HTTPHandlerList = append(
		HTTPHandlerList,
		&httpHandlerDescribeSelf{},
		&httpHandlerAnalysts{},
		&httpHandlerDataLoaders{},
		&httpHandlerProducers{},
		&httpHandlerSlurpers{},
		&httpHandlerAnalysisRange{},
		&httpHandlerAnalysisRequest{},
	)
}

// ConfigureRouter will attach the http api handlers to a router.
func ConfigureRouter(s *Slurpd, r *mux.Router) *mux.Router {
	for _, h := range HTTPHandlerList {
		r.HandleFunc(h.Path(), h.HandlerFunc(s)).Methods(h.Method())
	}
	return r
}

// WriteJSONResponse is a util func to send JSON back to the HTTP client.
func WriteJSONResponse(w http.ResponseWriter, response interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	data, _ := json.MarshalIndent(response, "", "  ")
	w.Write(data)
}

// HTTPHandler allows us to register and self document the http api.
type HTTPHandler interface {
	Method() string
	Path() string
	Description() string
	Readme() string
	HandlerFunc(s *Slurpd) http.HandlerFunc
}

type httpHandlerDescribeSelf struct {
	template *template.Template
}

func (h *httpHandlerDescribeSelf) Method() string {
	return "GET"
}

func (h *httpHandlerDescribeSelf) Path() string {
	return "/"
}

func (h *httpHandlerDescribeSelf) Description() string {
	return "Describes the available api."
}

func (h *httpHandlerDescribeSelf) Readme() string {
	return ""
}

func (h *httpHandlerDescribeSelf) HandlerFunc(s *Slurpd) http.HandlerFunc {
	if h.template == nil {
		h.template = template.Must(template.New("describe").Parse(`
<!doctype html>
<html>
	<head>
		<title>Slurpd HTTP API</title>
		<meta charset="utf-8" />
	</head>
  <body>
		<main>
			<header>
				<h1>Slurpd HTTP API</h1>
			</header>
{{range .}}
			<article class="http-handler">
				<header>
					<h2>
						<span class="method">{{.Method}}</span>
						{{if eq .Method "GET"}}<a class="path" href=".{{.Path}}">{{.Path}}</a>
						{{else}}<span class="path">{{.Path}}</span>
						{{end}}
					</h2>
					<p class="description">{{.Description}}<p>
				</header>
				<pre class="readme">{{.Readme}}</pre>
			</article>
{{end}}
		</main>
	</body>
</html>
		`))
	}
	return func(w http.ResponseWriter, r *http.Request) {
		err := h.template.Execute(w, HTTPHandlerList)
		if err != nil {
			log.Panicln(err)
		}
	}
}

type httpHandlerAnalysts struct{}

func (h *httpHandlerAnalysts) Method() string {
	return "GET"
}

func (h *httpHandlerAnalysts) Path() string {
	return "/analysts"
}

func (h *httpHandlerAnalysts) Description() string {
	return "Gets a list of available analysts."
}

func (h *httpHandlerAnalysts) Readme() string {
	return "blah blah blah."
}

func (h *httpHandlerAnalysts) HandlerFunc(s *Slurpd) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		WriteJSONResponse(w, response)
	}
}

type httpHandlerDataLoaders struct{}

func (h *httpHandlerDataLoaders) Method() string {
	return "GET"
}

func (h *httpHandlerDataLoaders) Path() string {
	return "/data-loaders"
}

func (h *httpHandlerDataLoaders) Description() string {
	return "Gets statistics for the available data-loaders."
}

func (h *httpHandlerDataLoaders) Readme() string {
	return "blah blah blah."
}

func (h *httpHandlerDataLoaders) HandlerFunc(s *Slurpd) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		WriteJSONResponse(w, response)
	}
}

type httpHandlerProducers struct{}

func (h *httpHandlerProducers) Method() string {
	return "GET"
}

func (h *httpHandlerProducers) Path() string {
	return "/producers"
}

func (h *httpHandlerProducers) Description() string {
	return "Gets a list of available producers."
}

func (h *httpHandlerProducers) Readme() string {
	return "blah blah blah."
}

func (h *httpHandlerProducers) HandlerFunc(s *Slurpd) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		WriteJSONResponse(w, response)
	}
}

type httpHandlerSlurpers struct{}

func (h *httpHandlerSlurpers) Method() string {
	return "GET"
}

func (h *httpHandlerSlurpers) Path() string {
	return "/slurpers"
}

func (h *httpHandlerSlurpers) Description() string {
	return "Gets information about any currently running slurpers."
}

func (h *httpHandlerSlurpers) Readme() string {
	return "blah blah blah."
}

func (h *httpHandlerSlurpers) HandlerFunc(s *Slurpd) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		WriteJSONResponse(w, response)
	}
}

type httpHandlerAnalysisRange struct{}

func (h *httpHandlerAnalysisRange) Method() string {
	return "POST"
}

func (h *httpHandlerAnalysisRange) Path() string {
	return "/analysis-range"
}

func (h *httpHandlerAnalysisRange) Description() string {
	return "For a given point in time returns the range exoected by an analyst."
}

func (h *httpHandlerAnalysisRange) Readme() string {
	return "blah blah blah."
}

func (h *httpHandlerAnalysisRange) HandlerFunc(s *Slurpd) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		WriteJSONResponse(w, response)
	}
}

type httpHandlerAnalysisRequest struct{}

func (h *httpHandlerAnalysisRequest) Method() string {
	return "POST"
}

func (h *httpHandlerAnalysisRequest) Path() string {
	return "/analysis-request"
}

func (h *httpHandlerAnalysisRequest) Description() string {
	return "Issue a request for analysis."
}

func (h *httpHandlerAnalysisRequest) Readme() string {
	return "blah blah blah."
}

func (h *httpHandlerAnalysisRequest) HandlerFunc(s *Slurpd) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		go s.SlurpAnalysisRequest(p, ar)
		WriteJSONResponse(w, "OK")
	}
}
