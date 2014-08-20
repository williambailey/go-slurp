package main

import (
	_ "expvar"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/williambailey/go-slurp/slurp"
	"github.com/williambailey/go-slurp/slurpd"
)

type slurperMapItem struct {
	started time.Time
	slurper *slurp.AnalysisRequestSlurper
}

var (
	loaderFunc      = make([]func(s *slurpd.Slurpd), 0)
	flagListen      string
	flagSlurpBuffer int
)

func init() {
	flag.StringVar(&flagListen, "listen", "127.0.0.1:9000", "where should we listen for http requests")
	flag.IntVar(&flagSlurpBuffer, "slurpBuffer", 100, "default buffer size to use when slurping")
}

func init() {
	// add items to loaderFunc
}

func main() {
	flag.Parse()
	if len(flag.Args()) != 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	slurpd := slurpd.NewSlurpd()
	slurpd.SlurpBuffer(flagSlurpBuffer)

	// Call any loader functions that we might have.
	log.Printf("Calling loader functions (%d).\n", len(loaderFunc))
	for i := range loaderFunc {
		log.Printf("- Function %d\n", i+1)
		loaderFunc[i](slurpd)
	}
	loaderFunc = nil

	// Wire up the HTTP API.
	log.Printf("Configuring HTTP API.\n")
	router := mux.NewRouter()
	slurpd.ConfigureRouter(router.PathPrefix("/api").Subrouter())

	log.Printf("Starting HTTP server on %s\n", flagListen)
	http.Handle("/", router)
	log.Panic(http.ListenAndServe(flagListen, nil))
}
