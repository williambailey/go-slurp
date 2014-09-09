package slurpd

import (
	"time"

	"github.com/williambailey/go-slurp/slurp"
)

// AnalystMapDTO is a map of AnalystDTO instances.
type AnalystMapDTO map[string]AnalystDTO

// AnalystDTO provides basic information for an Analyst.
type AnalystDTO struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// DataLoaderMapDTO is a map of DataLoaderDTO instances.
type DataLoaderMapDTO map[string]DataLoaderDTO

// DataLoaderDTO provides basic information for a DataLoader.
type DataLoaderDTO struct {
	Name        string                `json:"name"`
	Description string                `json:"description"`
	Stat        *slurp.DataLoaderStat `json:"stat,omitempty"`
}

// ProducerMapDTO is a map of ProducerDTO instances.
type ProducerMapDTO map[string]ProducerDTO

// ProducerDTO provides basic information for a Producer.
type ProducerDTO struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// SlurperMapDTO is a map of SlurperDTO instances.
type SlurperMapDTO map[string]SlurperDTO

// SlurperDTO provides basic information for a Slurper.
type SlurperDTO struct {
	Started         time.Time             `json:"started"`
	Stat            slurp.ItemChannelStat `json:"stat"`
	AnalysisRequest []AnalysisRequestDTO  `json:"analysisRequest"`
}

// AnalysisRequestDTO provides basic information for an AnalysisRequest.
type AnalysisRequestDTO struct {
	Analyst string                `json:"analyst"`
	Range   TimeRangeDTO          `json:"range"`
	Stat    slurp.ItemChannelStat `json:"stat"`
}

// TimeRangeDTO provides a from and until time.
type TimeRangeDTO struct {
	From  time.Time `json:"from"`
	Until time.Time `json:"until"`
}
