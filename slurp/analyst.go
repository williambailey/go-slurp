package slurp

import (
	"sync"
	"time"
)

// Analyst is the thing that conducts analysis.
type Analyst interface {
	//AnalysisRequest produces an *AnalysisRequest from a single point in time.
	AnalysisRequest(pointInTime time.Time) *AnalysisRequest
	//AnalysisRangeRequest produces an *AnalysisRequest from a time range.
	AnalysisRangeRequest(from time.Time, until time.Time) *AnalysisRequest
	//RangeForAnalysisRequest allows you to see what range the Analyst will request for
	//a single point in time.
	RangeForAnalysisRequest(pointInTime time.Time) (from time.Time, until time.Time)
	//RangeForAnalysisRangeRequest allows you to see what range the Analyst will request for
	//a time range.
	RangeForAnalysisRangeRequest(from time.Time, until time.Time) (f time.Time, u time.Time)
}

// AnalysisRequest contains information about how the Analyst wants its data.
type AnalysisRequest struct {
	Analyst     Analyst
	TimeFrom    time.Time
	TimeUntil   time.Time
	DataLoader  []DataLoader
	SlurperFunc SlurperFunc
}

// AnalysisRequestSlurper coordinates a Slurp for multiple AnalysisRequests.
type AnalysisRequestSlurper struct {
	Requests        []*AnalysisRequest
	slurpChanRate   *ItemChannelStatWrapper
	analystChanRate []*ItemChannelStatWrapper
}

// SlurpStat returns the stat of main item channel.
func (s *AnalysisRequestSlurper) SlurpStat() ItemChannelStat {
	return s.slurpChanRate.Stat()
}

// RequestStat return the stats of the channels for the analysis requests.
func (s *AnalysisRequestSlurper) RequestStat() []ItemChannelStat {
	r := make([]ItemChannelStat, len(s.Requests))
	for i, rate := range s.analystChanRate {
		if rate != nil {
			r[i] = rate.Stat()
		}
	}
	return r
}

// Slurp pull items and send to AnalysisRequests as required.
func (s *AnalysisRequestSlurper) Slurp(items <-chan *Item) {
	var (
		timeFrom   time.Time
		timeUntil  time.Time
		i          int
		r          *AnalysisRequest
		uAt        int64
		uFrom      int64
		uUntil     int64
		item       *Item
		itemLoaded bool
		loaders    []DataLoader
	)
	s.slurpChanRate = NewItemChannelStatWrapper(items)
	wg := sync.WaitGroup{}
	analystChan := make([]chan *Item, len(s.Requests))
	s.analystChanRate = make([]*ItemChannelStatWrapper, len(s.Requests))
	hasLoader := func(l DataLoader) bool {
		for _, v := range loaders {
			if l == v {
				return true
			}
		}
		return false
	}
	timeFrom, timeUntil = AnalysisRequestTimeRange(s.Requests...)
	for i, r = range s.Requests {
		for _, l := range r.DataLoader {
			if !hasLoader(l) {
				loaders = append(loaders, l)
			}
		}
		analystChan[i] = make(chan *Item, cap(items))
		s.analystChanRate[i] = NewItemChannelStatWrapper(analystChan[i])
		wg.Add(1)
		go func(s Slurper, ch <-chan *Item) {
			defer wg.Done()
			s.Slurp(ch)
		}(r.SlurperFunc, s.analystChanRate[i].Out)
	}
	uFrom = timeFrom.UnixNano()
	uUntil = timeUntil.UnixNano()
	for item = range s.slurpChanRate.Out {
		uAt = item.At.UnixNano()
		if uAt < uFrom || uAt >= uUntil {
			continue
		}
		itemLoaded = len(loaders) == 0
		for i, r = range s.Requests {
			if uAt < r.TimeFrom.UnixNano() || uAt >= r.TimeUntil.UnixNano() {
				continue
			}
			if !itemLoaded {
				LoadData(item, loaders...)
				itemLoaded = true
			}
			analystChan[i] <- item
		}
	}
	for i, rate := range s.analystChanRate {
		if rate != nil {
			close(analystChan[i])
		}
		s.analystChanRate[i] = nil
	}
	wg.Wait()
}

// NewAnalysisRequestSlurper create a new *AnalysisRequestSlurper
func NewAnalysisRequestSlurper(requests ...*AnalysisRequest) *AnalysisRequestSlurper {
	return &AnalysisRequestSlurper{
		Requests: requests,
	}
}

// AnalysisRequestTimeRange returns the min from and max until values.
func AnalysisRequestTimeRange(requests ...*AnalysisRequest) (time.Time, time.Time) {
	var (
		timeFrom  time.Time
		timeUntil time.Time
	)
	for _, r := range requests {
		if r.TimeFrom.Before(timeFrom) || timeFrom.IsZero() {
			timeFrom = r.TimeFrom
		}
		if r.TimeUntil.After(timeUntil) || timeUntil.IsZero() {
			timeUntil = r.TimeUntil
		}
	}
	return timeFrom, timeUntil
}
