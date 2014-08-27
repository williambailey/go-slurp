package slurp

import (
	"fmt"
	"sync"
	"time"
)

// DataLoader is used to load extra data onto an item.
type DataLoader interface {
	// LoadData will load extra data for an item and
	// return a suggested data key and value.
	// The item itself does NOT get updated directly by
	// calling this function.
	//
	// If their is no data to load and you dont want a nil
	// value added to a data key in the item then this
	// function must return "", nil
	LoadData(*Item) (string, interface{})
}

// DataLoaderFunc is an adapter that allow you to use
// an ordinary function as a DataLoader.
type DataLoaderFunc func(*Item) (string, interface{})

// LoadData calls f(item)
func (f DataLoaderFunc) LoadData(item *Item) (string, interface{}) {
	return f(item)
}

// DataLoaderStatValue provices a standard set of stat counters.
type DataLoaderStatValue struct {
	FirstCallAt   *time.Time    `json:"firstCallAt,omitempty"`
	LastCallAt    *time.Time    `json:"lastCallAt,omitempty"`
	Count         int64         `json:"count"`
	DurationTotal time.Duration `json:"durationTotal"`
	DurationMin   time.Duration `json:"durationMin"`
	DurationAvg   time.Duration `json:"durationAvg"`
	DurationMax   time.Duration `json:"durationMax"`
}

// Called updates *DataLoaderStat
func (s *DataLoaderStatValue) called(t time.Time, d time.Duration) {
	if s.FirstCallAt == nil || t.Before(*s.FirstCallAt) {
		s.FirstCallAt = &t
	}
	if s.LastCallAt == nil || t.After(*s.LastCallAt) {
		s.LastCallAt = &t
	}
	s.Count++
	s.DurationTotal += d
	if d < s.DurationMin || s.DurationMin == 0 {
		s.DurationMin = d
	}
	s.DurationAvg = time.Duration(int64(s.DurationTotal) / s.Count)
	if d > s.DurationMax {
		s.DurationMax = d
	}
}

// DataLoaderStatWrapper wraps a DataLoader and provides stats about it.
type DataLoaderStatWrapper struct {
	Loader         DataLoader
	mutex          sync.RWMutex
	called         *DataLoaderStatValue
	returnEmptyKey *DataLoaderStatValue
	returnNilData  *DataLoaderStatValue
	returnData     *DataLoaderStatValue
}

// NewDataLoaderStatWrapper allows you to wrap DataLoader for stat collection.
func NewDataLoaderStatWrapper(loader DataLoader) *DataLoaderStatWrapper {
	w := &DataLoaderStatWrapper{
		Loader: loader,
	}
	w.Reset()
	return w
}

// LoadData calls the origional loader and updates its stats.
func (w *DataLoaderStatWrapper) LoadData(item *Item) (string, interface{}) {
	t := time.Now()
	k, v := w.Loader.LoadData(item)
	d := time.Now().Sub(t)
	w.mutex.Lock()
	w.called.called(t, d)
	if k == "" {
		w.returnEmptyKey.called(t, d)
	} else {
		if v == nil {
			w.returnNilData.called(t, d)
		} else {
			w.returnData.called(t, d)
		}
	}
	w.mutex.Unlock()
	return k, v
}

// Reset clears current stats.
func (w *DataLoaderStatWrapper) Reset() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.called = &DataLoaderStatValue{}
	w.returnEmptyKey = &DataLoaderStatValue{}
	w.returnNilData = &DataLoaderStatValue{}
	w.returnData = &DataLoaderStatValue{}
}

// Stat returns information about the data loader.
func (w *DataLoaderStatWrapper) Stat() *DataLoaderStat {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return &DataLoaderStat{
		Called:         *w.called,
		ReturnEmptyKey: *w.returnEmptyKey,
		ReturnNilData:  *w.returnNilData,
		ReturnData:     *w.returnData,
	}
}

// Name ensures that this implements the Describer interface.
func (w *DataLoaderStatWrapper) Name() string {
	if d, ok := w.Loader.(Describer); ok {
		return d.Name()
	}
	return "Anonymous"
}

// Description ensures that this implements the Describer interface.
func (w *DataLoaderStatWrapper) Description() string {
	if d, ok := w.Loader.(Describer); ok {
		return d.Description()
	}
	return fmt.Sprintf("Anonymous %T", w.Loader)
}

// DataLoaderStat is returned from the DataLoaderStatWrapper.Stat method.
type DataLoaderStat struct {
	Called         DataLoaderStatValue `json:"called"`
	ReturnEmptyKey DataLoaderStatValue `json:"returnEmptyKey"`
	ReturnNilData  DataLoaderStatValue `json:"returnNilData"`
	ReturnData     DataLoaderStatValue `json:"returnData"`
}

// LoadData will load data for item concurrently for
// each loader provided. Data is assigned to the item in
// the same order as the arguments provided to this function.
func LoadData(item *Item, loaders ...DataLoader) {
	type data struct {
		k string
		v interface{}
	}
	wg := sync.WaitGroup{}
	newData := make([]*data, len(loaders))
	for offset, loader := range loaders {
		wg.Add(1)
		go func(offset int, loader DataLoader) {
			defer wg.Done()
			k, v := loader.LoadData(item)
			newData[offset] = &data{
				k: k,
				v: v,
			}
		}(offset, loader)
	}
	wg.Wait()
	for _, d := range newData {
		// We still assign nil data values to the map but not empty an key.
		if d.k != "" {
			item.Data[d.k] = d.v
		}
	}
}
