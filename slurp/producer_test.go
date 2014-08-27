package slurp

import "testing"

func TestProductionRunFunc(t *testing.T) {
	var (
		f    ProductionRunFunc
		fArg chan<- *Item
	)
	f = func(items chan<- *Item) {
		fArg = items
	}
	ch := make(chan *Item, 0)
	f.SendItems(ch)
	if fArg != ch {
		t.Error("Expecting to have the chan passed to the func")
	}
}
