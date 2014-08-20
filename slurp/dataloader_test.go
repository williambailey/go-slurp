package slurp

import (
	"testing"
	"time"
)

type simpleDataLoader struct {
	k string
	v interface{}
}

func (l *simpleDataLoader) LoadData(*Item) (string, interface{}) {
	return l.k, l.v
}

func TestLoadData(t *testing.T) {
	l1 := &simpleDataLoader{
		k: "l1",
		v: 1,
	}
	l2 := &simpleDataLoader{
		k: "l2",
		v: "two",
	}
	l3 := &simpleDataLoader{
		k: "l3",
		v: nil,
	}
	l4 := &simpleDataLoader{
		k: "",
		v: 4,
	}
	l5a := &simpleDataLoader{
		k: "l5",
		v: "a",
	}
	l5b := &simpleDataLoader{
		k: "l5",
		v: "b",
	}
	i := NewItem(time.Now())
	LoadData(i, l1, l2, l3, l4, l5a, l5b)
	v, ok := i.Data["l1"]
	if !ok {
		t.Error("Expecting l1 to exist in item.Data.")
	} else if v != 1 {
		t.Errorf("Expecting l1 value to be 1, got %v.", v)
	}
	v, ok = i.Data["l2"]
	if !ok {
		t.Error("Expecting l2 to exist in item.Data.")
	} else if v != "two" {
		t.Errorf("Expecting l2 value to be \"two\", got %v.", v)
	}
	v, ok = i.Data["l3"]
	if !ok {
		t.Error("Expecting l3 to exist in item.Data.")
	} else if v != nil {
		t.Errorf("Expecting l3 value to be nil, got %v.", v)
	}
	_, ok = i.Data["l4"]
	if ok {
		t.Error("Not expecting to see l4 in item.Data.")
	}
	v, ok = i.Data["l5"]
	if !ok {
		t.Error("Expecting l5 to exist in item.Data.")
	} else if v != "b" {
		t.Errorf("Expecting l5 value to be \"b\", got %v.", v)
	}
}
