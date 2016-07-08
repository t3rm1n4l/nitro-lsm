package supernitro

import (
	"bytes"
	"container/heap"
	"github.com/t3rm1n4l/nitro"
)

type Iterator struct {
	iters []*nitro.Iterator
	h     itmHeap
	curr  []byte
}

func newMergeIterator(iters []*nitro.Iterator) *Iterator {
	return &Iterator{iters: iters}
}

type itmVal struct {
	iter *nitro.Iterator
	itm  []byte
	prio int
}

type itmHeap []itmVal

func (h itmHeap) Len() int { return len(h) }

func (h itmHeap) Less(i, j int) bool {
	val := bytes.Compare(h[i].itm, h[j].itm)
	if val == 0 && h[i].prio-h[j].prio < 0 {
		return true
	}

	return val < 0
}

func (h itmHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *itmHeap) Push(x interface{}) {
	*h = append(*h, x.(itmVal))
}

func (h *itmHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (it *Iterator) SeekFirst() {
	it.Seek(nil)
}

func (it *Iterator) Seek(itm []byte) {
	it.curr = nil
	it.h = nil
	for prio, subIt := range it.iters {
		if itm == nil {
			subIt.SeekFirst()
		} else {
			subIt.Seek(itm)
		}
		if subIt.Valid() {
			itm := subIt.Get()
			it.h = append(it.h, itmVal{iter: subIt, itm: itm, prio: prio})
		}
	}

	heap.Init(&it.h)
	it.Next()
}

func (it *Iterator) Valid() bool {
	return it.curr != nil
}

func (it *Iterator) Get() []byte {
	return it.curr
}

func (it *Iterator) Next() {
	var next []byte
	for next = it.next(); it.curr != nil && bytes.Equal(next, it.curr); next = it.next() {
	}

	it.curr = next
}

func (it *Iterator) next() []byte {
	if it.h.Len() == 0 {
		return nil
	}

	o := heap.Pop(&it.h)
	hi := o.(itmVal)
	curr := hi.itm
	hi.iter.Next()
	if hi.iter.Valid() {
		hi.itm = hi.iter.Get()
		heap.Push(&it.h, hi)
	}

	return curr
}

func (it *Iterator) Close() {
	for _, subIt := range it.iters {
		subIt.Close()
	}
}
