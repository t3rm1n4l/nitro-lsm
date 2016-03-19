package skiplist

import "testing"
import "fmt"
import "unsafe"
import "math/rand"
import "sort"

var hash map[uintptr][]unsafe.Pointer
var maxItems = 1000

func init() {
	hash = make(map[uintptr][]unsafe.Pointer)
}

func batchOps(itms []unsafe.Pointer) (ops []BatchOp) {
	for _, itm := range itms {
		ops = append(ops, BatchOp{itm: itm})
	}

	return
}

func TestBatchInsert(t *testing.T) {
	s := New()
	cmp := CompareBytes
	buf := s.MakeBuf()
	defer s.FreeBuf(buf)
	var items []unsafe.Pointer

	sr := itemSorter{
		cmp: cmp,
	}

	for i := 0; i < 5000000; i++ {
		items = append(items, NewByteKeyItem([]byte(fmt.Sprintf("%010d", rand.Int()%1000000000))))
	}

	sr.itms = items
	sort.Sort(sr)

	s.ExecBatchOps(batchOps(sr.itms), buildLeafCallback, cmp, &s.Stats)

	dumpIt(s, cmp)
	items = nil
	for i := 0; i < 100000; i++ {
		items = append(items, NewByteKeyItem([]byte(fmt.Sprintf("%010d", rand.Int()%1000000000))))
	}

	sr.itms = items
	sort.Sort(sr)
	s.ExecBatchOps(batchOps(sr.itms), buildLeafCallback, cmp, &s.Stats)

	dumpIt(s, cmp)

	fmt.Println(s.GetStats())
}

func dumpIt(s *Skiplist, cmp CompareFn) {
	buf := s.MakeBuf()
	itr := s.NewIterator(cmp, buf)
	itr.SeekFirst()
	last := NewByteKeyItem([]byte("0"))
	for ; itr.Valid(); itr.Next() {
		itms := hash[uintptr(unsafe.Pointer(itr.GetNode()))]
		for _, itm := range itms {
			if cmp(last, itm) > 0 {
				panic(fmt.Sprintf("prob - %s > %s", string(*(*byteKeyItem)(last)), string(*(*byteKeyItem)(itm))))
			}
			last = itm
		}
	}
}

type itemSorter struct {
	itms []unsafe.Pointer
	cmp  CompareFn
}

func (s itemSorter) Len() int {
	return len(s.itms)
}

func (s itemSorter) Swap(i, j int) {
	s.itms[i], s.itms[j] = s.itms[j], s.itms[i]
}

func (s itemSorter) Less(i, j int) bool {
	return s.cmp(s.itms[i], s.itms[j]) < 0
}

func (s *itemSorter) Dedup() {
	j := 0
	i := 0
	for i = 0; i < len(s.itms); {
		for j < len(s.itms) && s.cmp(s.itms[i], s.itms[j]) == 0 {
			j++
		}
		i++
		if i < len(s.itms) && j < len(s.itms) {
			s.itms[i] = s.itms[j]
		} else {
			break
		}

	}

	s.itms = s.itms[:i]
}

func printItems(itms []unsafe.Pointer) {
	for _, itm := range itms {
		fmt.Println(string(*(*byteKeyItem)(itm)))
	}
}

func printBlock(itms []unsafe.Pointer) {
	fmt.Println("(", string(*(*byteKeyItem)(itms[0])), string(*(*byteKeyItem)(itms[len(itms)-1])), ")", len(itms))
}

func printItem(itm unsafe.Pointer) string {
	if itm == minItem {
		return "min"
	} else if itm == maxItem {
		return "max"
	}
	return string(*(*byteKeyItem)(itm))
}

func buildLeafCallback(s *Skiplist, node *Node, ops []BatchOp, cmp CompareFn) error {
	buf := s.MakeBuf()
	var items []unsafe.Pointer
	for _, op := range ops {
		items = append(items, op.itm)
	}

	var block []unsafe.Pointer
	if node.Item() != nil {
		ptr := uintptr(unsafe.Pointer(node))
		items = append(items, hash[ptr]...)

		sr := itemSorter{
			cmp:  cmp,
			itms: items,
		}
		sort.Sort(sr)
		sr.Dedup()
		items = sr.itms

		s.Delete(node.Item(), cmp, buf, &s.Stats)
		delete(hash, ptr)
	}

	for len(items) > 0 {
		if len(items) >= maxItems {
			block = items[:maxItems]
			items = items[maxItems:]
		} else {
			block = items
			items = nil
		}

		k := block[0]
		newnode, _ := s.Insert2(k, cmp, nil, buf, rand.Float32, &s.Stats)
		hash[uintptr(unsafe.Pointer(newnode))] = block
	}

	return nil
}
