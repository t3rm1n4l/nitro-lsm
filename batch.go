package nitro

import (
	"bytes"
	"fmt"
	"github.com/t3rm1n4l/nitro/skiplist"
	"unsafe"
)

const blockSize = 4096

type itemOp int

const (
	itemDeleteOp itemOp = iota
	itemInsertop
)

type diskWriter struct {
	shard      int
	w          *Writer
	rbuf, wbuf []byte

	stats BatchOpStats
}

type BatchOpStats struct {
	BlocksWritten int64
	BlocksRemoved int64

	ItemsInserted int64
	ItemsRemoved  int64
}

func (b BatchOpStats) String() string {
	return fmt.Sprintf(
		"blocks_written = %d\n"+
			"blocks_removed = %d\n"+
			"items_inserted = %d\n"+
			"items_removed  = %d",
		b.BlocksWritten, b.BlocksRemoved, b.ItemsInserted, b.ItemsRemoved)
}

func (r *BatchOpStats) ApplyDiff(a, b BatchOpStats) {
	r.BlocksWritten += a.BlocksWritten - b.BlocksWritten
	r.BlocksRemoved += a.BlocksRemoved - b.BlocksRemoved
	r.ItemsInserted += a.ItemsInserted - b.ItemsInserted
	r.ItemsRemoved += a.ItemsRemoved - b.ItemsRemoved
}

func (m *Nitro) newDiskWriter(shard int) *diskWriter {
	return &diskWriter{
		rbuf:  make([]byte, blockSize),
		wbuf:  make([]byte, blockSize),
		w:     m.NewWriter(),
		shard: shard,
	}
}

type nodeOpIterator struct {
	*Iterator
}

func NewOpIterator(itr *Iterator) BatchOpIterator {
	it := &nodeOpIterator{
		Iterator: itr,
	}

	return it
}

func (it *nodeOpIterator) Item() unsafe.Pointer {
	return it.Iterator.GetNode().Item()
}

func (it *nodeOpIterator) Next() {
	it.Iterator.Next()
}

func (it *nodeOpIterator) Op() itemOp {
	itm := (*Item)(it.Iterator.GetNode().Item())
	if itm.bornSn != 0 {
		return itemInsertop
	} else {
		return itemDeleteOp
	}
}

func (it *nodeOpIterator) Close() {
	it.Iterator.Close()
}

type BatchOpIterator interface {
	skiplist.BatchOpIterator
	Op() itemOp
	Close()
}

func (dw *diskWriter) batchModifyCallback(n *skiplist.Node, cmp skiplist.CompareFn,
	maxItem unsafe.Pointer, sOpItr skiplist.BatchOpIterator) error {

	var err error
	var indexItem []byte
	var db *dataBlock

	opItr := sOpItr.(BatchOpIterator)

	if n.Item() != skiplist.MinItem {
		dw.w.DeleteNode(n)
		dw.stats.BlocksRemoved++
		err := dw.w.bm.ReadBlock(blockPtr(n.DataPtr), dw.rbuf)
		if err != nil {
			return err
		}
		db = newDataBlock(dw.rbuf)
	}

	wblock := newDataBlock(dw.wbuf)

	flushBlock := func() error {
		bptr, err := dw.w.bm.WriteBlock(wblock.Bytes(), dw.shard)
		if err == nil {
			indexNode := dw.w.Put2(indexItem)
			if indexNode == nil {
				panic("index node creation should not fail")
			}
			indexNode.DataPtr = uint64(bptr)
			wblock.Reset()
			dw.stats.BlocksWritten++
		}

		return err
	}

	doWriteItem := func(itm []byte) error {
		if indexItem == nil {
			indexItem = itm
		}

		if err := wblock.Write(itm); err == errBlockFull {
			if err := flushBlock(); err != nil {
				return err
			}

			indexItem = itm
			return wblock.Write(itm)
		}

		return nil
	}

	var nItm []byte
	for nItm = db.Get(); err == nil && opItr.Valid() &&
		skiplist.Compare(cmp, opItr.Item(), maxItem) < 0 && nItm != nil; {
		opItm := (*Item)(opItr.Item()).Bytes()
		cmpval := bytes.Compare(nItm, opItm)
		switch {
		case cmpval < 0:
			err = doWriteItem(nItm)
			nItm = db.Get()
			break
		case cmpval == 0:
			if opItr.Op() == itemInsertop {
				err = doWriteItem(opItm)
			} else {
				dw.stats.ItemsRemoved++
			}

			opItr.Next()
			nItm = db.Get()
			break
		default:
			if opItr.Op() == itemInsertop {
				err = doWriteItem(opItm)
				dw.stats.ItemsInserted++
				opItr.Next()
			}
		}
	}

	for ; err == nil && opItr.Valid() &&
		skiplist.Compare(cmp, opItr.Item(), maxItem) < 0; opItr.Next() {

		if opItr.Op() == itemInsertop {
			opItm := (*Item)(opItr.Item()).Bytes()
			err = doWriteItem(opItm)
			dw.stats.ItemsInserted++
		}
	}

	for ; err == nil && nItm != nil; nItm = db.Get() {
		err = doWriteItem(nItm)
	}

	if err != nil {
		return err
	}

	if !wblock.IsEmpty() {
		return flushBlock()
	}

	return nil
}

type batchOpIterator struct {
	db *Nitro
	BatchOpIterator
	itm unsafe.Pointer
}

func (it *batchOpIterator) fillItem() {
	srcItm := (*Item)(it.BatchOpIterator.Item())
	l := len(srcItm.Bytes())
	dstItm := it.db.allocItem(l, false)
	copy(dstItm.Bytes(), srcItm.Bytes())
	dstItm.bornSn = it.db.getCurrSn()
	it.itm = unsafe.Pointer(dstItm)
}

func (it *batchOpIterator) Next() {
	it.BatchOpIterator.Next()
	if it.BatchOpIterator.Valid() {
		it.fillItem()
	}
}

func (it *batchOpIterator) Item() unsafe.Pointer {
	return it.itm
}

func isValidNode(n *skiplist.Node) bool {
	itm := n.Item()

	// TODO: move this check to skiplist module
	if itm != skiplist.MaxItem {
		return (*Item)(itm).deadSn == 0
	}

	return true
}

func (m *Nitro) newBatchOpIterator(it *Iterator) BatchOpIterator {
	bItr := &batchOpIterator{
		db:              m,
		BatchOpIterator: NewOpIterator(it),
	}

	if bItr.Valid() {
		bItr.fillItem()
	}
	return bItr
}

func (m *Nitro) ApplyOps(snap *Snapshot, concurr int) (BatchOpStats, error) {
	var err error
	var stats BatchOpStats

	w := m.NewWriter()
	currSnap := &Snapshot{db: m, sn: m.getCurrSn(), refCount: 1}
	pivots := m.partitionPivots(currSnap, concurr)

	beforeStats := make([]BatchOpStats, len(pivots)-1)
	errors := make([]chan error, len(pivots)-1)

	for i := 0; i < len(pivots)-1; i++ {
		errors[i] = make(chan error, 1)
		beforeStats[i] = m.shardWrs[i].stats

		itr := snap.NewIterator()
		itr.Seek(pivots[i].Bytes())
		itr.SetEnd(pivots[i+1].Bytes())
		opItr := m.newBatchOpIterator(itr)
		defer opItr.Close()
		head := w.GetNode(pivots[i].Bytes())
		tail := w.GetNode(pivots[i+1].Bytes())

		if pivots[i] == nil {
			head = nil
		}

		if pivots[i+1] == nil {
			tail = nil
		}

		go func(id int, opItr BatchOpIterator, head, tail *skiplist.Node) {
			errors[id] <- m.store.ExecBatchOps(opItr, head, tail, m.shardWrs[id].batchModifyCallback, m.insCmp, isValidNode, &m.store.Stats)
		}(i, opItr, head, tail)
	}

	for i := 0; i < len(pivots)-1; i++ {
		if e := <-errors[i]; e != nil {
			err = e
		}

		stats.ApplyDiff(m.shardWrs[i].stats, beforeStats[i])
	}

	return stats, err
}
