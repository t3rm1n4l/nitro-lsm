package nitro

import (
	"bytes"
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

	itr.SeekFirst()
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
			}

			opItr.Next()
			nItm = db.Get()
			break
		default:
			if opItr.Op() == itemInsertop {
				err = doWriteItem(opItm)
				opItr.Next()
			}
		}
	}

	for ; err == nil && opItr.Valid() &&
		skiplist.Compare(cmp, opItr.Item(), maxItem) < 0; opItr.Next() {

		if opItr.Op() == itemInsertop {
			opItm := (*Item)(opItr.Item()).Bytes()
			err = doWriteItem(opItm)
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

// TODO: Support multiple shards
func (m *Nitro) BatchModify(opItr BatchOpIterator) error {
	itr := &batchOpIterator{
		db:              m,
		BatchOpIterator: opItr,
	}

	if itr.Valid() {
		itr.fillItem()
	}
	return m.store.ExecBatchOps(itr, m.shardWrs[0].batchModifyCallback, m.insCmp, &m.store.Stats)
}
