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

type ItemOp struct {
	bs []byte
	op itemOp
}

type batchOp struct {
	itm unsafe.Pointer
	op  itemOp
}

type batchOpIterator struct {
	offset int
	ops    []batchOp
}

func (it *batchOpIterator) Valid() bool {
	return it.offset < len(it.ops)
}

func (it *batchOpIterator) Next() {
	it.offset++
}

func (it *batchOpIterator) Item() unsafe.Pointer {
	return it.ops[it.offset].itm
}

func (it *batchOpIterator) Op() itemOp {
	return it.ops[it.offset].op
}

func (dw *diskWriter) batchModifyCallback(n *skiplist.Node, cmp skiplist.CompareFn,
	maxItem unsafe.Pointer, sOpItr skiplist.BatchOpIterator) error {

	var err error
	var indexItem []byte
	var db *dataBlock

	opItr := sOpItr.(*batchOpIterator)

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

// TODO: Support multiple shards
func (m *Nitro) BatchModify(ops []ItemOp) error {
	sops := make([]batchOp, len(ops))
	for i, op := range ops {
		x := m.newItem(op.bs, m.useMemoryMgmt)
		x.bornSn = m.getCurrSn()
		sops[i].itm = unsafe.Pointer(x)
		sops[i].op = op.op
	}

	opItr := &batchOpIterator{
		ops: sops,
	}

	return m.store.ExecBatchOps(opItr, m.shardWrs[0].batchModifyCallback, m.insCmp, &m.store.Stats)
}
