package memdb

import (
	"bytes"
	"github.com/t3rm1n4l/memdb/skiplist"
	"unsafe"
)

const blockSize = 4096
const (
	itemDeleteOp = iota
	itemInsertop
)

type ItemOp struct {
	bs []byte
	op int
}

func (dw *DiskWriter) batchModifyCallback(n *skiplist.Node, ops []skiplist.BatchOp) error {
	var err error
	var indexItem []byte
	var nodeItems [][]byte

	if n.Item() != skiplist.MinItem {
		dw.w.DeleteNode(n)
		bs, err := dw.readBlock(blockPtr(n.DataPtr))
		if err != nil {
			return err
		}

		nodeItems = newDataBlock(bs).GetItems()
	}

	wblock := newDataBlock(dw.wbuf)

	flushBlock := func() error {
		bptr, err := dw.writeBlock(wblock.Bytes())
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

	opi, ni := 0, 0
	for err == nil && opi < len(ops) && ni < len(nodeItems) {
		nItm := nodeItems[ni]
		opItm := (*Item)(ops[opi].Itm).Bytes()
		cmpval := bytes.Compare(nItm, opItm)
		switch {
		case cmpval < 0:
			err = doWriteItem(nItm)
			ni++
			break
		case cmpval == 0:
			if ops[opi].Flag == itemInsertop {
				err = doWriteItem(opItm)
			}

			opi++
			ni++
			break
		default:
			if ops[opi].Flag == itemInsertop {
				err = doWriteItem(opItm)
				opi++
			}
		}
	}

	for ; err == nil && opi < len(ops); opi++ {
		if ops[opi].Flag == itemInsertop {
			opItm := (*Item)(ops[opi].Itm).Bytes()
			err = doWriteItem(opItm)
		}
	}

	for ; err == nil && ni < len(nodeItems); ni++ {
		nItm := nodeItems[ni]
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

func (dw *DiskWriter) BatchModify(ops []ItemOp) error {
	sops := make([]skiplist.BatchOp, len(ops))
	for i, op := range ops {
		x := dw.w.newItem(op.bs, dw.w.useMemoryMgmt)
		x.bornSn = dw.w.getCurrSn()
		sops[i].Itm = unsafe.Pointer(x)
		sops[i].Flag = op.op
	}

	return dw.w.store.ExecBatchOps(sops, dw.batchModifyCallback, dw.w.insCmp, &dw.w.store.Stats)
}
