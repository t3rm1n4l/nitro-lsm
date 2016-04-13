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

type diskWriter struct {
	shard      int
	w          *Writer
	rbuf, wbuf []byte
}

func (m *MemDB) newDiskWriter(shard int) *diskWriter {
	return &diskWriter{
		rbuf:  make([]byte, blockSize),
		wbuf:  make([]byte, blockSize),
		w:     m.NewWriter(),
		shard: shard,
	}
}

type ItemOp struct {
	bs []byte
	op int
}

func (dw *diskWriter) batchModifyCallback(n *skiplist.Node, ops []skiplist.BatchOp) error {
	var err error
	var indexItem []byte
	var nodeItems [][]byte

	if n.Item() != skiplist.MinItem {
		dw.w.DeleteNode(n)
		err := dw.w.bm.ReadBlock(blockPtr(n.DataPtr), dw.rbuf)
		if err != nil {
			return err
		}

		nodeItems = newDataBlock(dw.rbuf).GetItems()
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

// TODO: Support multiple shards
func (m *MemDB) BatchModify(ops []ItemOp) error {
	sops := make([]skiplist.BatchOp, len(ops))
	for i, op := range ops {
		x := m.newItem(op.bs, m.useMemoryMgmt)
		x.bornSn = m.getCurrSn()
		sops[i].Itm = unsafe.Pointer(x)
		sops[i].Flag = op.op
	}

	return m.store.ExecBatchOps(sops, m.shardWrs[0].batchModifyCallback, m.insCmp, &m.store.Stats)
}
