package memdb

import (
	"bytes"
	"encoding/binary"
	//	"fmt"
	"github.com/t3rm1n4l/memdb/skiplist"
	//	"reflect"
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

func (w *Writer) readBlock(dptr uint64) ([][]byte, error) {
	var itms [][]byte
	if dptr == 0 {
		return nil, nil
	}

	b := make([]byte, blockSize)

	w.rfd.Seek(int64(dptr), 0)
	w.rfd.Read(b)
	/*
		n, err := w.rfd.Read(b)
			if n != blockSize {
				panic(fmt.Sprintf("err %v %v", err, n))
			}
	*/

	offset := 0
	for offset+2 < blockSize {
		l := int(binary.BigEndian.Uint16(b[offset : offset+2]))
		if l == 0 {
			break
		}
		offset += 2
		itms = append(itms, b[offset:offset+l])
		offset += l
	}

	return itms, nil
}

func (w *Writer) writeBlock(block []byte) (uint64, error) {
	w.fd.Seek(int64(w.offset), 0)
	w.fd.Write(block)
	dp := w.offset
	w.offset += blockSize

	/*
		b := append([]byte(nil), block...)
		sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
		return uint64(sh.Data), nil
	*/

	return uint64(dp), nil
}

// mark node as delete
// gc should also punch hole into deleted block

func (w *Writer) batchModifyCallback(n *skiplist.Node, ops []skiplist.BatchOp) error {
	var indexItem []byte

	// min value
	if n.Item() != nil {
		w.DeleteNode(n)
	}
	nodeItems, err := w.readBlock(n.DataPtr)

	var blocksWritten int
	lenBuf := make([]byte, 2)
	blockBuf := bytes.NewBuffer(make([]byte, blockSize))
	blockBuf.Reset()

	doWriteItem := func(itm []byte, forceFlush bool) error {
	repeat:
		if indexItem == nil {
			indexItem = itm
		}

		if blockBuf.Len()+len(itm)+2 > blockBuf.Cap() || (forceFlush && blockBuf.Len() > 0) {
			dptr, err := w.writeBlock(blockBuf.Bytes())
			if err != nil {
				return err
			}
			newNode := w.Put2(indexItem)
			if newNode == nil {
				panic("hell")
			}
			newNode.DataPtr = dptr
			indexItem = nil
			blockBuf.Reset()
			blocksWritten++
			goto repeat
		}

		binary.BigEndian.PutUint16(lenBuf, uint16(len(itm)))
		blockBuf.Write(lenBuf)
		blockBuf.Write(itm)

		return nil
	}

	opi, ni := 0, 0
	for err == nil && opi < len(ops) && ni < len(nodeItems) {
		nItm := nodeItems[ni]
		opItm := (*Item)(ops[opi].Itm).Bytes()
		cmpval := bytes.Compare(nItm, opItm)
		switch {
		case cmpval < 0:
			err = doWriteItem(nItm, false)
			ni++
			break
		default:
			if ops[opi].Flag == itemInsertop {
				err = doWriteItem(opItm, false)
				opi++
			} else {
				ni++
			}
		}
	}

	for ; err == nil && opi < len(ops); opi++ {
		if ops[opi].Flag == itemInsertop {

			opItm := (*Item)(ops[opi].Itm).Bytes()
			err = doWriteItem(opItm, false)
		}
	}

	for ; err == nil && ni < len(nodeItems); ni++ {
		nItm := nodeItems[ni]
		err = doWriteItem(nItm, false)
	}

	if err != nil {
		return err
	}

	return doWriteItem(nil, true)
}

// pass randfn
func (w *Writer) BatchModify(ops []ItemOp) error {
	sops := make([]skiplist.BatchOp, len(ops))
	for i, op := range ops {
		x := w.newItem(op.bs, w.useMemoryMgmt)
		x.bornSn = w.getCurrSn()
		sops[i].Itm = unsafe.Pointer(x)
		sops[i].Flag = op.op
	}

	return w.store.ExecBatchOps(sops, w.batchModifyCallback, w.insCmp, &w.store.Stats)
}
