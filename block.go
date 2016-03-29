package memdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	errBlockFull = errors.New("Block full")
)

type blockPtr uint64

type dataBlock struct {
	buf    []byte
	offset int
}

func newDataBlock(bs []byte) *dataBlock {
	return &dataBlock{
		buf: bs[:cap(bs)],
	}
}

func (db *dataBlock) GetItems() [][]byte {
	var itms [][]byte

	for offset := 0; offset+2 < blockSize; {
		l := int(binary.BigEndian.Uint16(db.buf[offset : offset+2]))
		if l == 0 {
			break
		}
		offset += 2
		itms = append(itms, db.buf[offset:offset+l])
		offset += l
	}

	return itms
}

func (db *dataBlock) Write(itm []byte) error {
	newLen := db.offset + 2 + len(itm)
	if newLen > len(db.buf) {
		return errBlockFull
	}

	binary.BigEndian.PutUint16(db.buf[db.offset:db.offset+2], uint16(len(itm)))
	db.offset += 2
	copy(db.buf[db.offset:db.offset+len(itm)], itm)
	db.offset += len(itm)

	return nil
}

func (db *dataBlock) IsEmpty() bool {
	return db.offset == 0
}

func (db *dataBlock) Reset() {
	db.offset = 0
}

func (db *dataBlock) Bytes() []byte {
	return db.buf[:db.offset]
}

type DiskWriter struct {
	w        *Writer
	woffset  int64
	wfd, rfd *os.File

	rbuf, wbuf []byte
}

func (m *MemDB) NewDiskWriter() (*DiskWriter, error) {
	var err error

	if m.blockStoreDir == "" {
		return nil, errors.New("data filepath is not configured")
	}

	dw := &DiskWriter{
		rbuf: make([]byte, blockSize),
		wbuf: make([]byte, blockSize),
	}

	path := filepath.Join(m.blockStoreDir, "blockstore.data")
	dw.wfd, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	dw.woffset, err = dw.wfd.Seek(0, 2)
	if err != nil {
		dw.wfd.Close()
		return nil, err
	}

	dw.woffset += dw.woffset % blockSize

	dw.rfd, err = os.Open(path)
	if err != nil {
		dw.wfd.Close()
		return nil, err
	}
	dw.w = m.NewWriter()

	return dw, nil
}

func (w *DiskWriter) writeBlock(bs []byte) (blockPtr, error) {
	_, err := w.wfd.WriteAt(bs, w.woffset)
	if err != nil {
		return 0, err
	}
	bptr := blockPtr(w.woffset)
	w.woffset += blockSize

	return bptr, nil
}

func (w *DiskWriter) readBlock(bptr blockPtr) ([]byte, error) {
	_, err := w.rfd.ReadAt(w.rbuf, int64(bptr))
	return w.rbuf, err
}

func (w *DiskWriter) deleteBlock(bptr blockPtr) error {
	return nil
}
