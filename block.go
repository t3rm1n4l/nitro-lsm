package memdb

import (
	"encoding/binary"
	"errors"
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
