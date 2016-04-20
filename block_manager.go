package memdb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// TODO: Reopen fds on error
type BlockManager interface {
	DeleteBlock(bptr blockPtr) error
	WriteBlock(bs []byte, shard int) (blockPtr, error)
	ReadBlock(bptr blockPtr, buf []byte) error
}

func newBlockPtr(shard int, off int64) blockPtr {
	off |= int64(shard) << 55
	return blockPtr(off)
}

func (ptr blockPtr) Offset() int64 {
	off := int64(ptr) & ^(0xff << 55)
	return off
}

func (ptr blockPtr) Shard() int {
	shard := int(int64(ptr) >> 55)
	return shard
}

type fileBlockManager struct {
	wlocks []sync.Mutex
	wfds   []*os.File
	rfds   []*os.File

	wpos []int64

	freeBlocks [][]int64
}

func newFileBlockManager(nfiles int, path string) (*fileBlockManager, error) {
	var fd *os.File
	var err error

	fbm := &fileBlockManager{}
	defer func() {
		if err != nil {
			for _, wfd := range fbm.wfds {
				wfd.Close()
			}
			for _, rfd := range fbm.rfds {
				rfd.Close()
			}
		}
	}()

	fbm.wlocks = make([]sync.Mutex, nfiles)
	fbm.wpos = make([]int64, nfiles)
	fbm.freeBlocks = make([][]int64, nfiles)

	for i := 0; i < nfiles; i++ {
		fpath := filepath.Join(path, fmt.Sprintf("blockstore-%d.data", i))
		fd, err = os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			return nil, err
		}
		fbm.wfds = append(fbm.wfds, fd)

		fbm.wpos[i], err = fd.Seek(0, 2)
		if err != nil {
			return nil, err
		}

		fbm.wpos[i] += fbm.wpos[i] % blockSize
		fd, err = os.Open(fpath)
		if err != nil {
			return nil, err
		}
		fbm.rfds = append(fbm.rfds, fd)
		fbm.freeBlocks[i] = make([]int64, 0)
	}

	return fbm, err
}

func (fbm *fileBlockManager) DeleteBlock(bptr blockPtr) error {
	shard := bptr.Shard()
	fbm.wlocks[shard].Lock()
	defer fbm.wlocks[shard].Unlock()
	fbm.freeBlocks[shard] = append(fbm.freeBlocks[shard], bptr.Offset())

	return nil
}

func (fbm *fileBlockManager) WriteBlock(bs []byte, shard int) (blockPtr, error) {
	shard = shard % len(fbm.wpos)
	fbm.wlocks[shard].Lock()
	var pos int64

	flist := fbm.freeBlocks[shard]
	if len(flist) > 0 {
		pos = flist[len(flist)-1]
		flist = flist[0 : len(flist)-1]
		fbm.freeBlocks[shard] = flist
	} else {
		pos = fbm.wpos[shard]
		fbm.wpos[shard] += blockSize
	}
	fbm.wlocks[shard].Unlock()

	_, err := fbm.wfds[shard].WriteAt(bs, pos)
	if err != nil {
		return 0, err
	}

	bptr := newBlockPtr(shard, pos)
	return bptr, nil
}

func (fbm *fileBlockManager) ReadBlock(bptr blockPtr, buf []byte) error {
	shard := bptr.Shard()
	n, err := fbm.rfds[shard].ReadAt(buf, bptr.Offset())
	if err == io.EOF {
		for ; n < len(buf); n++ {
			buf[n] = 0
		}
		err = nil
	}
	return err
}
