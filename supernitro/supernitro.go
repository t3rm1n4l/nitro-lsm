package supernitro

import (
	"fmt"
	"github.com/t3rm1n4l/nitro"
	"github.com/t3rm1n4l/nitro/mm"
	"sync"
	"sync/atomic"
)

type Config struct {
	MaxMStoreSize  int64
	BlockstorePath string
	NitroConfig    nitro.Config
}

func DefaultConfig() Config {
	ncfg := nitro.DefaultConfig()
	ncfg.UseMemoryMgmt(mm.Malloc, mm.Free)

	return Config{
		MaxMStoreSize:  1 * 1024 * 1024,
		BlockstorePath: ".",
		NitroConfig:    ncfg,
	}
}

type SuperNitro struct {
	sync.Mutex
	Config
	mstore *nitro.Nitro
	dstore *nitro.Nitro

	// Lower level immutable snapshots
	snaps []*nitro.Snapshot

	wrlist []*Writer

	isMergeRunning int32
}

func New() *SuperNitro {
	return &SuperNitro{
		Config: DefaultConfig(),
		mstore: nitro.NewWithConfig(DefaultConfig().NitroConfig),
	}
}

func (m *SuperNitro) NewWriter() *Writer {
	w := &Writer{
		SuperNitro: m,
		mw:         m.mstore.NewWriter(),
	}

	m.wrlist = append(m.wrlist, w)
	return w
}

type Snapshot struct {
	snaps []*nitro.Snapshot
}

func (s *Snapshot) Open() bool {
	for i, snap := range s.snaps {
		if !snap.Open() {
			for x := 0; x < i; x++ {
				s.snaps[x].Close()
			}
			return false
		}
	}

	return true
}

func (s *Snapshot) Close() {
	for _, snap := range s.snaps {
		snap.Close()
	}
}

func (m *SuperNitro) NewIterator(snap *Snapshot) *Iterator {
	var iters []*nitro.Iterator
	for _, snap := range snap.snaps {
		iters = append(iters, snap.NewIterator())
	}
	return newMergeIterator(iters)
}

func (m *SuperNitro) execMerge(msnap *nitro.Snapshot, store *nitro.Nitro) {
	fmt.Println("execMerge")
	go func() {
		defer store.Close()
		// Perform merge operation
		itr := msnap.NewIterator()
		defer itr.Close()
		if m.dstore == nil {
			dcfg := m.Config.NitroConfig
			dcfg.SetBlockStoreDir(m.Config.BlockstorePath)
			m.dstore = nitro.NewWithConfig(dcfg)
		}

		opItr := nitro.NewOpIterator(itr)
		m.dstore.BatchModify(opItr)
		dsnap, err := m.dstore.NewSnapshot()
		if err != nil {
			panic(err)
		}

		func() {
			m.Lock()
			defer m.Unlock()

			for _, snap := range m.snaps {
				snap.Close()
			}

			m.snaps = []*nitro.Snapshot{dsnap}
			atomic.CompareAndSwapInt32(&m.isMergeRunning, 1, 0)
		}()
	}()
}

func (m *SuperNitro) NewSnapshot() (*Snapshot, error) {
	m.Lock()
	defer m.Unlock()

	snap := &Snapshot{}
	msnap, err := m.mstore.NewSnapshot()
	if err != nil {
		return nil, err
	}
	snaps := append([]*nitro.Snapshot{msnap}, m.snaps...)
	for _, snap := range m.snaps {
		snap.Open()
	}
	snap.snaps = snaps

	fmt.Println("newsnap", m.mstore.MemoryInUse(), m.MaxMStoreSize, len(m.snaps))
	if m.mstore.MemoryInUse() > m.MaxMStoreSize && atomic.CompareAndSwapInt32(&m.isMergeRunning, 0, 1) {
		msnap.Open()
		m.snaps = snaps
		mstoreOld := m.mstore
		m.mstore = nitro.NewWithConfig(m.Config.NitroConfig)
		for _, wr := range m.wrlist {
			wr.mw = m.mstore.NewWriter()
		}
		m.execMerge(msnap, mstoreOld)
	}

	return snap, nil
}

func (m *SuperNitro) Close() {
	for _, snap := range m.snaps {
		snap.Close()
	}
	m.mstore.Close()
	if m.dstore != nil {
		m.dstore.Close()
	}
}

type Writer struct {
	*SuperNitro
	mw *nitro.Writer
}

func (w *Writer) Put(bs []byte) bool {
	return w.mw.Put2(bs) != nil
}

func (w *Writer) Delete(bs []byte) bool {
	return w.mw.DeleteNonExist(bs)
}
