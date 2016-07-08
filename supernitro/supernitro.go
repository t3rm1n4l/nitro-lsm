package supernitro

import (
	"fmt"
	"github.com/t3rm1n4l/nitro"
	"sync"
)

type Config struct {
	MaxMStoreSize  int64
	BlockstorePath string
}

func DefaultConfig() Config {
	return Config{
		MaxMStoreSize:  1 * 1024 * 1024,
		BlockstorePath: ".",
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
}

func New() *SuperNitro {
	return &SuperNitro{
		Config: DefaultConfig(),
		mstore: nitro.New(),
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
	msnap.Open()
	go func() {
		defer msnap.Close()
		// Perform merge operation
		itr := msnap.NewIterator()
		defer itr.Close()
		if m.dstore == nil {
			dcfg := nitro.DefaultConfig()
			dcfg.SetBlockStoreDir(m.Config.BlockstorePath)
			m.dstore = nitro.NewWithConfig(dcfg)
		}

		opItr := nitro.NewOpIterator(itr)
		m.dstore.BatchModify(opItr)
		dsnap, err := m.dstore.NewSnapshot()
		if err != nil {
			panic(err)
		}

		m.Lock()

		for _, snap := range m.snaps {
			snap.Close()
		}

		m.snaps = []*nitro.Snapshot{dsnap}
		m.Unlock()
		store.Close()
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
	snap.snaps = append(snap.snaps, msnap)
	snap.snaps = append(snap.snaps, m.snaps...)

	fmt.Println("newsnap", m.mstore.MemoryInUse(), m.MaxMStoreSize, len(m.snaps))
	if m.mstore.MemoryInUse() > m.MaxMStoreSize && len(m.snaps) < 2 {
		snap.Open()
		m.snaps = append([]*nitro.Snapshot{msnap}, m.snaps...)
		mstoreOld := m.mstore
		m.mstore = nitro.New()
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
