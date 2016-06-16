package supernitro

import (
	"github.com/t3rm1n4l/nitro"
	"sync"
)

type Config struct {
	MaxMStoreSize int64
}

func DefaultConfig() Config {
	return Config{
		MaxMStoreSize: 100 * 1024 * 1024,
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
		m:  m,
		mw: m.mstore.NewWriter(),
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

func (m *SuperNitro) execMerge(msnap *nitro.Snapshot) {
	msnap.Open()
	go func() {
		defer msnap.Close()
		// Perform merge operation

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

	if m.mstore.MemoryInUse() > m.MaxMStoreSize && len(m.snaps) < 2 {
		m.snaps = append([]*nitro.Snapshot{msnap}, m.snaps...)
		m.mstore = nitro.New()
		m.execMerge(msnap)
		go m.mstore.Close()
	}

	return snap, nil
}

type Writer struct {
	m  *SuperNitro
	mw *nitro.Writer
}

func (w *Writer) Put(bs []byte) bool {
	return w.mw.Put2(bs) != nil
}

func (w *Writer) Delete(bs []byte) bool {
	return w.mw.DeleteNonExist(bs)
}
