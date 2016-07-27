package supernitro

import (
	"fmt"
	"github.com/t3rm1n4l/nitro"
	"github.com/t3rm1n4l/nitro/mm"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	MaxMStoreSize  int64
	BlockstorePath string
	NitroConfig    nitro.Config
	Writers        int
}

func DefaultConfig() Config {
	ncfg := nitro.DefaultConfig()
	ncfg.UseMemoryMgmt(mm.Malloc, mm.Free)

	return Config{
		MaxMStoreSize:  1 * 1024 * 1024,
		BlockstorePath: ".",
		NitroConfig:    ncfg,
		Writers:        8,
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
		if m.dstore == nil {
			dcfg := m.Config.NitroConfig
			dcfg.SetBlockStoreDir(m.Config.BlockstorePath)
			m.dstore = nitro.NewWithConfig(dcfg)
		}

		// Perform merge operation
		t0 := time.Now()
		stats, err := m.dstore.ApplyOps(msnap, m.Config.Writers)
		if err != nil {
			panic(err)
		}
		dur := time.Since(t0)
		fmt.Printf("\nexecMergeStats: took %v (%v items/sec)\n================\n%s\n\n", dur, float64(stats.ItemsInserted)/float64(dur.Seconds()), stats)
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

func (m *SuperNitro) Sync() error {
	for !atomic.CompareAndSwapInt32(&m.isMergeRunning, 0, 1) {
		time.Sleep(time.Millisecond)
	}
	atomic.CompareAndSwapInt32(&m.isMergeRunning, 1, 0)

	snap, err := m.newSnapshot(0)
	if err == nil {
		snap.Close()
	}

	for !atomic.CompareAndSwapInt32(&m.isMergeRunning, 0, 1) {
		time.Sleep(time.Millisecond)
	}
	atomic.CompareAndSwapInt32(&m.isMergeRunning, 1, 0)
	return err
}

func (m *SuperNitro) NewSnapshot() (*Snapshot, error) {
	return m.newSnapshot(m.MaxMStoreSize)
}

func (m *SuperNitro) newSnapshot(MaxMStoreSize int64) (*Snapshot, error) {
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
	if m.mstore.MemoryInUse() > MaxMStoreSize && atomic.CompareAndSwapInt32(&m.isMergeRunning, 0, 1) {
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
