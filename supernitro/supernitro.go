package supernitro

import (
	"github.com/t3rm1n4l/nitro"
)

type Config struct {
	MaxMStoreSize int
}

func DefaultConfig() Config {
	return Config{
		MaxMStoreSize: 100 * 1024 * 1024,
	}
}

type SuperNitro struct {
	Config
	mstore *nitro.Nitro
	dstore *nitro.Nitro

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
	msnap *nitro.Snapshot
	dsnap *nitro.Snapshot
}

func (s *Snapshot) Open() bool {
	if !s.msnap.Open() {
		return false
	}

	if s.dsnap != nil && !s.dsnap.Open() {
		return false
	}

	return true
}

func (s *Snapshot) Close() {
	s.msnap.Close()
	if s.dsnap != nil {
		s.dsnap.Close()
	}
}

func (m *SuperNitro) NewSnapshot() (*Snapshot, error) {
	msnap, err := m.mstore.NewSnapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		msnap: msnap,
	}, nil
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
