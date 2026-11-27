package strata

import (
	"io"
	"os"
)

type osFile struct{ f *os.File }

func (f *osFile) ReadAt(p []byte, off int64) (int, error)  { return f.f.ReadAt(p, off) }
func (f *osFile) WriteAt(p []byte, off int64) (int, error) { return f.f.WriteAt(p, off) }
func (f *osFile) Write(p []byte) (int, error)              { return f.f.Write(p) }
func (f *osFile) Sync() error                              { return f.f.Sync() }
func (f *osFile) Stat() (os.FileInfo, error)               { return f.f.Stat() }
func (f *osFile) Close() error {
	if f == nil || f.f == nil {
		return nil
	}
	return f.f.Close()
}
func (f *osFile) Fd() uintptr { return f.f.Fd() }

var _ io.ReaderAt = (*osFile)(nil)

type fileLock interface{ Close() error }
type mapView interface {
	close() error
	bytes() []byte
	adviseDontNeed()
}
