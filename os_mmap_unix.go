//go:build linux || android || (darwin && !ios)

package strata

import "golang.org/x/sys/unix"

type unixMap struct{ data []byte }

func mapPrefix(f *osFile, walOffset uint64, enabled bool) (mapView, error) {
	if !enabled || walOffset <= 0x1000 {
		return nil, nil
	}
	length := int(walOffset - 0x1000)
	if length <= 0 {
		return nil, nil
	}
	data, err := unix.Mmap(int(f.Fd()), 0x1000, length, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &unixMap{data: data}, nil
}
func (m *unixMap) bytes() []byte { return m.data }
func (m *unixMap) adviseDontNeed() {
	if m == nil || len(m.data) == 0 {
		return
	}
	_ = unix.Madvise(m.data, unix.MADV_DONTNEED)
}
func (m *unixMap) close() error {
	if m == nil || m.data == nil {
		return nil
	}
	err := unix.Munmap(m.data)
	m.data = nil
	return err
}
