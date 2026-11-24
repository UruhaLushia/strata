//go:build linux || android || darwin || ios

package strata

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type unixLock struct{ f *os.File }

func openLock(path string) (fileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, os.ErrExist
		}
		return nil, err
	}
	return &unixLock{f: f}, nil
}
func (l *unixLock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	return errors.Join(err, l.f.Close())
}
func openData(path string, readOnly bool) (*osFile, error) {
	flags := os.O_RDWR
	if readOnly {
		flags = os.O_RDONLY
	} else {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	return &osFile{f: f}, nil
}
func renameAtomic(old, new string) error { return os.Rename(old, new) }
func syncParent(dir string) error {
	f, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func syncData(f *osFile) error { return f.Sync() }
