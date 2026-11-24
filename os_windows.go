//go:build windows

package strata

import (
	"errors"
	"golang.org/x/sys/windows"
	"os"
	"time"
)

type windowsLock struct{ f *os.File }

func openLock(path string) (fileLock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	var ov windows.Overlapped
	err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ov)
	if err != nil {
		_ = f.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, os.ErrExist
		}
		return nil, err
	}
	return &windowsLock{f: f}, nil
}
func (l *windowsLock) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	var ov windows.Overlapped
	err := windows.UnlockFileEx(windows.Handle(l.f.Fd()), 0, 1, 0, &ov)
	return errors.Join(err, l.f.Close())
}
func openData(path string, readOnly bool) (*osFile, error) {
	access := uint32(windows.GENERIC_READ)
	creation := uint32(windows.OPEN_EXISTING)
	if !readOnly {
		access |= windows.GENERIC_WRITE
		creation = windows.OPEN_ALWAYS
	}
	h, err := windows.CreateFile(windows.StringToUTF16Ptr(path), access, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, creation, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	return &osFile{f: os.NewFile(uintptr(h), path)}, nil
}
func renameAtomic(old, new string) error {
	for i := 0; i < 8; i++ {
		err := windows.MoveFileEx(windows.StringToUTF16Ptr(old), windows.StringToUTF16Ptr(new), windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 5 * time.Millisecond)
	}
	return os.Rename(old, new)
}
func syncParent(dir string) error { return nil }
func syncData(f *osFile) error    { return f.Sync() }
