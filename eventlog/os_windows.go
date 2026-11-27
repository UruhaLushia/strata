//go:build windows

package eventlog

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func openFile(path string, flag int) (*os.File, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	access, creation := uint32(windows.GENERIC_READ), uint32(windows.OPEN_EXISTING)
	if flag&os.O_RDWR != 0 {
		access |= windows.GENERIC_WRITE
	}
	switch {
	case flag&os.O_EXCL != 0:
		creation = windows.CREATE_NEW
	case flag&os.O_CREATE != 0:
		creation = windows.OPEN_ALWAYS
	}
	const sharing = windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE
	h, err := windows.CreateFile(name, access, sharing, nil, creation, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

func lockFile(path string) (*os.File, error) {
	f, err := openFile(path, os.O_CREATE|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	var ov windows.Overlapped
	if err := windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ov); err != nil {
		_ = f.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return f, nil
}

// Windows has no portable directory fsync equivalent.
func syncParent(string) error { return nil }
