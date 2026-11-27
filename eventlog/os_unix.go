//go:build linux || android || darwin || ios

package eventlog

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func openFile(path string, flag int) (*os.File, error) {
	return os.OpenFile(path, flag, 0o644)
}

func lockFile(path string) (*os.File, error) {
	f, err := openFile(path, os.O_CREATE|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return f, nil
}

func syncParent(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
