package deepcopy

import (
	"io"
	"io/ioutil"
	"os"
	"path"
)

func fcopy(dst, src string) error {
	srcFd, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFd.Close()
	dstfd, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstfd.Close()
	if _, err = io.Copy(dstfd, srcFd); err != nil {
		return err
	}
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	return os.Chmod(dst, srcInfo.Mode())
}

// Copy copies a whole directory recursively.
func Copy(dst, src string) error {
	fInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(dst, fInfo.Mode()); err != nil {
		return err
	}
	fInfos, err := ioutil.ReadDir(src)
	if err != nil {
		return err
	}
	for _, info := range fInfos {
		srcFp := path.Join(src, info.Name())
		dstFp := path.Join(dst, info.Name())
		if info.IsDir() {
			if err = Copy(dstFp, srcFp); err != nil {
				return err
			}
		} else {
			if err = fcopy(dstFp, srcFp); err != nil {
				return err
			}
		}
	}
	return nil
}
