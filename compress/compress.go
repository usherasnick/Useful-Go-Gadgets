package compress

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
)

func Unzip(dst, src string) error {
	zr, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer zr.Close()

	if err = os.MkdirAll(dst, 0644); err != nil {
		return err
	}

	unit := func(path string, file *zip.File) error {
		fr, err := file.Open()
		if err != nil {
			return err
		}
		defer fr.Close()

		if err = os.MkdirAll(filepath.Dir(path), file.Mode()); err != nil {
			return err
		}

		fw, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, file.Mode())
		if err != nil {
			return err
		}
		defer fw.Close()

		if _, err = io.Copy(fw, fr); err != nil {
			return err
		}

		return nil
	}

	for _, f := range zr.File {
		p := filepath.Join(dst, f.Name)

		if f.FileInfo().IsDir() {
			if err = os.MkdirAll(p, f.Mode()); err != nil {
				return err
			}
			continue
		}

		if err = unit(p, f); err != nil {
			return err
		}
	}

	return nil
}
