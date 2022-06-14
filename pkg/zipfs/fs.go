package zipfs

import (
	"archive/zip"
	"github.com/goph/emperror"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type FS struct {
	r          *zip.ReadCloser
	w          *zip.Writer
	targetFile *os.File
	newFiles   []string
}

func NewFS(src, target string) (*FS, error) {
	var err error

	zfs := &FS{
		newFiles: []string{},
	}

	if src != "" {
		zr, err := zip.OpenReader(src)
		if err != nil {
			return nil, emperror.Wrapf(err, "cannot open zip file %s", src)
		}
		zfs.r = zr
	}
	zfs.targetFile, err = os.Create(target)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot create zip file %s", target)
	}
	zw := zip.NewWriter(zfs.targetFile)
	zfs.w = zw

	return zfs, nil
}

func (zf *FS) Close() error {
	// check whether we have to copy all stuff
	if zf.r != nil && zf.w != nil {
		// check whether there's a new version of the file
		found := false
		for _, zipItem := range zf.r.File {
			for _, added := range zf.newFiles {
				if added == zipItem.Name {
					found = true
					break
				}
			}
			if found {
				continue
			}
			zipItemReader, err := zipItem.OpenRaw()
			if err != nil {
				return emperror.Wrapf(err, "cannot open raw source %s", zipItem.Name)
			}
			header := zipItem.FileHeader
			targetItem, err := zf.w.CreateRaw(&header)
			if err != nil {
				return emperror.Wrapf(err, "cannot create raw target %s", zipItem.Name)
			}
			if _, err := io.Copy(targetItem, zipItemReader); err != nil {
				return emperror.Wrapf(err, "cannot raw copy %s", zipItem.Name)
			}
		}
	}
	finalError := emperror.NewMultiErrorBuilder()
	if zf.r != nil {
		if err := zf.r.Close(); err != nil {
			finalError.Add(err)
		}
	}
	if zf.w != nil {
		if err := zf.w.Flush(); err != nil {
			finalError.Add(err)
		}
		if err := zf.w.Close(); err != nil {
			finalError.Add(err)
		}
		if err := zf.targetFile.Close(); err != nil {
			finalError.Add(err)
		}
	}
	return finalError.ErrOrNil()
}

func (zfs *FS) Open(name string) (fs.File, error) {
	name = filepath.ToSlash(name)
	// check whether file is newly created
	for _, newItem := range zfs.newFiles {
		if newItem == name {
			return nil, fs.ErrInvalid // new files cannot be opened
		}
	}
	for _, zipItem := range zfs.r.File {
		if zipItem.Name == name {
			finfo, err := NewFileInfoFile(zipItem)
			if err != nil {
				return nil, emperror.Wrapf(err, "cannot create zipfs.FileInfo for %s", zipItem.Name)
			}
			f, err := NewFile(finfo)
			if err != nil {
				return nil, emperror.Wrapf(err, "cannot create zipfs.File from zipfs.FileInfo for %s", finfo.Name())
			}
			return f, nil
		}
	}
	return nil, fs.ErrNotExist
}

func (zfs *FS) Create(name string) (io.WriteCloser, error) {
	wc, err := zfs.Create(name)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot create file %s", name)
	}
	return wc, nil
}

func (zf *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if zf.r == nil {
		return []fs.DirEntry{}, nil
	}

	// force slash at the end
	name = strings.TrimSuffix(filepath.ToSlash(name), "/") + "/"
	if name == "." {
		name = ""
	}
	var entries = []*DirEntry{}
	for _, zipItem := range zf.r.File {
		if name != "" && !strings.HasPrefix(zipItem.Name, name) {
			continue
		}
		fi, err := NewFileInfoFile(zipItem)
		if err != nil {
			return nil, emperror.Wrapf(err, "cannot create FileInfo for %s", zipItem.Name)
		}
		entries = append(entries, NewDirEntry(fi))
	}
	var result = []fs.DirEntry{}
	for _, entry := range entries {
		result = append(result, entry)
	}
	return result, nil
}

func (zf *FS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.ToSlash(name)
	// check whether file is newly created
	for _, newItem := range zf.newFiles {
		if newItem == name {
			return nil, fs.ErrInvalid // new files cannot be opened
		}
	}
	for _, zipItem := range zf.r.File {
		if zipItem.Name == name {
			finfo, err := NewFileInfoFile(zipItem)
			if err != nil {
				return nil, emperror.Wrapf(err, "cannot create zipfs.FileInfo for %s", zipItem.Name)
			}
			return finfo, nil
		}
	}
	return nil, fs.ErrNotExist
}
