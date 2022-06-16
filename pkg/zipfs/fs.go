package zipfs

import (
	"archive/zip"
	"github.com/goph/emperror"
	"github.com/op/go-logging"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type FS struct {
	srcReader io.ReaderAt
	dstWriter io.Writer
	r         *zip.Reader
	w         *zip.Writer
	newFiles  []string
	logger    *logging.Logger
}

func NewFSIO(src io.ReaderAt, srcSize int64, dst io.Writer, logger *logging.Logger) (*FS, error) {
	logger.Debug("instantiating FSIO")
	var err error
	zfs := &FS{
		newFiles:  []string{},
		srcReader: src,
		dstWriter: dst,
		logger:    logger,
	}
	if src != nil && src != (*os.File)(nil) {
		if zfs.r, err = zip.NewReader(src, srcSize); err != nil {
			return nil, emperror.Wrap(err, "cannot create zip reader")
		}
	}
	if dst != nil {
		zfs.w = zip.NewWriter(dst)
	}
	return zfs, nil
}

func (zf *FS) Close() error {
	zf.logger.Debug()
	// check whether we have to copy all stuff
	if zf.r != nil && zf.w != nil {
		// check whether there's a new version of the file
		for _, zipItem := range zf.r.File {
			found := false
			for _, added := range zf.newFiles {
				if added == zipItem.Name {
					found = true
					zf.logger.Debugf("overwriting %s", added)
					break
				}
			}
			if found {
				continue
			}
			zf.logger.Debugf("copying %s", zipItem.Name)
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
	if zf.w != nil {
		if err := zf.w.Flush(); err != nil {
			finalError.Add(err)
		}
		if err := zf.w.Close(); err != nil {
			finalError.Add(err)
		}
	}
	return finalError.ErrOrNil()
}

func (zfs *FS) Open(name string) (fs.File, error) {
	if zfs.r == nil {
		return nil, fs.ErrNotExist
	}
	name = filepath.ToSlash(name)
	zfs.logger.Debugf("%s", name)
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
	zfs.logger.Debugf("%s not found")
	return nil, fs.ErrNotExist
}

func (zfs *FS) Create(name string) (io.Writer, error) {
	zfs.logger.Debugf("%s", name)
	wc, err := zfs.w.Create(name)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot create file %s", name)
	}
	zfs.newFiles = append(zfs.newFiles, name)
	return wc, nil
}

func (zf *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	zf.logger.Debugf("%s", name)
	if zf.r == nil {
		return []fs.DirEntry{}, nil
	}

	if name == "." {
		name = ""
	}
	// force slash at the end
	if name != "" {
		name = strings.TrimSuffix(filepath.ToSlash(name), "/") + "/"
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
	zf.logger.Debugf("%s", name)

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
