package bagit

import (
	"github.com/goph/emperror"
	"io/ioutil"
	"path/filepath"
	"strings"
)

type FolderEntry struct {
	Basedir     string `json:"basedir"`
	Name        string `json:"name"`
	Fullpath    string `json:"fullpath"`
	HasChildren bool   `json:"haschildren"`
}

type FolderInfo struct {
	FolderEntry
	Folders int64    `json:"folders"`
	Files   int64    `json:"files"`
	Size    int64    `json:"size"`
	Errs    []string `json:"errors"`
}

func HasSubfolders(basedir, folder string) (bool, error) {
	files, err := ioutil.ReadDir(filepath.Join(basedir, folder))
	if err != nil {
		return false, err
	}
	for _, file := range files {
		fname := file.Name()
		if !file.IsDir() || fname == "." || fname == ".." {
			continue
		}
		return true, nil
	}
	return false, nil
}

func iterate(folder string) (size int64, folders int64, files int64, errs []string) {
	fileList, err := ioutil.ReadDir(folder)
	if err != nil {
		return 0, 0, 0, []string{emperror.Wrapf(err, "cannot read folder").Error()}
	}

	errs = []string{}
	for _, f := range fileList {
		if f.IsDir() {
			name := f.Name()
			if name == "." || name == ".." {
				continue
			}
			folders++
			fullpath := filepath.Join(folder, f.Name())
			s, fo, fi, nerrs := iterate(fullpath)
			if len(nerrs) > 0 {
				errs = append(errs, nerrs...)
			} else {
				size += s
				folders += fo
				files += fi
			}
		} else {
			size += f.Size()
			files++
		}
		//		fmt.Println(f.Name())
	}
	return
}

func GetFolderInfo(basedir, folder string) *FolderInfo {
	folder = "/" + strings.TrimPrefix(folder, "/")
	finfo := &FolderInfo{
		FolderEntry: FolderEntry{
			Basedir:  basedir,
			Name:     filepath.Base(folder),
			Fullpath: filepath.ToSlash(folder),
		},
	}
	var dir = filepath.Join(basedir, folder)
	finfo.Size, finfo.Folders, finfo.Files, finfo.Errs = iterate(dir)
	finfo.HasChildren = finfo.Folders > 0
	return finfo
}

func GetSubfolders(basedir, folder string) ([]FolderEntry, error) {
	folder = "/" + strings.TrimPrefix(folder, "/")
	dir := filepath.Join(basedir, folder)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot read directory of %s", folder)
	}
	var entries []FolderEntry
	for _, file := range files {
		fname := file.Name()
		if !file.IsDir() || fname == "." || fname == ".." {
			continue
		}
		entry := FolderEntry{
			Basedir:  basedir,
			Name:     file.Name(),
			Fullpath: filepath.ToSlash(filepath.Join(folder, fname)),
		}
		entry.HasChildren, err = HasSubfolders(basedir, entry.Fullpath)
		if err != nil {
			//return nil, emperror.Wrapf(err, "cannot check for subolders of %s", entry.Fullpath)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
