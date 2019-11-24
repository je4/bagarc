package common

import (
	"github.com/goph/emperror"
	"io/ioutil"
	"path/filepath"
)

type FolderEntry struct {
	Name        string `json:"name"`
	Fullpath    string `json:"fullpath"`
	HasChildren bool   `json:"haschildren"`
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

func GetSubfolders(basedir, folder string) ([]FolderEntry, error) {
	if folder == "" {
		folder = "/"
	}
	files, err := ioutil.ReadDir(filepath.Join(basedir, folder))
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
