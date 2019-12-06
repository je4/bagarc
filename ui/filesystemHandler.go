package main

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/gorilla/mux"
	"github.com/je4/bagarc/common"
	"github.com/shirou/gopsutil/disk"
	"net/http"
	"path/filepath"
	"strings"
)

type folderEntryNoChildren struct {
	Id       string `json:"id"`
	Name     string `json:"name"`
	Fullpath string `json:"fullpath"`
}

type folderEntryChildren struct {
	folderEntryNoChildren
	Children []folderEntryChildren `json:"children"`
}

type Filesystem struct {
	name          string
	dirmap        map[string]string
	partitions    map[string]string
	db            *badger.DB
	usePartitions bool
	restBase      string
}

func NewFilesystem(name string, partitions bool, restBase string, dirmap map[string]string, db *badger.DB) *Filesystem {
	fs := &Filesystem{name: name, usePartitions: partitions, restBase:restBase, dirmap: dirmap, db: db}
	fs.loadMap() // ignore errors
	if partitions {
		fs.loadPartitions() // ignore errors
	}
	return fs
}

func (fs *Filesystem) loadPartitions() error {
	fs.partitions = map[string]string{}
	partitions, err := disk.Partitions(false)
	if err != nil {
		return emperror.Wrapf(err, "cannot read partitions")
	}
	for _, partition := range partitions {
		fs.partitions[partition.Device] = partition.Mountpoint
	}
	return nil
}

func (fs *Filesystem) getFolder(name string) (string, bool) {
	folder, ok := fs.dirmap[name]
	if ok {
		return folder, true
	}
	if fs.usePartitions {
		folder, ok = fs.partitions[name]
	}
	return folder, ok
}

func (fs *Filesystem) addDir(name, folder string) {
	fs.dirmap[name] = folder
}

func (fs *Filesystem) loadMap() error {
	data := map[string]string{}
	if err := fs.db.View(func(txn *badger.Txn) error {
		jsonstr, err := txn.Get([]byte(fs.name))
		if err != nil {
			return emperror.Wrapf(err, "cannot get data for key %s", fs.name)
		}
		if err := jsonstr.Value(func(val []byte) error {
			if err := json.Unmarshal(val, &data); err != nil {
				return emperror.Wrapf(err, "cannot unmarshal %s", string(val))
			}
			return nil
		}); err != nil {
			return emperror.Wrapf(err, "cannot get value of key %s", fs.name)
		}
		return nil
	}); err != nil {
		return emperror.Wrapf(err, "cannot get data")
	}
	for name, folder := range data {
		fs.dirmap[name] = folder
	}
	return nil
}

func (fs *Filesystem) store() error {
	if err := fs.db.Update(func(txn *badger.Txn) error {
		jsonstr, err := json.Marshal(fs.dirmap)
		if err != nil {
			return emperror.Wrapf(err, "cannot marshal dirmap")
		}
		txn.Set([]byte(fs.name), jsonstr)
		return nil
	}); err != nil {
		return emperror.Wrapf(err, "cannot store dirmap")
	}
	if err := fs.db.Sync(); err != nil {
		return emperror.Wrapf(err, "cannot sync database")
	}
	return nil
}

func (fs *Filesystem) addDirmapHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		name, ok := vars["name"]
		if !ok {
			http.Error(w, "no name in url", http.StatusBadRequest)
			return
		}
		var folder string
		err := json.NewDecoder(r.Body).Decode(&folder)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, ok := fs.getFolder(name); ok {
			http.Error(w, fmt.Sprintf("%smap %v already exists", fs.restBase, name), http.StatusConflict)
			return
		}
		fs.dirmap[name] = folder
		if err := fs.store(); err != nil {
			http.Error(w, emperror.Wrapf(err, "cannot store dirmap data").Error(), http.StatusInternalServerError)
			return
		}
		return
	}
}

func (fs *Filesystem) removeDirmapHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		name, ok := vars["name"]
		if !ok {
			http.Error(w, "no name in url", http.StatusBadRequest)
			return
		}
		if _, ok := fs.dirmap[name]; !ok {
			http.Error(w, fmt.Sprintf("%smap %v does not exist exists", fs.restBase, name), http.StatusNotFound)
			return
		}
		delete(fs.dirmap, name)
		if err := fs.store(); err != nil {
			http.Error(w, emperror.Wrapf(err, "cannot store dirmap data").Error(), http.StatusInternalServerError)
			return
		}
		return
	}
}

func (fs *Filesystem) createGetDirmapHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		name, ok := vars["name"]
		if ok {
			ok = false
			var folder = ""
			if fs.usePartitions {
				folder, ok = fs.partitions[name]
			}
			if folder == "" {
				folder, ok = fs.dirmap[name]
			}
			if !ok {
				http.Error(w, fmt.Sprintf("could not find directory map for %s", name), http.StatusNotFound)
				return
			}

			jsonstr, err := json.Marshal(folder)
			if err != nil {
				http.Error(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
				return
			}
			w.Write(jsonstr)
			return
		}

		dirmap := map[string]string{}
		if fs.usePartitions {
			for name, folder := range fs.partitions {
				dirmap[name] = folder
			}
		}
		for name, folder := range fs.dirmap {
			dirmap[name] = folder
		}

		jsonstr, err := json.Marshal(dirmap)
		if err != nil {
			http.Error(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
			return
		}
		w.Write(jsonstr)
	}
}

func (fs *Filesystem) createSubfolderHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		//vars := mux.Vars(r)

		upath := filepath.Clean(r.URL.Path)
		flist := strings.Split(strings.Trim(filepath.ToSlash(upath), "/"), "/")
		if len(flist) == 0 {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(fmt.Sprintf("invalid path %v", upath)))
			return
		}

		basedir, ok := fs.getFolder(flist[0])
		if !ok {
			http.Error(w, fmt.Sprintf("%v not in list of known directories", flist[0]), http.StatusNotFound)
			return
		}
		upath = strings.Join(flist[1:], "/")

		var q = r.URL.Query()
		if _, info := q["info"]; info {
			finfo := common.GetFolderInfo(basedir, upath)
			jsonstr, err := json.Marshal(finfo)
			if err != nil {
				http.Error(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
				return
			}
			w.Write(jsonstr)
			return
		}

		subfolders, err := common.GetSubfolders(basedir, upath)
		if err != nil {
			http.Error(w, fmt.Sprintf("%v", err), http.StatusNotFound)
			return
		}
		var dirlist []interface{}
		for _, sf := range subfolders {
			if sf.HasChildren {
				dirlist = append(dirlist, folderEntryChildren{
					folderEntryNoChildren: folderEntryNoChildren{
						Name:     sf.Name,
						Fullpath: sf.Fullpath,
						Id:       sf.Fullpath,
					},
					Children: []folderEntryChildren{},
				})
			} else {
				dirlist = append(dirlist, folderEntryNoChildren{
					Name:     sf.Name,
					Fullpath: sf.Fullpath,
					Id:       sf.Fullpath,
				})
			}
		}
		jsonstr, err := json.Marshal(dirlist)
		if err != nil {
			http.Error(w, fmt.Sprintf("%v", err), http.StatusInternalServerError)
			return
		}
		w.Write(jsonstr)
	}
}
