package main

import (
	"encoding/json"
	_ "github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/gorilla/mux"
	"github.com/op/go-logging"
	"net/http"
)

type Bagit struct {
	//	bagitFile string
	//	sourceDir string
	//	bagInfo   map[string]string
	//	db        *badger.DB
	siegfried string
	logger    *logging.Logger
	tempDir   string
}

func NewBagit(tempDir string, siegfried string, logger *logging.Logger) *Bagit {
	return &Bagit{
		siegfried: siegfried,
		logger:    logger,
		tempDir:   tempDir,
	}
}

func (b *Bagit) addBagitHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		param := struct {
			BagitFile string `json:"bagitfile"`
			SourceDir string `json:"sourcedir"`
			BagInfo map[string]string `json:"baginfo"`
		}{}
		vars := mux.Vars(r)
		bagitfile, ok := vars["name"]
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
