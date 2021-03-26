package bagit

import (
	"database/sql"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/v2/pkg/ingest"
	"github.com/op/go-logging"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type BagitIngest struct {
	baseDir string
	tempDir string
	logger  *logging.Logger
	ingest  *ingest.Ingest
}

func NewBagitIngest(baseDir, tempDir string, db *sql.DB, dbschema string, logger *logging.Logger) (*BagitIngest, error) {
	i, err := ingest.NewIngest(db, dbschema)
	if err != nil {
		return nil, err
	}
	bi := &BagitIngest{
		baseDir: baseDir,
		tempDir: tempDir,
		logger:  logger,
		ingest:  i,
	}

	return bi, nil
}

func (bi *BagitIngest) Run() error {
	if err := filepath.Walk(bi.baseDir, func(path string, info os.FileInfo, err error) error {
		// ignore dot files
		if []rune(path)[0] == '.' {
			return nil
		}
		// ignore directories
		if info.IsDir() {
			return nil
		}
		// ignore files without .zip extension
		if !strings.HasSuffix(path, ".zip") {
			return nil
		}

		bagitFile := filepath.Base(filepath.Join(bi.baseDir, path))
		tmpdir, err := ioutil.TempDir(bi.tempDir, bagitFile)
		if err != nil {
			bi.logger.Fatalf("cannot create temporary folder in %s", bi.tempDir)
		}
		bconfig := badger.DefaultOptions(filepath.Join(tmpdir, "/badger"))
		bconfig.Logger = bi.logger // use our logger...
		checkDB, err := badger.Open(bconfig)
		if err != nil {
			return emperror.Wrapf(err, "cannot open badger database")
		}

		checker, err := NewBagit(bagitFile, tmpdir, checkDB, bi.logger)
		if err := checker.Check(); err != nil {
			checkDB.Close()
			if err := os.RemoveAll(tmpdir); err != nil {
				return emperror.Wrapf(err, "cannot remove %s", tmpdir)
			}
			return emperror.Wrapf(err, "error checking file %v", bagitFile)
		}

		checkDB.Close()
		if err := os.RemoveAll(tmpdir); err != nil {
			return emperror.Wrapf(err, "cannot remove %s", tmpdir)
		}

		return nil
	}); err != nil {
		return emperror.Wrapf(err, "error walking %s", bi.baseDir)
	}
	return nil
}
