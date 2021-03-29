package bagit

import (
	"crypto/rand"
	"crypto/sha512"
	"database/sql"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/v2/pkg/ingest"
	"github.com/op/go-logging"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type BagitIngest struct {
	tempDir string
	logger  *logging.Logger
	ingest  *ingest.Ingest
	keyDir  string
}

func NewBagitIngest(tempDir, keyDir string, db *sql.DB, dbschema string, logger *logging.Logger) (*BagitIngest, error) {
	i, err := ingest.NewIngest(db, dbschema)
	if err != nil {
		return nil, err
	}
	bi := &BagitIngest{
		tempDir: tempDir,
		keyDir:  keyDir,
		logger:  logger,
		ingest:  i,
	}
	return bi, nil
}

func (bi *BagitIngest) Run() error {
	if err := bi.RunInit(); err != nil {
		return emperror.Wrap(err, "cannut initialaze")
	}

	return nil
}

func (bi *BagitIngest) RunInit() error {
	initLoc, err := bi.ingest.LocationInit()
	if err != nil {
		return emperror.Wrap(err, "cannot get init location")
	}

	fp := "/" + strings.Trim(initLoc.GetPath().Path, "/")
	if err := filepath.Walk(fp, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		name := info.Name()
		// hope: there's no empty filename
		if name[0] == '.' {
			return nil
		}
		if !strings.HasSuffix(name, ".zip") {
			return nil
		}

		bagitFile := filepath.Base(filepath.Join(fp, path))
		tmpdir, err := ioutil.TempDir(bi.tempDir, bagitFile)
		if err != nil {
			return emperror.Wrapf(err, "cannot create temporary folder in %s", bi.tempDir)
		}

		// initialize badget database
		bconfig := badger.DefaultOptions(filepath.Join(tmpdir, "/badger"))
		bconfig.Logger = bi.logger // use our logger...
		checkDB, err := badger.Open(bconfig)
		if err != nil {
			return emperror.Wrapf(err, "cannot open badger database")
		}

		// check bagit file
		checker, err := NewBagit(bagitFile, tmpdir, checkDB, bi.logger)
		bi.logger.Infof("deep checking bagit file %s", bagitFile)
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

		ib, err := bi.ingest.BagitLoad(name)
		if err != nil {
			return emperror.Wrapf(err, "cannot load bagit %s", name)
		}
		if ib != nil {
			bi.logger.Infof("bagit %s already ingested", name)
			return nil
		}

		if _, err := os.Stat(bagitFile + ".aes256"); err == nil {
			return fmt.Errorf("encrypted bagit file %s.aes256 already exists", name)
		} else if !os.IsNotExist(err) {
			return emperror.Wrapf(err, "error checking existence of %s.aes256", name)
		}

		// create checksum of bagit
		bi.logger.Infof("calculating checksum of %s", bagitFile)
		checksum := sha512.New()

		key := make([]byte, 32)
		iv := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return emperror.Wrapf(err, "cannot generate 32 byte key for aes256")
		}

		f, err := os.Open(bagitFile)
		if err != nil {
			return emperror.Wrapf(err, "cannot open %s", bagitFile)
		}
		if _, err := io.Copy(checksum, f); err != nil {
			f.Close()
			return emperror.Wrapf(err, "cannot calculate checksum of %s", bagitFile)
		}
		f.Close()

		return nil
	}); err != nil {
		return emperror.Wrapf(err, "error walking %s", fp)
	}
	return nil
}
