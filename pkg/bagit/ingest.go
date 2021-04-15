package bagit

import (
	"database/sql"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/v2/pkg/ingest"
	"github.com/op/go-logging"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
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

	fp := strings.Trim(initLoc.GetPath().Path, "/") + "/"
	if runtime.GOOS == "windows" {
		fp = strings.Replace(fp, "|", ":", -1)
	} else {
		fp = "/" + fp
	}
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

		bagitFile := path

		tmpdir, err := ioutil.TempDir(bi.tempDir, filepath.Base(bagitFile))
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
		//		bi.logger.Infof("calculating checksum of %s", bagitFile)
		//		checksum := sha512.New()

		fin, err := os.Open(bagitFile)
		if err != nil {
			return emperror.Wrapf(err, "cannot open %s", bagitFile)
		}
		defer fin.Close()
		fout, err := os.OpenFile(bagitFile+".aes256", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			return emperror.Wrapf(err, "cannot create encrypted bagit %s", bagitFile+".aes256")
		}
		defer fout.Close()
		if err != nil {
			return emperror.Wrapf(err, "cannot open %s.aes", bagitFile)
		}
		defer fout.Close()

		key, iv, hashBytes, err := ingest.Encrypt(fin, fout)
		if err != nil {
			return emperror.Wrapf(err, "cannot encrypt %s", bagitFile)
		}

		os.WriteFile(bagitFile+".aes256.key", []byte(fmt.Sprintf("%x", key)), 0600)
		os.WriteFile(bagitFile+".aes256.iv", []byte(fmt.Sprintf("%x", iv)), 0600)

		bi.logger.Infof("key: %x, iv: %x, hash: %x", key, iv, hashBytes)
		bi.logger.Infof("decrypt using openssl: \n openssl enc -aes-256-ctr -nosalt -d -in %s.aes256 -out %s -K '%x' -iv '%x'", bagitFile, bagitFile, key, iv)

		/*
			if _, err := io.Copy(checksum, fin); err != nil {
				return emperror.Wrapf(err, "cannot calculate checksum of %s", bagitFile)
			}

		*/

		return nil
	}); err != nil {
		return emperror.Wrapf(err, "error walking %s", fp)
	}
	return nil
}
