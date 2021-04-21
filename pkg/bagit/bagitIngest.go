package bagit

import (
	"database/sql"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/op/go-logging"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type BagitIngest struct {
	tempDir     string
	logger      *logging.Logger
	ingest      *Ingest
	keyDir      string
	initLocName string
	initLoc     *Location
	tests       map[string]*IngestTest
	locations   map[string]*Location
}

const encExt = "aes256"

func NewBagitIngest(tempDir, keyDir, initLocName string, db *sql.DB, dbschema string, logger *logging.Logger) (*BagitIngest, error) {
	i, err := NewIngest(db, dbschema)
	if err != nil {
		return nil, err
	}
	bi := &BagitIngest{
		tempDir:     tempDir,
		keyDir:      keyDir,
		logger:      logger,
		ingest:      i,
		initLocName: initLocName,
	}
	if err := bi.Init(); err != nil {
		return nil, emperror.Wrap(err, "cannot initialaze")
	}
	return bi, nil
}

func (bi *BagitIngest) Init() error {
	var err error
	bi.locations, err = bi.ingest.LocationLoadAll()
	if err != nil {
		return emperror.Wrapf(err, "cannot load locations")
	}

	bi.tests, err = bi.ingest.TestLoadAll()
	if err != nil {
		return emperror.Wrapf(err, "cannot load locations")
	}

	var ok bool
	bi.initLoc, ok = bi.locations[bi.initLocName]
	if !ok {
		return fmt.Errorf("cannot get init location %s", bi.initLoc)
	}
	return nil
}

func (bi *BagitIngest) Run() error {

	fp := strings.Trim(bi.initLoc.GetPath().Path, "/") + "/"
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

		bagitPath := path

		tmpdir, err := ioutil.TempDir(bi.tempDir, filepath.Base(bagitPath))
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
		checker, err := NewBagit(bagitPath, tmpdir, checkDB, bi.logger)
		bi.logger.Infof("deep checking bagit file %s", bagitPath)
		if err := checker.Check(); err != nil {
			checkDB.Close()
			if err := os.RemoveAll(tmpdir); err != nil {
				return emperror.Wrapf(err, "cannot remove %s", tmpdir)
			}
			return emperror.Wrapf(err, "error checking file %v", bagitPath)
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

		cs, err := SHA512(bagitPath)
		if err != nil {
			return emperror.Wrap(err, "cannot calculate checksum")
		}

		bagit, err := bi.ingest.BagitNew(name, info.Size(), cs, "", "", "bagarc", time.Now())
		if err != nil {
			return emperror.Wrapf(err, "cannot create bagit entity %s", name)
		}
		if err := bagit.Store(); err != nil {
			return emperror.Wrapf(err, "cannot store %s", name)
		}

		if err := bi.Encrypt(name, bagitPath); err != nil {
			return emperror.Wrapf(err, "cannot encrypt %s", bagitPath)
		}

		return nil
	}); err != nil {
		return emperror.Wrapf(err, "error walking %s", fp)
	}
	return nil
}

func (bi *BagitIngest) Encrypt(name, bagitPath string) error {
	if _, err := os.Stat(bagitPath + "." + encExt); err == nil {
		return fmt.Errorf("encrypted bagit file %s.%s already exists", name, encExt)
	} else if !os.IsNotExist(err) {
		return emperror.Wrapf(err, "error checking existence of %s.%s", name, encExt)
	}

	// create checksum of bagit
	//		bi.logger.Infof("calculating checksum of %s", bagitFile)
	//		checksum := sha512.New()

	fin, err := os.Open(bagitPath)
	if err != nil {
		return emperror.Wrapf(err, "cannot open %s", bagitPath)
	}
	defer fin.Close()
	fout, err := os.OpenFile(bagitPath+"."+encExt, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return emperror.Wrapf(err, "cannot create encrypted bagit %s.%s", bagitPath, encExt)
	}
	defer fout.Close()
	if err != nil {
		return emperror.Wrapf(err, "cannot open %s.%s", bagitPath, encExt)
	}
	defer fout.Close()

	key, iv, hashBytes, err := Encrypt(fin, fout)
	if err != nil {
		return emperror.Wrapf(err, "cannot encrypt %s", bagitPath)
	}

	os.WriteFile(fmt.Sprintf("%s/%s.%s.key", bi.keyDir, name, encExt), []byte(fmt.Sprintf("%x", key)), 0600)
	os.WriteFile(fmt.Sprintf("%s/%s.%s.iv", bi.keyDir, name, encExt), []byte(fmt.Sprintf("%x", iv)), 0600)

	bi.logger.Infof("key: %x, iv: %x, hash: %x", key, iv, hashBytes)
	bi.logger.Infof("decrypt using openssl: \n openssl enc -aes-256-ctr -nosalt -d -in %s.%s -out %s -K '%x' -iv '%x'", bagitPath, encExt, bagitPath, key, iv)

	/*
		if _, err := io.Copy(checksum, fin); err != nil {
			return emperror.Wrapf(err, "cannot calculate checksum of %s", bagitFile)
		}
	*/
	return nil
}
