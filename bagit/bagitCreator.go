package bagit

import (
	"archive/zip"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/common"
	"github.com/op/go-logging"
	"golang.org/x/crypto/sha3"
	"hash"
	"io"
	"os"
	"path/filepath"
)

// describes a structure for ingest process
type BagitCreator struct {
	logger    *logging.Logger
	sourcedir string     // folder to ingest
	bagitfile string     // resultung bagit zip file
	checksum  []string   // list of checksums to create
	db        *badger.DB // file storage
	siegfried string     // url for siegfried daemon
	tempdir   string     // folder for temporary files
}

type rwStruct struct {
	reader *io.PipeReader
	writer *io.PipeWriter
}

// creates a new bagit creation structure
func NewBagitCreator(sourcedir, bagitfile string, checksum []string, db *badger.DB, siegfried string, tempdir string, logger *logging.Logger) (*BagitCreator, error) {
	sourcedir = filepath.ToSlash(filepath.Clean(sourcedir))
	bagitfile = filepath.ToSlash(filepath.Clean(bagitfile))
	// make sure, that file does not exist...
	_, err := os.Stat(bagitfile)
	if !os.IsNotExist(err) {
		return nil, errors.New(fmt.Sprintf("file %v exists", bagitfile))
	}

	bagitCreator := &BagitCreator{
		sourcedir: sourcedir,
		bagitfile: bagitfile,
		logger:    logger,
		checksum:  checksum,
		db:        db,
		siegfried: siegfried,
		tempdir:   tempdir,
	}
	return bagitCreator, nil
}

// executes creation of bagit
func (bc *BagitCreator) Run() (err error) {
	// create a new zip file
	zipFile, err := os.Create(bc.bagitfile)
	if err != nil {
		return emperror.Wrapf(err, "cannot create zip file %v", bc.bagitfile)
	}
	defer zipFile.Close()

	// create writer for zip
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	if err := bc.fileIterator(zipWriter); err != nil {
		return emperror.Wrapf(err, "cannot create zip")
	}

	bc.logger.Info("creating manifest files")
	if err := bc.createManifestsAndTags(); err != nil {
		return emperror.Wrap(err, "error creating manifest files")
	}

	// write manifests zu zip
	bc.logger.Info("adding manifest files to bagit")
	if err := bc.writeManifestToZip(zipWriter); err != nil {
		return emperror.Wrap(err, "cannot write manifests to zip")
	}

	// create metadata json file


	return
}

// create all manifest files within the temp folder
func (bc *BagitCreator) createManifestsAndTags() (err error) {
	// open manifest files for all checksums
	manifests := map[string]*os.File{}
	for _, cType := range bc.checksum {
		manifestfile := fmt.Sprintf("manifest-%s.txt", cType)
		bc.logger.Infof("creating %s", manifestfile)
		mfilename := filepath.Join(bc.tempdir, manifestfile)
		manifests[cType], err = os.Create(mfilename)
		if err != nil {
			return emperror.Wrapf(err, "cannot open manifest file %v", mfilename)
		}
	}
	defer func() {
		for _, f := range manifests {
			f.Close()
		}
	}()

	bc.logger.Infof("creating %s", "metainfo.json")
	metainfofname := filepath.Join(bc.tempdir, "metainfo.json")
	metainfo, err := os.Create(metainfofname)
	if err != nil {
		return emperror.Wrapf(err, "cannot create %v", metainfofname)
	}
	defer metainfo.Close()
	metainfo.WriteString("[")

	if err := bc.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()
		first := true
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				bf := &BagitFile{}
				if err := json.Unmarshal(v, bf); err != nil {
					return emperror.Wrapf(err, "cannot unmarshal json: %v", string(v))
				}
				for cType, cs := range bf.Checksum {
					f, ok := manifests[cType]
					if !ok {
						return errors.New(fmt.Sprintf("no manifest file for checksum %s", cType))
					}
					if _, err := f.WriteString(fmt.Sprintf("%s %s\n", cs, "data"+bf.Path)); err != nil {
						return emperror.Wrapf(err, "cannot write checksum to %s", bf.Path)
					}
				}

				if !first {
					metainfo.WriteString(",")
				}
				metainfo.Write(v)
				//			fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return emperror.Wrapf(err, "cannot get value for key %v", k)
			}
			first = false
		}
		return nil
	}); err != nil {
		return emperror.Wrap(err, "cannot create manifest files")
	}
	metainfo.WriteString("]")
	return nil
}

// write manifest files from temp folder to zip
func (bc *BagitCreator) writeManifestToZip(zipWriter *zip.Writer) error {
	for _, cType := range bc.checksum {
		manifestfile := fmt.Sprintf("manifest-%s.txt", cType)
		bc.logger.Infof("storing %s", manifestfile)
		mfilename := filepath.Join(bc.tempdir, manifestfile)
		info, err := os.Stat(mfilename)
		if err != nil {
			return emperror.Wrapf(err, "cannot stat %v", mfilename)
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return emperror.Wrap(err, "cannot create zip.FileInfoHeader")
		}
		// we write only to the data subfolder
		header.Name = fmt.Sprintf("manifest-%s.txt", cType)

		// make sure, that compression is ok
		header.Method = zip.Deflate

		zWriter, err := zipWriter.CreateHeader(header)
		if err != nil {
			return emperror.Wrap(err, "cannot write header to zip")
		}

		reader, err := os.Open(mfilename)
		if err != nil {
			return emperror.Wrapf(err, "cannot open %v", mfilename)
		}
		defer reader.Close()
		_, err = io.Copy(zWriter, reader)
		if err != nil {
			return emperror.Wrapf(err, "cannot write %v to zip", mfilename)
		}
	}
	return nil
}

func (bc *BagitCreator) writeMetainfoToZip( zipWriter zip.Writer) error {
	// create the map of all Checksum-pipes and start async process
	rws := map[string]rwStruct{}
	for _, csType := range bc.checksum {
		rw := rwStruct{}
		rw.reader, rw.writer = io.Pipe()
		rws[csType] = rw
		//go doChecksum(rw.reader, csType, bc.checksumResult, bc.asyncError, done)
	}

	return nil
}

// called by file walker.
func (bc *BagitCreator) visitFile(path string, f os.FileInfo, zipWriter *zip.Writer, txn *badger.Txn, err error) error {
	bf, err := NewBagitFile(bc.sourcedir, path)
	if err != nil {
		return emperror.Wrap(err, "error creating BagitFile")
	}
	bc.logger.Infof("walk: %s", bf)
	if bf.IsDir() {
		return nil
	}
	if err := bf.AddToZip(zipWriter, bc.checksum); err != nil {
		return emperror.Wrapf(err, "cannot add %s to zip", bf)
	}

	if bc.siegfried != "" {
		if err := bf.GetSiegfried(bc.siegfried); err != nil {
			bc.logger.Errorf("error querying siegfried: %v", err)
		}
	}

	// add file to key value store
	jsonstr, err := json.Marshal(bf)
	if err != nil {
		return emperror.Wrap(err, "cannot marshal BagitFile")
	}
	txn.Set([]byte(bf.Path), jsonstr)

	return nil
}

// iterates through all files of source directory
func (bc *BagitCreator) fileIterator(zipWriter *zip.Writer) (err error) {
	// Start a writable transaction.
	txn := bc.db.NewTransaction(true)
	defer txn.Discard()

	if err := filepath.Walk(bc.sourcedir, func(path string, f os.FileInfo, err error) error {
		return bc.visitFile(path, f, zipWriter, txn, err)
	}); err != nil {
		return emperror.Wrapf(err, "cannot walk filesystem")
	}
	txn.Commit()
	return
}

// start Checksum process
// supported csType's: md5, sha256, sha512
func doChecksum(reader io.Reader, csType string, resultFunc func( string, string ), errFunc func(error), done chan bool) {
	// we should end in all cases
	defer func() { done <- true }()

	var sink hash.Hash
	switch csType {
	case "md5":
		sink = md5.New()
	case "sha256":
		sink = sha256.New()
	case "sha512":
		sink = sha512.New()
	case "sha3-256":
		sink = sha3.New256()
	case "sha3-384":
		sink = sha3.New384()
	case "sha3-512":
		sink = sha3.New512()
	default:
		errFunc(errors.New(fmt.Sprintf("invalid hash function %s", csType)))
		null := &common.NullWriter{}
		io.Copy(null, reader)
		return
	}
	if _, err := io.Copy(sink, reader); err != nil {
		errFunc(emperror.Wrapf(err, "cannot create checkum %s", csType))
		return
	}
	resultFunc(csType, fmt.Sprintf("%x", sink.Sum(nil)))
}
