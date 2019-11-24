package bagit

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/op/go-logging"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// describes a structure for ingest process
type BagitCreator struct {
	logger          *logging.Logger
	sourcedir       string            // folder to ingest
	bagitfile       string            // resultung bagit zip file
	checksum        []string          // list of checksums to create
	db              *badger.DB        // file storage
	siegfried       string            // url for siegfried daemon
	tempdir         string            // folder for temporary files
	fixFilename     bool              // true, if filenames should be corrected
	bagInfo         map[string]string // list of entries for bag-info.txt
	storeOnly       []string          // list of pronom id's which should be be compressed
	oxumOctetCount  int64             // octetstream sum - octet count
	oxumStreamCount int64             // octetstream sum - file count
}

type rwStruct struct {
	reader *io.PipeReader
	writer *io.PipeWriter
}

// creates a new bagit creation structure
func NewBagitCreator(sourcedir, bagitfile string, checksum []string, bagInfo map[string]string, db *badger.DB, fixFilename bool, storeOnly []string, siegfried string, tempdir string, logger *logging.Logger) (*BagitCreator, error) {
	sourcedir = filepath.ToSlash(filepath.Clean(sourcedir))
	bagitfile = filepath.ToSlash(filepath.Clean(bagitfile))
	// make sure, that file does not exist...
	_, err := os.Stat(bagitfile)
	if !os.IsNotExist(err) {
		return nil, errors.New(fmt.Sprintf("file %v exists", bagitfile))
	}

	bagitCreator := &BagitCreator{
		sourcedir:   sourcedir,
		bagitfile:   bagitfile,
		logger:      logger,
		checksum:    checksum,
		db:          db,
		fixFilename: fixFilename,
		siegfried:   siegfried,
		tempdir:     tempdir,
		bagInfo:     bagInfo,
		storeOnly:   storeOnly,
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

	if err := bc.writeBagitToZip(zipWriter); err != nil {
		return emperror.Wrapf(err, "cannot write bagit")
	}

	if err := bc.fileIterator(zipWriter); err != nil {
		return emperror.Wrapf(err, "cannot create zip")
	}

	bc.logger.Info("creating manifest files")
	if err := bc.createManifestsAndTags(); err != nil {
		return emperror.Wrap(err, "error creating manifest files")
	}

	// write manifests to zip
	bc.logger.Info("adding manifest files to bagit")
	if err := bc.writeManifestToZip(zipWriter); err != nil {
		return emperror.Wrap(err, "cannot write manifests to zip")
	}

	tagmanifests := map[string]map[string]string{}
	for _, csType := range bc.checksum {
		tagmanifests[csType] = map[string]string{}
	}

	// create metadata json file
	checksums, err := bc.writeMetainfoToZip(zipWriter)
	if err != nil {
		return emperror.Wrap(err, "cannot write metainfo to zip")
	}

	for csType, cs := range checksums {
		tagmanifests[csType]["bagarc/metainfo.json"] = cs
	}

	checksums, err = bc.writeRenamesToZip(zipWriter)
	if err != nil {
		return emperror.Wrap(err, "cannot write renames to zip")
	}

	for csType, cs := range checksums {
		tagmanifests[csType]["bagarc/renames.csv"] = cs
	}

	if len(bc.bagInfo) > 0 {
		checksums, err = bc.writeBaginfoToZip(zipWriter)
		if err != nil {
			return emperror.Wrap(err, "cannot write bag-info.txt to zip")
		}
		for csType, cs := range checksums {
			tagmanifests[csType]["bag-info.txt"] = cs
		}
	}

	for csType, tags := range tagmanifests {
		manifestfile := fmt.Sprintf("tagmanifest-%s.txt", csType)
		f, err := zipWriter.Create(manifestfile)
		if err != nil {
			return emperror.Wrapf(err, "cannot create zip file %v", manifestfile)
		}
		for filename, checksum := range tags {
			_, err = f.Write([]byte(fmt.Sprintf("%s %s\n", filename, checksum)))
			if err != nil {
				log.Fatal(err)
			}
		}
	}

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

	bc.logger.Infof("creating %s", "metainfo.json/renames.csv")
	metainfofname := filepath.Join(bc.tempdir, "metainfo.json")
	metainfo, err := os.Create(metainfofname)
	if err != nil {
		return emperror.Wrapf(err, "cannot create %v", metainfofname)
	}
	defer metainfo.Close()
	renamesfname := filepath.Join(bc.tempdir, "renames.csv")
	renamesf, err := os.Create(renamesfname)
	if err != nil {
		return emperror.Wrapf(err, "cannot create %v", renamesfname)
	}
	defer renamesf.Close()
	renames := csv.NewWriter(renamesf)

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
					if _, err := f.WriteString(fmt.Sprintf("%s %s\n", cs, "data"+bf.ZipPath)); err != nil {
						return emperror.Wrapf(err, "cannot write checksum to %s", bf.ZipPath)
					}
				}

				if !first {
					metainfo.WriteString(",")
				}
				metainfo.Write(v)

				if bf.ZipPath != bf.Path {
					renames.Write([]string{strings.Trim(bf.Path, "/"), strings.TrimPrefix(bf.ZipPath, "/")})
				}
				//			fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return emperror.Wrapf(err, "cannot get value for key %v", k)
			}
			first = false
		}
		renames.Flush()
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

func (bc *BagitCreator) writeMetainfoToZip(zipWriter *zip.Writer) (map[string]string, error) {
	minfofile := filepath.Join(bc.tempdir, "metainfo.json")
	info, err := os.Stat(minfofile)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot stat %v", minfofile)
	}
	reader, err := os.Open(minfofile)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot open %v", minfofile)
	}
	defer reader.Close()

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		// todo: error handling
	}
	header.Name = "bagarc/metainfo.json"

	// make sure, that compression is ok
	header.Method = zip.Deflate

	zWriter, err := zipWriter.CreateHeader(header)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot create zip file header")
	}
	checksums, err := ChecksumCopy(zWriter, reader, bc.checksum)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot write data to zip")
	}

	return checksums, nil
}

func (bc *BagitCreator) writeRenamesToZip(zipWriter *zip.Writer) (map[string]string, error) {
	renamesfile := filepath.Join(bc.tempdir, "renames.csv")
	renames, err := os.Stat(renamesfile)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot stat %v", renamesfile)
	}
	reader, err := os.Open(renamesfile)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot open %v", renamesfile)
	}
	defer reader.Close()

	header, err := zip.FileInfoHeader(renames)
	if err != nil {
		// todo: error handling
	}
	header.Name = "bagarc/renames.csv"

	// make sure, that compression is ok
	header.Method = zip.Deflate

	zWriter, err := zipWriter.CreateHeader(header)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot create zip file header")
	}
	checksums, err := ChecksumCopy(zWriter, reader, bc.checksum)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot write data to zip")
	}

	return checksums, nil
}

func (bc *BagitCreator) writeBaginfoToZip(zipWriter *zip.Writer) (map[string]string, error) {
	writer, err := zipWriter.Create("bag-info.txt")
	if err != nil {
		return nil, emperror.Wrap(err, "cannot create bag-info.txt in zip")
	}
	bc.bagInfo["Bag-Software-Agent"] = fmt.Sprintf("%s", NAME)
	bc.bagInfo["Bagging-Date"] = time.Now().Format("2006-01-02")
	bc.bagInfo["Payload-Oxum"] = fmt.Sprintf("%v.%v", bc.oxumOctetCount, bc.oxumStreamCount)
	re := regexp.MustCompile(`\r?\n`)
	buf := bytes.NewBufferString("")
	for key, val := range bc.bagInfo {
		val = re.ReplaceAllString(val, "\n    ")
		buf.WriteString(fmt.Sprintf("%s: %s\n", key, val))
	}
	reader := bytes.NewReader(buf.Bytes())

	return ChecksumCopy(writer, reader, bc.checksum)
}

func (bc *BagitCreator) writeBagitToZip(zipWriter *zip.Writer) (error) {
	writer, err := zipWriter.Create("bagit.txt")
	if err != nil {
		return emperror.Wrap(err, "cannot create bagit.txt in zip")
	}
	if _, err := io.WriteString(writer, fmt.Sprintf("BagIt-Version: %s\n", BAGITVERSION )); err != nil {
		return emperror.Wrapf(err, "cannot write to bagit.txt")
	}
	if _, err := io.WriteString(writer, "Tag-File-Character-Encoding: UTF-8\n"); err != nil {
		return emperror.Wrapf(err, "cannot write to bagit.txt")
	}
	return nil
}


// called by file walker.
func (bc *BagitCreator) visitFile(path string, f os.FileInfo, zipWriter *zip.Writer, txn *badger.Txn, err error) error {
	bf, err := NewBagitFile(bc.sourcedir, path, bc.fixFilename)
	if err != nil {
		return emperror.Wrap(err, "error creating BagitFile")
	}
	bc.logger.Infof("walk: %s", bf)
	if bf.IsDir() {
		return nil
	}

	compression := zip.Deflate
	if bc.siegfried != "" {
		if err := bf.GetSiegfried(bc.siegfried); err != nil {
			bc.logger.Errorf("error querying siegfried: %v", err)
		} else {
			// checkManifest wether compression should be avoided
			if len(bf.Siegfried) > 0 {
				id := bf.Siegfried[0].Id
				for _, nc := range bc.storeOnly {
					if nc == id {
						compression = zip.Store
						break
					}
				}
			}
		}
	}
	if err := bf.AddToZip(zipWriter, bc.checksum, compression); err != nil {
		return emperror.Wrapf(err, "cannot add %s to zip", bf)
	}

	// add file to key value store
	jsonstr, err := json.Marshal(bf)
	if err != nil {
		return emperror.Wrap(err, "cannot marshal BagitFile")
	}
	txn.Set([]byte(bf.Path), jsonstr)

	// calculate 0xum
	bc.oxumOctetCount += bf.Size
	bc.oxumStreamCount++

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
