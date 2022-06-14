package bagit

import (
	"archive/zip"
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/op/go-logging"
	"golang.org/x/crypto/sha3"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// describes a structure for ingest process
type Bagit struct {
	logger    *logging.Logger
	bagitfile string     // zip to checkManifest
	db        *badger.DB // file storage
	tmpdir    string     // folder for temporary files
	indexer   string
}

func NewBagit(bagitFile string, tmpdir string, db *badger.DB, logger *logging.Logger) (*Bagit, error) {
	checker := &Bagit{
		logger:    logger,
		bagitfile: bagitFile,
		db:        db,
		tmpdir:    tmpdir,
	}

	return checker, nil
}

func (bagit *Bagit) checkFormal(zipReader *zip.ReadCloser) (string, string, error) {
	bagit.logger.Infof("pass #1: checkManifest format, encodings, checksums")
	var version string
	var encodingName string
	var checksums = []string{}
	var baginfoOxumOctetCount int64
	var baginfoOxumStreamCount int64
	var oxumOctetCount int64
	var oxumStreamCount int64

	// first get all manifestRegexp files
	manifestRegexp := regexp.MustCompile(`manifest-(md5|sha1|sha256|sha512|sha3-256|sha3-512).txt`)
	versionRegexp := regexp.MustCompile(`BagIt-Version: ([0-9]+\.[0-9]+)`)
	encodingRegexp := regexp.MustCompile(`Tag-File-Character-Encoding: (.+)`)
	oxumRegexp := regexp.MustCompile(`Payload-Oxum\s*:\s*([0-9]+)\.([0-9]+)`)
	for _, f := range zipReader.File {
		slashPath := filepath.ToSlash(f.Name)
		found := manifestRegexp.FindStringSubmatch(slashPath)
		if found != nil {
			checksums = append(checksums, found[1])
			rc, err := f.Open()
			if err != nil {
				return "", "", emperror.Wrapf(err, "cannot read %s", slashPath)
			}
			defer rc.Close()
			h, err := os.Create(fmt.Sprintf("%s/%s", bagit.tmpdir, slashPath))
			if err != nil {
				return "", "", emperror.Wrapf(err, "cannot create file %s/%s", bagit.tmpdir, slashPath)
			}
			defer h.Close()
			if _, err := io.Copy(h, rc); err != nil {
				return "", "", emperror.Wrapf(err, "cannot write manifest file %s/%s", bagit.tmpdir, slashPath)
			}
		}

		if slashPath == "bagarc/renames.csv" {
			rc, err := f.Open()
			if err != nil {
				return "", "", emperror.Wrapf(err, "cannot read %s", "renames.csv")
			}
			defer rc.Close()
			h, err := os.Create(filepath.Join(bagit.tmpdir, "renames.csv"))
			if err != nil {
				return "", "", emperror.Wrapf(err, "cannot create file %s/%s", bagit.tmpdir, "renames.csv")
			}
			defer h.Close()
			if _, err := io.Copy(h, rc); err != nil {
				return "", "", emperror.Wrapf(err, "cannot write manifest file %s/%s", bagit.tmpdir, "renames.csv")
			}
		}
		if strings.HasPrefix(slashPath, "data/") {
			oxumStreamCount++
			oxumOctetCount += int64(f.UncompressedSize64)
		}
		if slashPath == "bag-info.txt" {
			rc, err := f.Open()
			if err != nil {
				return "", "", emperror.Wrapf(err, "cannot read bag-info.txt")
			}
			defer rc.Close()
			scanner := bufio.NewScanner(rc)
			for scanner.Scan() {
				line := scanner.Text()
				if found := oxumRegexp.FindStringSubmatch(line); found != nil {
					baginfoOxumOctetCount, err = strconv.ParseInt(found[1], 10, 64)
					if err != nil {
						bagit.logger.Errorf("invalid octet count in bag-info.txt: %v", line)
						break
					}
					baginfoOxumStreamCount, err = strconv.ParseInt(found[2], 10, 64)
					if err != nil {
						bagit.logger.Errorf("invalid stream count in bag-info.txt: %v", line)
						baginfoOxumOctetCount = 0
						break
					}
					break
				}
			}
		} // bag-info.txt

		if slashPath == "bagit.txt" {
			rc, err := f.Open()
			defer rc.Close()
			if err != nil {
				log.Fatal(err)
			}
			br := bufio.NewReader(rc)
			line1, err := br.ReadString('\n')
			if err != nil {
				return "", "", emperror.Wrapf(err, "cannot read line 1 of bagit.txt")
			}
			found = versionRegexp.FindStringSubmatch(line1)
			if found == nil {
				return "", "", errors.New(fmt.Sprintf("invalid version line in bagit.txt: %s", line1))
			}
			version = found[1]
			line2, err := br.ReadString('\n')
			if err != nil {
				return "", "", emperror.Wrapf(err, "cannot read line 2 of bagit.txt")
			}
			found = encodingRegexp.FindStringSubmatch(line2)
			if found == nil {
				return "", "", errors.New(fmt.Sprintf("invalid encodingName line in bagit.txt: %s", line2))
			}
			encodingName = found[1]

			bagit.logger.Infof("Bagit v%v, encodingName %v", version, encodingName)
		} // bagit.txt
	} // iterate through zip directory

	if version == "" || encodingName == "" {
		return "", "", errors.New("invalid or no bagit.txt file")
	}
	// if this checkManifest fails, there's no need for checksum generation
	if baginfoOxumOctetCount > 0 && baginfoOxumStreamCount > 0 {
		if baginfoOxumOctetCount != oxumOctetCount || baginfoOxumStreamCount != oxumStreamCount {
			return "", "", errors.New(fmt.Sprintf("invalid Payload-Oxum: %v.%v <> %v.%v",
				oxumStreamCount, oxumStreamCount,
				baginfoOxumOctetCount, baginfoOxumStreamCount))
		}
	}

	// find the checksum with highest rating to test
	var checksumHierarchy = map[string]int{
		"md5":      1,
		"sha1":     2,
		"sha3-256": 3,
		"sha256":   4,
		"sha3-512": 5,
		"sha512":   6,
	}
	var checksum string
	for _, cs := range checksums {
		cs = strings.ToLower(cs)
		if checksum == "" {
			checksum = cs
		} else {
			if checksumHierarchy[cs] > checksumHierarchy[checksum] {
				checksum = cs
			}
		}
	}
	if checksum == "" {
		return "", "", errors.New("no manifest with known checksum found")
	}
	bagit.logger.Infof("using %v checksum for testing", checksum)

	return checksum, encodingName, nil
}

func (bagit *Bagit) checkManifest(zipReader *zip.ReadCloser, checksum string, encodingName string, metadataSink, bag_info io.Writer) error {
	var encDecoder *encoding.Decoder
	switch encodingName {
	case "UTF-8":
		encDecoder = unicode.UTF8.NewDecoder()
		// todo: which other decoder could be relevant????
	default:
		return errors.New(fmt.Sprintf("cannot handle encoding %s", encodingName))
	}

	bagit.logger.Infof("using %v checksum and manifest encoding %v for testing", checksum, encodingName)

	var checksumSink hash.Hash
	switch checksum {
	case "md5":
		checksumSink = md5.New()
	case "sha1":
		checksumSink = sha1.New()
	case "sha256":
		checksumSink = sha256.New()
	case "sha512":
		checksumSink = sha512.New()
	case "sha3-256":
		checksumSink = sha3.New256()
	case "sha3-512":
		checksumSink = sha3.New512()
	default:
		return errors.New(fmt.Sprintf("unknown checksum %s", checksum))
	}
	if err := bagit.db.Update(func(txn *badger.Txn) error {
		for _, f := range zipReader.File {
			rc, err := f.Open()
			if err != nil {
				return emperror.Wrapf(err, "cannot read %s", f.Name)
			}
			defer rc.Close()
			checksumSink.Reset()
			writers := []io.Writer{checksumSink}
			name := filepath.ToSlash(f.Name)
			if metadataSink != nil && (name == "bagarc/metainfo.json") {
				writers = append(writers, metadataSink)
			}
			if bag_info != nil && (name == "bag-info.txt") {
				writers = append(writers, bag_info)
			}
			sink := io.MultiWriter(writers...)
			if _, err := io.Copy(sink, rc); err != nil {
				return emperror.Wrapf(err, "cannot calculate checksum for %s", f.Name)
			}
			sum := checksumSink.Sum(nil)
			sumstr := fmt.Sprintf("%x", sum)
			if err := txn.Set([]byte(filepath.ToSlash(f.Name)), []byte(sumstr)); err != nil {
				return emperror.Wrapf(err, "cannot write checksum for %s", f.Name)
			}
			bagit.logger.Infof("%s[%s] %s", sumstr, checksum, f.Name)
		}
		return nil
	}); err != nil {
		return emperror.Wrapf(err, "cannot write checksum for into database")
	}

	// read manifest file and checkManifest sums
	manifestLineRegexp := regexp.MustCompile(`([A-Fa-f0-9]+)\s+(.+)`)
	manifestName := fmt.Sprintf("%s/manifest-%s.txt", bagit.tmpdir, checksum)
	of, err := os.Open(manifestName)
	if err != nil {
		return emperror.Wrapf(err, "cannot open %s", manifestName)
	}
	defer of.Close()
	// use the right decoder
	of2 := transform.NewReader(of, encDecoder)
	scanner := bufio.NewScanner(of2)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		// ignore empty lines
		if line == "" {
			continue
		}
		found := manifestLineRegexp.FindStringSubmatch(line)
		if found == nil {
			return errors.New(fmt.Sprintf("invalid line in %s: %s", manifestName, line))
		}
		var mfilename = found[2]
		var mhash = strings.ToLower(found[1])
		var fsum string
		if err := bagit.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(mfilename))
			if err != nil {
				return emperror.Wrapf(err, "no entry for %s", mfilename)
			}
			if err := item.Value(func(val []byte) error {
				fsum = string(val)
				return nil
			}); err != nil {
				return emperror.Wrapf(err, "cannot get value of %s", mfilename)
			}
			return nil
		}); err != nil {
			return emperror.Wrapf(err, "%s not in archive", mfilename)
		}
		if fsum != mhash {
			return errors.New(fmt.Sprintf("invalid checksum %s for file %s", fsum, mfilename))
		}
		bagit.logger.Infof("%s: ok", mfilename)
	}
	bagit.logger.Infof("manifest-%s.txt verified ok", checksum)
	return nil
}

func (bagit *Bagit) Check(metadataSink, bag_info_txt io.Writer) error {
	r, err := zip.OpenReader(bagit.bagitfile)
	if err != nil {
		return emperror.Wrapf(err, "cannot open zip %v", bagit.bagitfile)
	}
	defer r.Close()

	checksum, encodingName, err := bagit.checkFormal(r)
	if err != nil {
		return emperror.Wrapf(err, "error running pass #1")
	}

	if err := bagit.checkManifest(r, checksum, encodingName, metadataSink, bag_info_txt); err != nil {
		return emperror.Wrapf(err, "error running pass #2")
	}

	return nil
}
func (bagit *Bagit) extract(zipReader *zip.ReadCloser, targetFolder string, restoreFilenames bool, checksum string, encodingName string) error {
	if err := os.MkdirAll(targetFolder, os.ModePerm); err != nil {
		return emperror.Wrapf(err, "cannot create %s", targetFolder)
	}

	// read renames into key value store if necessary
	if restoreFilenames {
		bagit.db.Update(func(txn *badger.Txn) error {
			csvFile, err := os.Open(filepath.Join(bagit.tmpdir, "renames.csv"))
			if err != nil && !os.IsNotExist(err) {
				return emperror.Wrapf(err, "cannot open renames.csv")
			}
			if err == nil {
				defer csvFile.Close()
				csvReader := csv.NewReader(csvFile)
				for {
					record, err := csvReader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						return emperror.Wrapf(err, "error reading renames.csv")
					}
					if len(record) != 2 {
						return errors.New(fmt.Sprintf("no tupel in renames.csv: %v", record))
					}
					// set ZIP Path as key, original name as value
					if err := txn.Set([]byte(fmt.Sprintf("rename:%s", record[1])), []byte(record[0])); err != nil {
						return emperror.Wrapf(err, "cannot store key rename:%s", record[1])
					}
				}
			}
			return nil
		})
	}

	for _, f := range zipReader.File {
		// just for th	e defer ....
		err := func() error {
			targetFilename := f.Name
			slashName := filepath.ToSlash(f.Name)
			if strings.HasPrefix(slashName, "data/") && restoreFilenames {
				bagit.db.View(func(txn *badger.Txn) error {
					key := fmt.Sprintf("rename:%s", strings.TrimPrefix(slashName, "data/"))
					val, err := txn.Get([]byte(key))
					if err != nil {
						return err
					}
					val.Value(func(val2 []byte) error {
						targetFilename = filepath.Join("data", string(val2))
						return nil
					})
					return nil
				})
			}
			sourceFile, err := f.Open()
			if err != nil {
				return emperror.Wrapf(err, "cannot open compressed file %s", f.Name)
			}
			bagit.logger.Infof("extracting [%s] to [%s]", f.Name, targetFilename)
			targetFileFull := filepath.Join(targetFolder, targetFilename)
			dir := filepath.Dir(targetFileFull)
			if err := os.MkdirAll(dir, os.ModePerm); err != nil {
				return emperror.Wrapf(err, "cannot create folder %s", dir)
			}
			targetFile, err := os.Create(targetFileFull)
			if err != nil {
				return emperror.Wrapf(err, "cannot create file %s", targetFileFull)
			}

			if _, err := io.Copy(targetFile, sourceFile); err != nil {
				targetFile.Close()
				return emperror.Wrapf(err, "cannot write %s -> %s", f.Name, targetFileFull)
			}
			targetFile.Close()
			tf, err := os.Open(targetFileFull)
			if err != nil {
				return emperror.Wrapf(err, "cannot open %s", targetFileFull)
			}
			defer tf.Close()
			cSum, err := Checksum(tf, checksum)
			if err != nil {
				return emperror.Wrapf(err, "cannot create checksum of %s", targetFileFull)
			}
			bagit.logger.Infof("%s: %s", f.Name, cSum)
			if err := bagit.db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(filepath.ToSlash(f.Name)), []byte(cSum))
			}); err != nil {
				return emperror.Wrapf(err, "cannot set checksum for key %s", f.Name)
			}
			return nil
		}()
		if err != nil {
			return emperror.Wrapf(err, "cannot handle %s", f.Name)
		}
	} // range zipReader.File

	var encDecoder *encoding.Decoder
	switch encodingName {
	case "UTF-8":
		encDecoder = unicode.UTF8.NewDecoder()
		// todo: which other decoder could be relevant????
	default:
		return errors.New(fmt.Sprintf("cannot handle encoding %s", encodingName))
	}

	bagit.logger.Infof("using %v checksum and manifest encoding %v for testing", checksum, encodingName)

	// read manifest file and checkManifest sums
	manifestLineRegexp := regexp.MustCompile(`([A-Fa-f0-9]+)\s+(.+)`)
	manifestName := fmt.Sprintf("%s/manifest-%s.txt", bagit.tmpdir, checksum)
	of, err := os.Open(manifestName)
	if err != nil {
		return emperror.Wrapf(err, "cannot open %s", manifestName)
	}
	defer of.Close()
	// use the right decoder
	of2 := transform.NewReader(of, encDecoder)
	scanner := bufio.NewScanner(of2)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		// ignore empty lines
		if line == "" {
			continue
		}
		found := manifestLineRegexp.FindStringSubmatch(line)
		if found == nil {
			return errors.New(fmt.Sprintf("invalid line in %s: %s", manifestName, line))
		}
		var mfilename = found[2]
		var mhash = strings.ToLower(found[1])
		var fsum string
		if err := bagit.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(mfilename))
			if err != nil {
				return emperror.Wrapf(err, "no entry for %s", mfilename)
			}
			if err := item.Value(func(val []byte) error {
				fsum = string(val)
				return nil
			}); err != nil {
				return emperror.Wrapf(err, "cannot get value of %s", mfilename)
			}
			return nil
		}); err != nil {
			return emperror.Wrapf(err, "%s not in archive", mfilename)
		}
		if fsum != mhash {
			return errors.New(fmt.Sprintf("invalid checksum %s for file %s", fsum, mfilename))
		}
		bagit.logger.Infof("%s: ok", mfilename)
	}
	bagit.logger.Infof("manifest-%s.txt verified ok", checksum)

	return nil
}

func (bagit *Bagit) Extract(targetFolder string, restoreFilenames bool) error {
	r, err := zip.OpenReader(bagit.bagitfile)
	if err != nil {
		return emperror.Wrapf(err, "cannot open zip %v", bagit.bagitfile)
	}
	defer r.Close()

	checksum, encodingName, err := bagit.checkFormal(r)
	if err != nil {
		return emperror.Wrapf(err, "error running pass #1")
	}

	if err := bagit.extract(r, targetFolder, restoreFilenames, checksum, encodingName); err != nil {
		return emperror.Wrapf(err, "cannot extract bagit to %s", targetFolder)
	}
	return nil
}
