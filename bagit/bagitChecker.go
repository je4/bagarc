package bagit

import (
	"archive/zip"
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
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
type BagitChecker struct {
	logger    *logging.Logger
	bagitfile string     // zip to check
	db        *badger.DB // file storage
	tmpdir    string     // folder for temporary files
}

func NewBagitChecker(bagitFile string, tmpdir string, db *badger.DB, logger *logging.Logger) (*BagitChecker, error) {
	checker := &BagitChecker{
		logger:    logger,
		bagitfile: bagitFile,
		db:        db,
		tmpdir:    tmpdir,
	}

	return checker, nil
}

func (bc *BagitChecker) Run() error {
	r, err := zip.OpenReader(bc.bagitfile)
	if err != nil {
		return emperror.Wrapf(err, "cannot open zip %v", bc.bagitfile)
	}
	defer r.Close()

	bc.logger.Infof("pass #1: check format, encodings, checksums")
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
	for _, f := range r.File {
		found := manifestRegexp.FindStringSubmatch(f.Name)
		if found != nil {
			checksums = append(checksums, found[1])
			rc, err := f.Open()
			if err != nil {
				return emperror.Wrapf(err, "cannot read %s", f.Name)
			}
			defer rc.Close()
			h, err := os.Create(fmt.Sprintf("%s/%s", bc.tmpdir, f.Name))
			if err != nil {
				return emperror.Wrapf(err, "cannot create file %s/%s", bc.tmpdir, f.Name)
			}
			defer h.Close()
			if _, err := io.Copy(h, rc); err != nil {
				return emperror.Wrapf(err, "cannot write manifest file %s/%s", bc.tmpdir, f.Name)
			}
		}
		if strings.HasPrefix(filepath.ToSlash(f.Name), "data/") {
			oxumStreamCount++
			oxumOctetCount += int64(f.UncompressedSize64)
		}
		if f.Name == "bag-info.txt" {
			rc, err := f.Open()
			if err != nil {
				return emperror.Wrapf(err, "cannot read bag-info.txt")
			}
			defer rc.Close()
			scanner := bufio.NewScanner(rc)
			for scanner.Scan() {
				line := scanner.Text()
				if found := oxumRegexp.FindStringSubmatch(line); found != nil {
					baginfoOxumOctetCount, err = strconv.ParseInt(found[1], 10, 64)
					if err != nil {
						bc.logger.Errorf("invalid octet count in bag-info.txt: %v", line)
						break
					}
					baginfoOxumStreamCount, err = strconv.ParseInt(found[2], 10, 64)
					if err != nil {
						bc.logger.Errorf("invalid stream count in bag-info.txt: %v", line)
						baginfoOxumOctetCount = 0
						break
					}
					break
				}
			}
		} // bag-info.txt

		if f.Name == "bagit.txt" {
			rc, err := f.Open()
			defer rc.Close()
			if err != nil {
				log.Fatal(err)
			}
			br := bufio.NewReader(rc)
			line1, err := br.ReadString('\n')
			if err != nil {
				return emperror.Wrapf(err, "cannot read line 1 of bagit.txt")
			}
			found = versionRegexp.FindStringSubmatch(line1)
			if found == nil {
				return errors.New(fmt.Sprintf("invalid version line in bagit.txt: %s", line1))
			}
			version = found[1]
			line2, err := br.ReadString('\n')
			if err != nil {
				return emperror.Wrapf(err, "cannot read line 2 of bagit.txt")
			}
			found = encodingRegexp.FindStringSubmatch(line2)
			if found == nil {
				return errors.New(fmt.Sprintf("invalid encodingName line in bagit.txt: %s", line2))
			}
			encodingName = found[1]

			bc.logger.Infof("Bagit v%v, encodingName %v", version, encodingName)
		} // bagit.txt
	} // iterate through zip directory

	if version == "" || encodingName == "" {
		return errors.New("invalid or no bagit.txt file")
	}
	// if this check fails, there's no need for checksum generation
	if baginfoOxumOctetCount > 0 && baginfoOxumStreamCount > 0 {
		if baginfoOxumOctetCount != oxumOctetCount || baginfoOxumStreamCount != oxumStreamCount {
			return errors.New(fmt.Sprintf("invalid Payload-Oxum: %v.%v <> %v.%v",
				oxumStreamCount, oxumStreamCount,
				baginfoOxumOctetCount, baginfoOxumStreamCount))
		}
	}

	var encDecoder *encoding.Decoder
	switch encodingName {
	case "UTF-8":
		encDecoder = unicode.UTF8.NewDecoder()
		// todo: which decoder could be relevant????
	default:
		return errors.New(fmt.Sprintf("cannot handle encoding %s", encodingName))
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
		return errors.New("no manifest with known checksum found")
	}
	bc.logger.Infof("using %v checksum for testing", checksum)

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
	if err := bc.db.Update(func(txn *badger.Txn) error {
		for _, f := range r.File {
			rc, err := f.Open()
			if err != nil {
				return emperror.Wrapf(err, "cannot read %s", f.Name)
			}
			defer rc.Close()
			checksumSink.Reset()
			if _, err := io.Copy(checksumSink, rc); err != nil {
				return emperror.Wrapf(err, "cannot calculate checksum for %s", f.Name)
			}
			sum := checksumSink.Sum(nil)
			sumstr := fmt.Sprintf("%x", sum)
			if err := txn.Set([]byte(filepath.ToSlash(f.Name)), []byte(sumstr)); err != nil {
				return emperror.Wrapf(err, "cannot write checksum for %s", f.Name)
			}
			bc.logger.Infof("%s[%s] %s", sumstr, checksum, f.Name)
		}
		return nil
	}); err != nil {
		return emperror.Wrapf(err, "cannot write checksum for into database")
	}

	// read manifest file and check sums
	manifestLineRegexp := regexp.MustCompile(`([A-Fa-f0-9]+)\s+(.+)`)
	manifestName := fmt.Sprintf("%s/manifest-%s.txt", bc.tmpdir, checksum)
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
		if err := bc.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(mfilename))
			if err != nil {
				return emperror.Wrapf(err, "no entry for %s", mfilename)
			}
			if err := item.Value(func (val []byte) error {
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
		bc.logger.Infof("%s: ok", mfilename)
	}
	bc.logger.Infof("manifest-%s.txt verified ok", checksum)
	return nil
}
