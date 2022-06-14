package ocfl

import (
	"errors"
	"fmt"
	"github.com/goph/emperror"
	"io"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const VERSION = "1.0"

type DigestAlgorithm string

const (
	DigestMD5        DigestAlgorithm = "md5"
	DigestSHA1       DigestAlgorithm = "sha1"
	DigestSHA256     DigestAlgorithm = "sha256"
	DigestSHA512     DigestAlgorithm = "sha512"
	DigestBlake2b512 DigestAlgorithm = "blake2b-512"
)

var rootConformanceDeclaration = fmt.Sprintf("0=ocfl_%s", VERSION)

type OCFLFS interface {
	fs.ReadDirFS
	Create(name string) (io.WriteCloser, error)
}

type OCFL struct {
	fs OCFLFS
	i  *Inventory
}

func NewOCFL(fs OCFLFS) (*OCFL, error) {
	ocfl := &OCFL{fs: fs}
	return ocfl, nil
}

var versionRegexp = regexp.MustCompile("^v(\\d+)/$")
var inventoryDigestRegexp = regexp.MustCompile(fmt.Sprintf("^(?i)inventory\\.json\\.(%s|%s)$", string(DigestSHA512), string(DigestSHA256)))

func (ocfl *OCFL) readInventory(folder string) (*Inventory, error) {
	folder = strings.TrimSuffix(filepath.ToSlash(folder), "/")
	inventoryFilename := fmt.Sprintf("%s/inventory.json", folder)
	fpInventory, err := ocfl.fs.Open(inventoryFilename)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot open %s", inventoryFilename)
	}
	defer fpInventory.Close()
	var digest DigestAlgorithm
	var digestStr string
	for _, d := range []DigestAlgorithm{DigestSHA512, DigestSHA256} {
		digestFilename := fmt.Sprintf("%s.%s", inventoryFilename, string(d))
		fpDigest, err := ocfl.fs.Open(digestFilename)
		if err != nil {
			continue
		}
		dBytes, err := io.ReadAll(fpDigest)
		if err != nil {
			fpDigest.Close()
			return nil, emperror.Wrapf(err, "cannot read digest %s", digestFilename)
		}
		fpDigest.Close()
		digestStr = string(dBytes)
		digest = d
	}

}

func (ocfl *OCFL) Load() error {
	fp, err := ocfl.fs.Open(rootConformanceDeclaration)
	if err != nil {
		return emperror.Wrap(err, "cannot load OCFL layout - no conformance declaration found")
	}
	fp.Close()
	entries, err := ocfl.fs.ReadDir(".")
	for _, entry := range entries {
		name := entry.Name()
		if matches := inventoryDigestRegexp.FindStringSubmatch(name); matches != nil {
			var digest DigestAlgorithm
			switch matches[1] {
			case string(DigestSHA512):
				digest = DigestSHA512
			case string(DigestSHA256):
				digest = DigestSHA256
			default:
				return errors.New(fmt.Sprintf("invalid digest file for inventory - %s", name))
			}

		} else {
			continue
		}
	}
}

func (ocfl *OCFL) Init() error {
	// first check whether ocfl is not empty
	fp, err := ocfl.fs.Open(rootConformanceDeclaration)
	if err != nil {
		if err != fs.ErrNotExist {
			return emperror.Wrap(err, "cannot initialize OCFL layout")
		}
		fp, err := ocfl.fs.Create(rootConformanceDeclaration)
		if err != nil {
			return emperror.Wrapf(err, "cannot create %s", rootConformanceDeclaration)
		}
		if err := fp.Close(); err != nil {
			return emperror.Wrapf(err, "cannot close %s", rootConformanceDeclaration)
		}
		ocfl.i = NewInventory()
	} else {
		if err := fp.Close(); err != nil {
			return emperror.Wrapf(err, "cannot initialize OCFL layout - error closing %s", rootConformanceDeclaration)
		}
		// check for next version
		entries, err := ocfl.fs.ReadDir(".")
		if err != nil {
			return emperror.Wrap(err, "cannot read root directory of OCFL layout")
		}
		for _, entry := range entries {
			matches := versionRegexp.FindStringSubmatch(entry.Name())
			if matches == nil {
				continue
			}
			vStr := matches[1]
			v, err := strconv.Atoi(vStr)
			if err != nil {
				return emperror.Wrapf(err, "suspicious error getting version number of %s", entry.Name())
			}
			if v >= ocfl.version {
				ocfl.version = v + 1
			}
		}
	}

	return nil
}
