package bagit

import (
	"fmt"
	"github.com/goph/emperror"
	"os"
	"path/filepath"
	"time"
)

//
// IngestBagit
//
type IngestBagit struct {
	ingest       *Ingest
	id           int64
	name         string
	size         int64
	sha512       string
	sha512_aes   string
	report       string
	creator      string
	creationdate time.Time
}

func (bagit *IngestBagit) Store() error {
	_, err := bagit.ingest.bagitStore(bagit)
	return err
}

func (bagit *IngestBagit) ExistsAt(location *IngestLocation) (bool, error) {
	return bagit.ingest.bagitExistsAt(bagit, location)
}

func (bagit *IngestBagit) Check(location *IngestLocation, checkInterval time.Duration) (bool, error) {
	exists, err := bagit.ExistsAt(location)
	if err != nil {
		return false, emperror.Wrapf(err, "cannot check bagit location %v", location.name)
	}
	if !exists {
		return false, fmt.Errorf("bagit %v not at location %v", bagit.name, location.name)
	}

	return true, nil
}

func (bagit *IngestBagit) AddContent(zippath, diskpath string, filesize int64, sha256, sha512, md5 string) error {
	return bagit.ingest.bagitAddContent(bagit, zippath, diskpath, filesize, sha256, sha512, md5)
}

func (bagit *IngestBagit) GetKey() []byte {
	key, err := os.ReadFile(filepath.Join(bagit.ingest.keyDir, bagit.name+".key"))
	if err != nil {
		return nil
	}
	return key
}

func (bagit *IngestBagit) SetKey(key []byte) error {
	fname := filepath.Join(bagit.ingest.keyDir, bagit.name+".key")
	if err := os.WriteFile(fname, key, 0600); err != nil {
		return emperror.Wrapf(err, "cannot write file %s", fname)
	}
	return nil
}

func (bagit *IngestBagit) GetIV() []byte {
	iv, err := os.ReadFile(filepath.Join(bagit.ingest.keyDir, bagit.name+".iv"))
	if err != nil {
		return nil
	}
	return iv
}

func (bagit *IngestBagit) SetIV(iv []byte) error {
	fname := filepath.Join(bagit.ingest.keyDir, bagit.name+".iv")
	if err := os.WriteFile(fname, iv, 0600); err != nil {
		return emperror.Wrapf(err, "cannot write file %s", fname)
	}
	return nil
}
