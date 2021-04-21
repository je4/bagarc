package ingest

import (
	"fmt"
	"github.com/goph/emperror"
	"time"
)

type Bagit struct {
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

func (bagit *Bagit) Store() error {
	_, err := bagit.ingest.bagitStore(bagit)
	return err
}

func (bagit *Bagit) ExistsAt(location *Location) (bool, error) {
	return bagit.ingest.bagitExistsAt(bagit, location)
}

func (bagit *Bagit) StoreAt(location *Location, transfer_start, transfer_end time.Time, status, message string) error {
	err := bagit.ingest.bagitStoreAt(bagit, location, transfer_start, transfer_end, status, message)
	return err
}

func (bagit *Bagit) Check(location *Location, checkInterval time.Duration) (bool, error) {
	exists, err := bagit.ExistsAt(location)
	if err != nil {
		return false, emperror.Wrapf(err, "cannot check bagit location %v", location.name)
	}
	if !exists {
		return false, fmt.Errorf("bagit %v not at location %v", bagit.name, location.name)
	}

	return true, nil
}
