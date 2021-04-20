package ingest

import "time"

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
