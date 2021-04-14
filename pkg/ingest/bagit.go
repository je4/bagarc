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