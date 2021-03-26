package ingest

import "time"

type Transfer struct {
	ingest  *Ingest
	loc     *Location
	bagit   *Bagit
	start   time.Time
	end     time.Time
	status  string
	message string
}

func (trans *Transfer) Store() error {
	_, err := trans.ingest.transferStore(trans)
	return err
}
