package bagit

import "net/url"

type Location struct {
	ingest    *Ingest
	id        int64
	name      string
	path      *url.URL
	params    string
	encrypted bool
	quality   float64
	costs     float64
}

func (loc *Location) HasBagit(bagit *IngestBagit) (bool, error) {
	return loc.ingest.hasBagit(loc, bagit)
}

func (loc *Location) LoadTransfer(bagit *IngestBagit) (*Transfer, error) {
	return loc.ingest.TransferLoad(loc, bagit)
}

func (loc *Location) Store() error {
	_, err := loc.ingest.locationStore(loc)
	return err
}

func (loc *Location) GetPath() *url.URL {
	return loc.path
}
