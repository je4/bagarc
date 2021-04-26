package bagit

import "net/url"

type IngestLocation struct {
	ingest    *Ingest
	id        int64
	name      string
	path      *url.URL
	params    string
	encrypted bool
	quality   float64
	costs     float64
}

func (loc *IngestLocation) HasBagit(bagit *IngestBagit) (bool, error) {
	return loc.ingest.hasBagit(loc, bagit)
}

func (loc *IngestLocation) LoadTransfer(bagit *IngestBagit) (*Transfer, error) {
	return loc.ingest.transferLoad(loc, bagit)
}

func (loc *IngestLocation) Store() error {
	_, err := loc.ingest.locationStore(loc)
	return err
}

func (loc *IngestLocation) GetPath() *url.URL {
	return loc.path
}

func (loc *IngestLocation) IsEncrypted() bool {
	return loc.encrypted
}
