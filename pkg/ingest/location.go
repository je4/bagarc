package ingest

type Location struct {
	ingest    *Ingest
	id        int64
	name      string
	host      string
	path      string
	typ       string
	params    string
	encrypted bool
	quality   float64
	costs     float64
}

func (loc *Location) HasBagit(bagit *Bagit) (bool, error) {
	return loc.ingest.HasBagit(loc, bagit)
}

func (loc *Location) LoadTransfer(bagit *Bagit) (*Transfer, error) {
	return loc.ingest.LoadTransfer(loc, bagit)
}
