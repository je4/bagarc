package ingest

import (
	"fmt"
	"github.com/goph/emperror"
)

type Location struct {
	ingest *Ingest
	id int64
	name string
	host string
	path string
	typ string
	params string
	encrypted bool
	quality float64
	costs float64
}

func (loc *Location) HasBagit(name string) (bool, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s.bagit_location bl, %s.bagit b WHERE bl.bagitid=b.bagitid AND bl.locationid=? AND b.name=?")
	row := loc.ingest.db.QueryRow(sql, loc.id, name)
	var num  int64
	if err := row.Scan(&num); err != nil {
		return false, emperror.Wrapf(err, "cannot check for bagit - %s - %v, %v", sql, loc.id, name)
	}
	return num > 0, nil
}

func (loc *Location)