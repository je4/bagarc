package ingest

import (
	"database/sql"
	"fmt"
	"github.com/goph/emperror"
)

type Ingest struct {
	db     *sql.DB
	schema string
}

func NewIngest(db *sql.DB, dbschema string) (*Ingest, error) {
	i := &Ingest{
		db:     db,
		schema: dbschema,
	}
	return i, nil
}

func (i *Ingest) LoadLocation(path string) (*Location, error) {
	sqlstr := fmt.Sprintf("SELECT locationid, name, host, path, type, params, encrypted, quality, costs FROM %s.location WHERE path = ?", i.schema)
	row := i.db.QueryRow(sqlstr, path)
	loc := &Location{ingest: i}
	if err := row.Scan(&loc.id, &loc.name, &loc.host, &loc.path, &loc.typ, &loc.params, &loc.encrypted, &loc.quality, &loc.costs); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get location %s - %s", sqlstr, path)
	}
	return loc, nil
}

func (i *Ingest) LoadBagit(name string) (*Bagit, error) {
	sqlstr := fmt.Sprintf("SELECT bagitid, name, filesize, sha512, sha512_aes, report, creator, creationdate FROM %s.bagit WHERE name=?", i.schema)
	row := i.db.QueryRow(sqlstr, name)
	bagit := &Bagit{
		ingest: i,
	}
	var sha512_aes sql.NullString
	if err := row.Scan(&bagit.id, &bagit.name, &bagit.size, &bagit.sha512, &sha512_aes, &bagit.creator, &bagit.report); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get bagit %s - %s", sqlstr, name)
	}
	bagit.sha512_aes = sha512_aes.String
	return bagit, nil
}

func (i *Ingest) HasBagit(loc *Location, bagit *Bagit) (bool, error) {
	sqlstr := fmt.Sprintf("SELECT COUNT(*) FROM %s.bagit_location bl, %s.bagit b WHERE bl.bagitid=b.bagitid AND bl.locationid=? AND b.bagitid=?",
		loc.ingest.schema,
		loc.ingest.schema)
	row := loc.ingest.db.QueryRow(sqlstr, loc.id, bagit.id)
	var num int64
	if err := row.Scan(&num); err != nil {
		return false, emperror.Wrapf(err, "cannot check for bagit - %s - %v, %v", sqlstr, loc.id, bagit.id)
	}
	return num > 0, nil
}

func (i *Ingest) LoadTransfer(loc *Location, bagit *Bagit) (*Transfer, error) {
	sqlstr := fmt.Sprintf("SELECT transfer_start, transfer_end, status, message FROM %s.bagit_location WHERE bagitid=? AND locationid=?",
		loc.ingest.schema)
	row := loc.ingest.db.QueryRow(sqlstr, loc.id, bagit.id)
	trans := &Transfer{
		ingest: i,
		loc:    loc,
		bagit:  bagit,
	}
	var start, end sql.NullTime
	if err := row.Scan(&start, &end, &trans.status, &trans.message); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot load transfer - %s - %v, %v", sqlstr, loc.id, bagit.id)
	}
	trans.start = start.Time
	trans.end = end.Time
	return trans, nil
}
