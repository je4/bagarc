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
	sql := fmt.Sprintf("SELECT locationid, name, host, path, type, params, encrypted, quality, costs FROM %s.location WHERE path = ?", i.schema)
	row := i.db.QueryRow(sql, path)
	loc := &Location{ingest: i}
	if err := row.Scan(&loc.id, &loc.name, &loc.host, &loc.path, &loc.typ, &loc.params, &loc.encrypted, &loc.quality, &loc.costs); err != nil {
		return nil, emperror.Wrapf(err, "cannot get location %s - %s", sql, path)
	}
	return loc, nil
}
