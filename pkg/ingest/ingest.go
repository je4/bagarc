package ingest

import (
	"database/sql"
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
