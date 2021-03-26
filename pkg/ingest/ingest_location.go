package ingest

import (
	"database/sql"
	"fmt"
	"github.com/goph/emperror"
)

//
// Location
//

func (i *Ingest) LocationLoad(path string) (*Location, error) {
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

func (i *Ingest) locationStore(loc *Location) (*Location, error) {
	if loc.id == 0 {
		sqlstr := fmt.Sprintf("INSERT INTO %s.location(name, host, type, params, encrypted, quality, costs) VALUES(?, ?, ?, ?, ?, ?, ?) returning locationid", i.schema)
		row := i.db.QueryRow(sqlstr, loc.name, loc.host, loc.typ, loc.params, loc.encrypted, loc.quality, loc.costs)
		if err := row.Scan(&loc.id); err != nil {
			return nil, emperror.Wrapf(err, "cannot insert location %s - %s", sqlstr, loc.name)
		}
		return loc, nil
	} else {
		sqlstr := fmt.Sprintf("UPDATE %s.location SET name=?, host=?, type=?, params=?, encrypted=?, quality=?, costs=? WHERE locationid=?", i.schema)
		if _, err := i.db.Exec(sqlstr, loc.name, loc.host, loc.typ, loc.params, loc.encrypted, loc.quality, loc.costs, loc.id); err != nil {
			return nil, emperror.Wrapf(err, "cannot update location %s - %v", sqlstr, loc.id)
		}
	}
	return nil, fmt.Errorf("LocationStore() - strange things happen - %v", loc)
}
