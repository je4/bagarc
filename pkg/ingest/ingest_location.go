package ingest

import (
	"database/sql"
	"fmt"
	"github.com/goph/emperror"
	"net/url"
)

//
// Location
//

func (i *Ingest) LocationLoadAll() (map[string]*Location, error) {
	sqlstr := fmt.Sprintf("SELECT locationid, name, path, params, encrypted, quality, costs FROM %s.location", i.schema)

	var locations = make(map[string]*Location)

	rows, err := i.db.Query(sqlstr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get locations %s", sqlstr)
	}
	defer rows.Close()
	for rows.Next() {
		loc := &Location{}
		var p string
		if err := rows.Scan(&loc.id, &loc.name, &p, &loc.params, &loc.encrypted, &loc.quality, &loc.costs); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, emperror.Wrapf(err, "cannot get location %s", sqlstr)
		}
		loc.path, err = url.Parse(p)
		if err != nil {
			return nil, emperror.Wrapf(err, "cannot parse url %s", p)
		}
		locations[loc.name] = loc
	}
	return locations, nil
}

func (i *Ingest) LocationLoad(name string) (*Location, error) {
	sqlstr := fmt.Sprintf("SELECT locationid, name, path, params, encrypted, quality, costs FROM %s.location WHERE name = ?", i.schema)
	row := i.db.QueryRow(sqlstr, name)
	loc := &Location{ingest: i}
	var err error
	var p string
	if err := row.Scan(&loc.id, &loc.name, &p, &loc.params, &loc.encrypted, &loc.quality, &loc.costs); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get location %s - %s", sqlstr, name)
	}
	loc.path, err = url.Parse(p)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot parse url %s", p)
	}
	return loc, nil
}

func (i *Ingest) locationStore(loc *Location) (*Location, error) {
	if loc.id == 0 {
		sqlstr := fmt.Sprintf("INSERT INTO %s.location(name, path, params, encrypted, quality, costs) VALUES(?, ?, ?, ?, ?, ?, ?, ?) returning locationid", i.schema)
		row := i.db.QueryRow(sqlstr, loc.name, loc.path.String(), loc.params, loc.encrypted, loc.quality, loc.costs)
		if err := row.Scan(&loc.id); err != nil {
			return nil, emperror.Wrapf(err, "cannot insert location %s - %s", sqlstr, loc.name)
		}
		return loc, nil
	} else {
		sqlstr := fmt.Sprintf("UPDATE %s.location SET name=?, path=?, params=?, encrypted=?, quality=?, costs=? WHERE locationid=?", i.schema)
		if _, err := i.db.Exec(sqlstr, loc.name, loc.path.String(), loc.params, loc.encrypted, loc.quality, loc.costs, loc.id); err != nil {
			return nil, emperror.Wrapf(err, "cannot update location %s - %v", sqlstr, loc.id)
		}
	}
	return nil, fmt.Errorf("LocationStore() - strange things happen - %v", loc)
}
