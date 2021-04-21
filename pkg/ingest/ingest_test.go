package ingest

import (
	"database/sql"
	"fmt"
	"github.com/goph/emperror"
)

func (i *Ingest) TestLoadAll() (map[string]*Test, error) {
	sqlstr := fmt.Sprintf("SELECT testid, name, description FROM %s.test", i.schema)

	var tests = make(map[string]*Test)
	rows, err := i.db.Query(sqlstr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get locations %s", sqlstr)
	}
	defer rows.Close()
	for rows.Next() {
		test := &Test{}
		if err := rows.Scan(&test.id, &test.name, test.description); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, emperror.Wrapf(err, "cannot get location %s", sqlstr)
		}
		tests[test.name] = test
	}
	return tests, nil
}

func (i *Ingest) testLoad(name string) (*Test, error) {
	sqlstr := fmt.Sprintf("SELECT testid, name, description FROM %s.test WHERE name = ?", i.schema)
	row := i.db.QueryRow(sqlstr, name)
	test := &Test{}
	if err := row.Scan(&test.id, &test.name, test.description); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get location %s - %s", sqlstr, name)
	}
	return test, nil
}
