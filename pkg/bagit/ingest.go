package bagit

import (
	"database/sql"
	"fmt"
	"github.com/goph/emperror"
	"net/url"
	"time"
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

func (i *Ingest) TestLoadAll() (map[string]*IngestTest, error) {
	sqlstr := fmt.Sprintf("SELECT testid, name, description FROM %s.test", i.schema)

	var tests = make(map[string]*IngestTest)
	rows, err := i.db.Query(sqlstr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get tests %s", sqlstr)
	}
	defer rows.Close()
	for rows.Next() {
		test := &IngestTest{}
		if err := rows.Scan(&test.id, &test.name, &test.description); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, emperror.Wrapf(err, "cannot get tests %s", sqlstr)
		}
		tests[test.name] = test
	}
	return tests, nil
}

func (i *Ingest) testLoad(name string) (*IngestTest, error) {
	sqlstr := fmt.Sprintf("SELECT testid, name, description FROM %s.test WHERE name = ?", i.schema)
	row := i.db.QueryRow(sqlstr, name)
	test := &IngestTest{}
	if err := row.Scan(&test.id, &test.name, test.description); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get location %s - %s", sqlstr, name)
	}
	return test, nil
}

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

func (i *Ingest) TransferLoad(loc *Location, bagit *IngestBagit) (*Transfer, error) {
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

func (i *Ingest) transferStore(transfer *Transfer) (*Transfer, error) {
	sqlstr := fmt.Sprintf("REPLACE INTO %s.transfer(bagitid, locationid, transfer_start, transfer_end, status, message) VALUES(?, ?, ?, ?, ?, ?)", i.schema)
	if _, err := i.db.Exec(sqlstr, transfer.bagit.id, transfer.loc.id, transfer.start, transfer.end, transfer.status, transfer.message); err != nil {
		return nil, emperror.Wrapf(err, "cannot insert transfer %s - %s -> %s", sqlstr, transfer.loc.name, transfer.bagit.name)
	}
	return transfer, nil
}

func (i *Ingest) BagitNew(name string, size int64, sha512, sha512_aes, report, creator string, creationtime time.Time) (*IngestBagit, error) {
	bagit := &IngestBagit{
		ingest:       i,
		id:           0,
		name:         name,
		size:         size,
		sha512:       sha512,
		sha512_aes:   sha512_aes,
		report:       report,
		creator:      creator,
		creationdate: creationtime,
	}
	return bagit, nil
}

func (i *Ingest) BagitLoad(name string) (*IngestBagit, error) {
	sqlstr := fmt.Sprintf("SELECT bagitid, name, filesize, sha512, sha512_aes, creator, report FROM %s.bagit WHERE name=?", i.schema)
	row := i.db.QueryRow(sqlstr, name)
	bagit := &IngestBagit{
		ingest: i,
	}
	var sha512_aes, report sql.NullString
	if err := row.Scan(&bagit.id, &bagit.name, &bagit.size, &bagit.sha512, &sha512_aes, &bagit.creator, &report); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get bagit %s - %s", sqlstr, name)
	}
	bagit.sha512_aes = sha512_aes.String
	bagit.report = report.String
	return bagit, nil
}

func (i *Ingest) bagitStore(bagit *IngestBagit) (*IngestBagit, error) {
	if bagit.id == 0 {
		var sha512_aes, report sql.NullString
		if bagit.sha512_aes != "" {
			sha512_aes.String = bagit.sha512_aes
			sha512_aes.Valid = true
		}
		if bagit.report != "" {
			report.String = bagit.report
			report.Valid = true
		}
		sqlstr := fmt.Sprintf("INSERT INTO %s.bagit(name, filesize, sha512, sha512_aes, report, creator, creationdate) VALUES(?, ?, ?, ?, ?, ?, ?)", i.schema)
		res, err := i.db.Exec(sqlstr, bagit.name, bagit.size, bagit.sha512, sha512_aes, report, bagit.creator, bagit.creationdate)
		if err != nil {
			return nil, emperror.Wrapf(err, "cannot insert bagit %s - %s", sqlstr, bagit.name)
		}
		bagit.id, err = res.LastInsertId()
		if err != nil {
			return nil, emperror.Wrapf(err, "cannot get last insert id %s", sqlstr)
		}

		return bagit, nil
	} else {
		sqlstr := fmt.Sprintf("UPDATE %s.location SET name=?, filesize=?, sha512=?, sha512_aes=?, report=?, creator=?, creationdate=? WHERE bagitid=?", i.schema)
		if _, err := i.db.Exec(sqlstr, bagit.name, bagit.size, bagit.sha512, bagit.sha512_aes, bagit.report, bagit.creator, bagit.creationdate, bagit.ingest); err != nil {
			return nil, emperror.Wrapf(err, "cannot update bagit %s", sqlstr)
		}
	}
	return nil, fmt.Errorf("BagitStore() - strange things happen - %v", bagit)
}

func (i *Ingest) bagitExistsAt(bagit *IngestBagit, location *Location) (bool, error) {
	sqlstr := fmt.Sprintf("SELECT COUNT(*) FROM bagit_location WHERE bagitid=? AND locationid=?", i.schema)
	row := i.db.QueryRow(sqlstr, bagit.id, location.id)
	var num int64
	if err := row.Scan(&num); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, emperror.Wrapf(err, "cannot check bagit %v at location %v", bagit.id, location.id)
	}
	return num > 0, nil
}

func (i *Ingest) bagitStoreAt(bagit *IngestBagit, location *Location, transfer_start, transfer_end time.Time, status, message string) error {
	sqlstr := fmt.Sprintf("INSERT INTO %s.bagit_location (bagitid, locationid, transfer_start, transfer_end, status, message) VALUES (?, ?, ?, ?, ?, ?)", i.schema)
	_, err := i.db.Exec(sqlstr, bagit.id, location.id, transfer_start, transfer_end, status, message)
	if err != nil {
		return emperror.Wrapf(err, "cannot insert bagit %s - %s", sqlstr, bagit.name)
	}
	return nil
}

func (i *Ingest) hasBagit(loc *Location, bagit *IngestBagit) (bool, error) {
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
