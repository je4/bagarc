package ingest

import (
	"database/sql"
	"fmt"
	"github.com/goph/emperror"
	"time"
)

//
// Bagit
//

func (i *Ingest) BagitNew(name string, size int64, sha512, sha512_aes, report, creator string, creationtime time.Time) (*Bagit, error) {
	bagit := &Bagit{
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

func (i *Ingest) BagitLoad(name string) (*Bagit, error) {
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

func (i *Ingest) bagitStore(bagit *Bagit) (*Bagit, error) {
	if bagit.id == 0 {
		sqlstr := fmt.Sprintf("INSERT INTO %s.bagit(name, filesize, sha512, sha512_aes, report, creator, creationdate) VALUES(?, ?, ?, ?, ?, ?, ?) returning bagitid", i.schema)
		row := i.db.QueryRow(sqlstr, bagit.name, bagit.size, bagit.sha512, bagit.sha512_aes, bagit.report, bagit.creator, bagit.creationdate)
		if err := row.Scan(&bagit.id); err != nil {
			return nil, emperror.Wrapf(err, "cannot insert bagit %s - %s", sqlstr, bagit.name)
		}
		return bagit, nil
	} else {
		sqlstr := fmt.Sprintf("UPDATE %s.location SET name=?, filesize=?, sha512=?, sha512_aes=?, report=?, creator=?, creationdate=? WHERE bagitid=?", i.schema)
		if _, err := i.db.Exec(sqlstr, bagit.name, bagit.size, bagit.sha512, bagit.sha512_aes, bagit.report, bagit.creator, bagit.creationdate, bagit.ingest); err != nil {
			return nil, emperror.Wrapf(err, "cannot update bagit %s - %v", sqlstr, bagit.id)
		}
	}
	return nil, fmt.Errorf("BagitStore() - strange things happen - %v", bagit)
}

func (i *Ingest) hasBagit(loc *Location, bagit *Bagit) (bool, error) {
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
