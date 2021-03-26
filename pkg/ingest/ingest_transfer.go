package ingest

import (
	"database/sql"
	"fmt"
	"github.com/goph/emperror"
)

//
// Transfer
//

func (i *Ingest) TransferLoad(loc *Location, bagit *Bagit) (*Transfer, error) {
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
