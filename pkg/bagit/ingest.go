package bagit

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/goph/emperror"
	"github.com/op/go-logging"
	"io/fs"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const encExt = "aes256"

type Ingest struct {
	tempDir     string
	logger      *logging.Logger
	keyDir      string
	db          *sql.DB
	schema      string
	initLocName string
	initLoc     *IngestLocation
	tests       map[string]*IngestTest
	locations   map[string]*IngestLocation
	sftp        *SFTP
}

func NewIngest(tempDir, keyDir, initLocName string, db *sql.DB, dbschema string, privateKeys []string, logger *logging.Logger) (*Ingest, error) {
	sftp, err := NewSFTP(privateKeys, "", "", logger)
	if err != nil {
		return nil, emperror.Wrap(err, "cannot create sftp")
	}
	i := &Ingest{
		db:          db,
		tempDir:     tempDir,
		logger:      logger,
		keyDir:      keyDir,
		schema:      dbschema,
		initLocName: initLocName,
		sftp:        sftp,
	}
	return i, i.Init()
}

func (i *Ingest) Init() error {
	var err error
	i.locations, err = i.locationLoadAll()
	if err != nil {
		return emperror.Wrapf(err, "cannot load locations")
	}

	i.tests, err = i.TestLoadAll()
	if err != nil {
		return emperror.Wrapf(err, "cannot load locations")
	}

	var ok bool
	i.initLoc, ok = i.locations[i.initLocName]
	if !ok {
		return fmt.Errorf("cannot get init location %s", i.initLoc)
	}
	return nil
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

/* Database functions */

func (i *Ingest) BagitLoadAll(fn func(bagit *IngestBagit) error) error {
	sqlstr := fmt.Sprintf("SELECT bagitid, name, filesize, sha512, sha512_aes, creator, report FROM %s.bagit", i.schema)

	rows, err := i.db.Query(sqlstr)
	if err != nil {
		return emperror.Wrapf(err, "cannot get locations %s", sqlstr)
	}
	defer rows.Close()
	for rows.Next() {
		bagit := &IngestBagit{
			ingest: i,
		}
		var sha512_aes, report sql.NullString
		if err := rows.Scan(&bagit.id, &bagit.name, &bagit.size, &bagit.sha512, &sha512_aes, &bagit.creator, &report); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return emperror.Wrapf(err, "cannot get bagit %s", sqlstr)
		}
		bagit.sha512_aes = sha512_aes.String
		bagit.report = report.String
		if err := fn(bagit); err != nil {
			return err
		}
	}
	return nil
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

func (i *Ingest) locationLoadAll() (map[string]*IngestLocation, error) {
	sqlstr := fmt.Sprintf("SELECT locationid, name, path, params, encrypted, quality, costs FROM %s.location", i.schema)

	var locations = make(map[string]*IngestLocation)

	rows, err := i.db.Query(sqlstr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get locations %s", sqlstr)
	}
	defer rows.Close()
	for rows.Next() {
		loc := &IngestLocation{}
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

func (i *Ingest) locationLoad(name string) (*IngestLocation, error) {
	sqlstr := fmt.Sprintf("SELECT locationid, name, path, params, encrypted, quality, costs FROM %s.location WHERE name = ?", i.schema)
	row := i.db.QueryRow(sqlstr, name)
	loc := &IngestLocation{ingest: i}
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

func (i *Ingest) locationStore(loc *IngestLocation) (*IngestLocation, error) {
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

func (i *Ingest) transferLoad(loc *IngestLocation, bagit *IngestBagit) (*Transfer, error) {
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

func (i *Ingest) bagitExistsAt(bagit *IngestBagit, location *IngestLocation) (bool, error) {
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

func (i *Ingest) hasBagit(loc *IngestLocation, bagit *IngestBagit) (bool, error) {
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

func (i *Ingest) bagitLocationStore(ibl *IngestBagitLocation) error {
	sqlstr := fmt.Sprintf("INSERT INTO %s.bagit_location (bagitid, locationid, transfer_start, transfer_end, status, message) VALUES (?, ?, ?, ?, ?, ?)", i.schema)
	_, err := i.db.Exec(sqlstr, ibl.bagit.id, ibl.location.id, ibl.start, ibl.end, ibl.status, ibl.message)
	if err != nil {
		return emperror.Wrapf(err, "cannot insert bagit %s at %s - %s", sqlstr, ibl.bagit.name, ibl.location.name)
	}
	return nil
}

func (i *Ingest) bagitLoad(name string) (*IngestBagit, error) {
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

func (i *Ingest) bagitAddContent(bagit *IngestBagit, zippath string, diskpath string, filesize int64, sha256 string, sha512 string, md5 string) error {
	sqlstr := fmt.Sprintf("INSERT INTO %s.content (bagitid, zippath, diskpath, filesize, sha256, sha512, md5) VALUES(?, ?, ?, ?, ?, ?, ?)", i.schema)
	_, err := i.db.Exec(sqlstr, bagit.id, zippath, diskpath, filesize, sha256, sha512, md5)
	if err != nil {
		return emperror.Wrapf(err, "cannot insert content of bagit %s at %s - %s", bagit.name, sqlstr)
	}
	return nil

}

func (i *Ingest) bagitLocationLoad(bagit *IngestBagit, location *IngestLocation) (*IngestBagitLocation, error) {
	sqlstr := fmt.Sprintf("SELECT transfer_start, transfer_end, status, message FROM %s.bagit_location WHERE bagitid=? AND locationid=?", i.schema)
	row := i.db.QueryRow(sqlstr, bagit.id, location.id)
	ibl, err := NewIngestBagitLocation(i, bagit, location)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot create ingestbagitlocation")
	}

	if err := row.Scan(&ibl.start, &ibl.end, &ibl.status, &ibl.message); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, emperror.Wrapf(err, "cannot get bagit %s - %s - %s", sqlstr, bagit.name, location.name)
	}
	return ibl, nil
}

/* Creator functions */

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

func (i *Ingest) IngestBagitLocationNew(bagit *IngestBagit, loc *IngestLocation) (*IngestBagitLocation, error) {
	return NewIngestBagitLocation(i, bagit, loc)
}

/* Actions */

func (i *Ingest) Transfer() error {
	if err := i.BagitLoadAll(func(bagit *IngestBagit) error {
		source, err := i.bagitLocationLoad(bagit, i.initLoc)
		if err != nil {
			return emperror.Wrapf(err, "cannot load initial transfer of %s - %s", bagit.name, i.initLoc.name)
		}

		for key, loc := range i.locations {
			if key == i.initLocName {
				continue
			}
			target, err := i.IngestBagitLocationNew(bagit, loc)
			if err != nil {
				return emperror.Wrapf(err, "cannot create transfer object")
			}
			if exists, err := target.Exists(); exists {
				return err
			}
			if err := target.Transfer(source); err != nil {
				return emperror.Wrapf(err, "cannot transfer bagit %s to %s", bagit.name, target.location.name)
			}
		}
		return nil
	}); err != nil {
		return emperror.Wrap(err, "error iterating bagits")
	}
	return nil
}

func (i *Ingest) Ingest() error {
	// get path of init location
	fp := strings.Trim(i.initLoc.GetPath().Path, "/") + "/"
	if runtime.GOOS == "windows" {
		fp = strings.Replace(fp, "|", ":", -1)
	} else {
		fp = "/" + fp
	}
	// walk through the path
	if err := filepath.Walk(fp, func(path string, info fs.FileInfo, err error) error {
		// ignore directory
		if info.IsDir() {
			return nil
		}
		name := info.Name()
		// hope: there's no empty filename
		// ignore . files
		if name[0] == '.' {
			return nil
		}

		// ignore any non-zip file
		if !strings.HasSuffix(name, ".zip") {
			return nil
		}

		// if it's already in database ignore file
		ib, err := i.bagitLoad(name)
		if err != nil {
			return emperror.Wrapf(err, "cannot load bagit %s", name)
		}
		if ib != nil {
			i.logger.Infof("bagit %s already ingested", name)
			return nil
		}

		bagitPath := path

		// create tempdir for database
		tmpdir, err := ioutil.TempDir(i.tempDir, filepath.Base(bagitPath))
		if err != nil {
			return emperror.Wrapf(err, "cannot create temporary folder in %s", i.tempDir)
		}

		// initialize badger database
		bconfig := badger.DefaultOptions(filepath.Join(tmpdir, "/badger"))
		bconfig.Logger = i.logger // use our logger...
		checkDB, err := badger.Open(bconfig)
		if err != nil {
			return emperror.Wrapf(err, "cannot open badger database")
		}
		// close database and delete temporary files
		defer func() {
			checkDB.Close()
			if err := os.RemoveAll(tmpdir); err != nil {
				i.logger.Errorf("cannot remove %s: %v", tmpdir, err)
			}
		}()

		// check bagit file
		checker, err := NewBagit(bagitPath, tmpdir, checkDB, i.logger)
		i.logger.Infof("deep checking bagit file %s", bagitPath)

		var metaBytes bytes.Buffer
		metaWriter := bufio.NewWriter(&metaBytes)
		if err := checker.Check(metaWriter); err != nil {
			return emperror.Wrapf(err, "error checking file %v", bagitPath)
		}
		// paranoia
		metaWriter.Flush()

		type Metadata []struct {
			Path     string            `json:"path"`
			Zippath  string            `json:"zippath"`
			Checksum map[string]string `json:"checksum"`
			Size     int64             `json:"size"`
		}
		var metadata Metadata

		if err := json.Unmarshal(metaBytes.Bytes(), &metadata); err != nil {
			return emperror.Wrapf(err, "cannot unmarshal json %s", metaBytes.String())
		}

		// create checksum
		cs, err := SHA512(bagitPath)
		if err != nil {
			return emperror.Wrap(err, "cannot calculate checksum")
		}

		// create bagit ingest object
		bagit, err := i.BagitNew(name, info.Size(), cs, "", "", "bagarc", time.Now())
		if err != nil {
			return emperror.Wrapf(err, "cannot create bagit entity %s", name)
		}
		// store it in database
		if err := bagit.Store(); err != nil {
			return emperror.Wrapf(err, "cannot store %s", name)
		}

		ibl, err := i.IngestBagitLocationNew(bagit, i.initLoc)
		if err != nil {
			return emperror.Wrapf(err, "cannot create initial ingestbagitlocation")
		}
		if err := ibl.SetData("ok", "initial ingest location", time.Now().Local(), time.Now().Local()); err != nil {
			return emperror.Wrapf(err, "cannot set data for ingestbagitlocation")
		}
		if err := ibl.Store(); err != nil {
			return emperror.Wrapf(err, "cannot store initial bagit location")
		}

		for _, meta := range metadata {
			var sha256, sha512, md5 string
			if str, ok := meta.Checksum["sha256"]; ok {
				sha256 = str
			}
			if str, ok := meta.Checksum["sha512"]; ok {
				sha512 = str
			}
			if str, ok := meta.Checksum["md5"]; ok {
				md5 = str
			}
			bagit.AddContent(meta.Zippath, meta.Path, meta.Size, sha256, sha512, md5)
		}

		/*
			if err := bi.Encrypt(name, bagitPath); err != nil {
				return emperror.Wrapf(err, "cannot encrypt %s", bagitPath)
			}
		*/
		return nil
	}); err != nil {
		return emperror.Wrapf(err, "error walking %s", fp)
	}
	return nil
}

func (i *Ingest) Encrypt(name, bagitPath string) error {
	if _, err := os.Stat(bagitPath + "." + encExt); err == nil {
		return fmt.Errorf("encrypted bagit file %s.%s already exists", name, encExt)
	} else if !os.IsNotExist(err) {
		return emperror.Wrapf(err, "error checking existence of %s.%s", name, encExt)
	}

	// create checksum of bagit
	//		bi.logger.Infof("calculating checksum of %s", bagitFile)
	//		checksum := sha512.New()

	fin, err := os.Open(bagitPath)
	if err != nil {
		return emperror.Wrapf(err, "cannot open %s", bagitPath)
	}
	defer fin.Close()
	fout, err := os.OpenFile(bagitPath+"."+encExt, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return emperror.Wrapf(err, "cannot create encrypted bagit %s.%s", bagitPath, encExt)
	}
	defer fout.Close()
	if err != nil {
		return emperror.Wrapf(err, "cannot open %s.%s", bagitPath, encExt)
	}
	defer fout.Close()

	key, iv, hashBytes, err := Encrypt(fin, fout)
	if err != nil {
		return emperror.Wrapf(err, "cannot encrypt %s", bagitPath)
	}

	os.WriteFile(fmt.Sprintf("%s/%s.%s.key", i.keyDir, name, encExt), []byte(fmt.Sprintf("%x", key)), 0600)
	os.WriteFile(fmt.Sprintf("%s/%s.%s.iv", i.keyDir, name, encExt), []byte(fmt.Sprintf("%x", iv)), 0600)

	i.logger.Infof("key: %x, iv: %x, hash: %x", key, iv, hashBytes)
	i.logger.Infof("decrypt using openssl: \n openssl enc -aes-256-ctr -nosalt -d -in %s.%s -out %s -K '%x' -iv '%x'", bagitPath, encExt, bagitPath, key, iv)

	/*
		if _, err := io.Copy(checksum, fin); err != nil {
			return emperror.Wrapf(err, "cannot calculate checksum of %s", bagitFile)
		}
	*/
	return nil
}
