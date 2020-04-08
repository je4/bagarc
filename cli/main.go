package main

import (
	"encoding/json"
	"github.com/BurntSushi/toml"
	"github.com/dgraph-io/badger"
	_ "github.com/dgraph-io/badger"
	"github.com/je4/bagarc/bagit"
	"github.com/je4/bagarc/common"
	flag "github.com/spf13/pflag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func main() {
	var action = flag.String("action", "bagit", "bagit|bagitcheck")
	var sourcedir = flag.String("sourcedir", ".", "source folder with archive content")
	var bagitfile = flag.String("bagit", "bagarc.zip", "target filename (bagit zip)")
	var configfile = flag.String("cfg", "/etc/bagit.toml", "configuration file")
	var tempdir = flag.String("temp", "/tmp", "folder for temporary files")
	var checksum = flag.StringArray("checksum", []string{"md5", "sha512"}, "checksum algorithms to use (md5|sha256|sha512)")
	var siegfried = flag.String("sf", "", "url for siegfried [[PATH]] is placeholder for local file reference")
	var fixFilenames = flag.Bool("fixfilenames", true, "set this flag, if filenames should be corrected")
	var bagInfoFile = flag.String("baginfo", "", "json file with bag-info entries (only string, no hierarchy)")
	var cleanup = flag.Bool("cleanup", false, "remove temporary files after bagit creation")
	var restoreFilenames = flag.Bool("restorefilenames", true, "rename strange characters back while extracting")
	var outputFolder  = flag.String("output", ".", "folder in which output structure has to be copied")

	flag.Parse()

	var conf = &common.BagitConfig{
		Logfile:   "",
		Loglevel:  "DEBUG",
		Logformat: `%{time:2006-01-02T15:04:05.000} %{module}::%{shortfunc} > %{level:.5s} - %{message}`,
		Checksum:  []string{"md5", "sha512"},
		Tempdir:   "/tmp",
	}
	if err := common.LoadBagitConfig(*configfile, conf); err != nil {
		log.Printf("cannot load config file: %v", err)
	}

	// set all config values, which could be orverridden by flags
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "temp":
			conf.Tempdir = *tempdir
		case "checksum":
			conf.Checksum = *checksum
		case "siegfried":
			conf.Siegfried = *siegfried
		case "fixfilenames":
			conf.FixFilenames = *fixFilenames
		case "cleanup":
			conf.Cleanup = *cleanup
		}
	})

	logger, lf := common.CreateLogger("bagit", conf.Logfile, nil, conf.Loglevel, conf.Logformat)
	defer lf.Close()

	switch *action {
	case "check":
		tmpdir, err := ioutil.TempDir(conf.Tempdir, filepath.Base(*bagitfile))
		if err != nil {
			logger.Fatalf("cannot create temporary folder in %s", conf.Tempdir)
		}
		bconfig := badger.DefaultOptions(filepath.Join(tmpdir, "/badger"))
		bconfig.Logger = logger // use our logger...
		db, err := badger.Open(bconfig)
		if err != nil {
			logger.Fatalf("cannot open badger database: %v", err)
		}
		defer func() {
			db.Close()
			if err := os.RemoveAll(tmpdir); err != nil {
				logger.Errorf("cannot remove %s: %v", tmpdir, err)
			}
		}()

		checker, err := bagit.NewBagit(*bagitfile, tmpdir, db, logger)
		if err := checker.Check(); err != nil {
			logger.Fatalf("error checking file: %v", err)
		}
	case "extract":
		tmpdir, err := ioutil.TempDir(conf.Tempdir, filepath.Base(*bagitfile))
		if err != nil {
			logger.Fatalf("cannot create temporary folder in %s", conf.Tempdir)
		}
		bconfig := badger.DefaultOptions(filepath.Join(tmpdir, "/badger"))
		bconfig.Logger = logger // use our logger...
		db, err := badger.Open(bconfig)
		if err != nil {
			logger.Fatalf("cannot open badger database: %v", err)
		}
		defer func() {
			db.Close()
			if err := os.RemoveAll(tmpdir); err != nil {
				logger.Errorf("cannot remove %s: %v", tmpdir, err)
			}
		}()

		checker, err := bagit.NewBagit(*bagitfile, tmpdir, db, logger)
		if err := checker.Extract(*outputFolder, *restoreFilenames); err != nil {
			logger.Fatalf("error extracting file: %v", err)
		}
	case "bagit":
		// clean up all files
		tmpdir := *bagitfile + ".tmp"
		os.Remove(*bagitfile)
		os.RemoveAll(tmpdir)
		os.Mkdir(tmpdir, os.ModePerm)

		bconfig := badger.DefaultOptions(filepath.Join(tmpdir, "/badger"))
		bconfig.Logger = logger // use our logger...
		db, err := badger.Open(bconfig)
		if err != nil {
			logger.Fatalf("cannot open badger database: %v", err)
		}
		defer func() {
			db.Close()
			if conf.Cleanup {
				if err := os.RemoveAll(tmpdir); err != nil {
					logger.Errorf("cannot remove %s: %v", tmpdir, err)
				}
			}
		}()

		bagInfo := map[string]string{}
		if *bagInfoFile != "" {
			data, err := ioutil.ReadFile(*bagInfoFile)
			if err != nil {
				logger.Fatalf("cannot read bag info file %s", *bagInfoFile)
			}
			if err := json.Unmarshal(data, &bagInfo); err != nil {
				_, err2 := toml.DecodeFile(*bagInfoFile, &bagInfo)
				if err2 != nil {
					logger.Fatalf("cannot unmarshal or read bag info file %s: %v // %v", *bagInfoFile, err, err2)
				}
			}
		}

		creator, err := bagit.NewBagitCreator(*sourcedir, *bagitfile, conf.Checksum, bagInfo, db, conf.FixFilenames, conf.StoreOnly, conf.Siegfried, tmpdir, logger)
		if err != nil {
			log.Fatalf("cannot create BagitCreator: %v", err)
			return
		}
		if err := creator.Run(); err != nil {
			log.Fatalf("cannot create Bagit: %v", err)
		}
	default:
	}

}
