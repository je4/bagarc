package main

import (
	"github.com/dgraph-io/badger"
	_ "github.com/dgraph-io/badger"
	"github.com/je4/bagarc/bagit"
	"github.com/je4/bagarc/common"
	flag "github.com/spf13/pflag"
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
	var siegfried = flag.String( "sf", "", "url for siegfried [[PATH]] is placeholder for local file reference")
	var fixFilenames = flag.Bool("fixfilenames", true, "set this flag, if filenames should be corrected")
	flag.Parse()

	var conf = &BagitConfig{
		Logfile:   "",
		Loglevel:  "DEBUG",
		Logformat: `%{time:2006-01-02T15:04:05.000} %{module}::%{shortfunc} > %{level:.5s} - %{message}`,
		Checksum:  []string{"md5", "sha512"},
		Tempdir:   "/tmp",
	}
	if err := LoadBagitConfig(*configfile, conf); err != nil {
		log.Printf("cannot load config file: %v", err)
	}

	// set all config values, which could be orverridden by flags
	flag.Visit(func( f *flag.Flag){
		switch f.Name {
		case "temp":
			conf.Tempdir = *tempdir
		case "checksum":
			conf.Checksum = *checksum
		case "siegfried":
			conf.Siegfried = *siegfried
		case "fixfilenames":
			conf.FixFilenames = *fixFilenames
		}
	})

	logger, lf := common.CreateLogger("bagit", conf.Logfile, conf.Loglevel, conf.Logformat)
	defer lf.Close()


	switch *action {
	case "bagit":
		// clean up all files
		tmpdir := *bagitfile + ".tmp"
		os.Remove(*bagitfile)
		os.RemoveAll(tmpdir)
		os.Mkdir(tmpdir, os.ModePerm)

		// Open the Badger database located in the /tmp/badger directory.
		// It will be created if it doesn't exist.

		bconfig := badger.DefaultOptions(filepath.Join(tmpdir, "/badger"))
		bconfig.Logger = logger // use our logger...
		db, err := badger.Open(bconfig)
		if err != nil {
			logger.Fatalf("cannot open badger database: %v", err)
		}
		defer db.Close()

		creator, err := bagit.NewBagitCreator(*sourcedir, *bagitfile, conf.Checksum, db, conf.FixFilenames, conf.Siegfried, tmpdir, logger)
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
