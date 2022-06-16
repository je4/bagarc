package main

import (
	"fmt"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/v2/pkg/ocfl"
	"github.com/je4/bagarc/v2/pkg/zipfs"
	lm "github.com/je4/utils/v2/pkg/logger"
	flag "github.com/spf13/pflag"
	"log"
	"os"
	"path/filepath"
	"time"
)

const LOGFORMAT = `%{time:2006-01-02T15:04:05.000} %{module}::%{shortfunc} [%{shortfile}] > %{level:.5s} - %{message}`

func main() {
	var err error

	var zipfile = flag.String("file", "", "ocfl zip filename")
	var logfile = flag.String("logfiel", "", "name of logfile")
	var loglevel = flag.String("loglevel", "DEBUG", "CRITICAL|ERROR|WARNING|NOTICE|INFO|DEBUG")

	flag.Parse()

	logger, lf := lm.CreateLogger("ocfl", *logfile, nil, *loglevel, LOGFORMAT)
	defer lf.Close()

	var zipSize int64
	var zipReader *os.File
	var zipWriter *os.File

	tempFile := fmt.Sprintf("%s.tmp", *zipfile)
	if zipWriter, err = os.Create(tempFile); err != nil {
		err = emperror.ExposeStackTrace(emperror.Wrapf(err, "cannot create zip file %s", tempFile))
		stack, ok := emperror.StackTrace(err)
		if ok {
			log.Print(stack)
		}
		panic(err)
	}
	defer func() {
		zipWriter.Close()
		if err := os.Rename(fmt.Sprintf("%s.tmp", *zipfile), *zipfile); err != nil {
			log.Print(err)
		}
	}()

	stat, err := os.Stat(*zipfile)
	if err != nil {
		log.Print(emperror.Wrapf(err, "%s does not exist. creating new file", *zipfile))
	} else {
		zipSize = stat.Size()
		if zipReader, err = os.Open(*zipfile); err != nil {
			err = emperror.ExposeStackTrace(emperror.Wrapf(err, "cannot open zip file %s", *zipfile))
			stack, ok := emperror.StackTrace(err)
			if ok {
				log.Print(stack)
			}
			panic(err)
		}
		defer func() {
			zipReader.Close()
			if err := os.Rename(*zipfile, fmt.Sprintf("%s.%s", *zipfile, time.Now().Format("20060201_150405"))); err != nil {
				panic(err)
			}
		}()

	}

	zfs, err := zipfs.NewFSIO(zipReader, zipSize, zipWriter, logger)
	if err != nil {
		err = emperror.ExposeStackTrace(emperror.Wrap(err, "cannot create zipfs"))
		stack, ok := emperror.StackTrace(err)
		if ok {
			log.Print(stack)
		}
		panic(err)
	}
	defer zfs.Close()
	o, err := ocfl.NewOCFL(zfs, filepath.Base(*zipfile), logger)
	if err != nil {
		err = emperror.ExposeStackTrace(emperror.Wrap(err, "cannot create zipfs"))
		stack, ok := emperror.StackTrace(err)
		if ok {
			log.Print(stack)
		}
		panic(err)
	}
	defer o.Close()

	testfile := "c:/temp/Updates_Artists_20220606.csv"
	file, err := os.Open(testfile)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if err := o.StartUpdate("test 42", "Jürgen Enge", "juergen.enge@unibas.ch"); err != nil {
		err = emperror.ExposeStackTrace(emperror.Wrap(err, "cannot add file"))
		stack, ok := emperror.StackTrace(err)
		if ok {
			log.Print(stack)
		}
		panic(err)
	}
	checksum, err := ocfl.Checksum(file, ocfl.DigestSHA512)
	if err != nil {
		err = emperror.ExposeStackTrace(emperror.Wrap(err, "cannot add file"))
		stack, ok := emperror.StackTrace(err)
		if ok {
			log.Print(stack)
		}
		panic(err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		panic(err)
	}
	if err := o.AddFile("Update's_Artists?_202206#06.csv", file, checksum); err != nil {
		err = emperror.ExposeStackTrace(emperror.Wrap(err, "cannot add file"))
		stack, ok := emperror.StackTrace(err)
		if ok {
			log.Print(stack)
		}
		panic(err)
	}

}
