package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/je4/bagarc/common"
	"github.com/mash/go-accesslog"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

type alogger struct {
	handle *os.File
}

func (l alogger) Log(record accesslog.LogRecord) {
	//log.Println(record.Host+" ["+(time.Now().Format(time.RFC3339))+"] \""+record.Method+" "+record.Uri+" "+record.Protocol+"\" "+strconv.Itoa(record.Status)+" "+strconv.FormatInt(record.Size, 10))
	if _, err := fmt.Fprintf(l.handle, "%s [%s] \"%s %s %s\" %d %d\n", record.Host, time.Now().Format(time.RFC3339), record.Method, record.Uri, record.Protocol, record.Status, record.Size); err != nil {

	}
}

func main() {
	var configfile = flag.String("cfg", "./bagarc.toml", "configuration file")
	var basedir = flag.String("basedir", "", "root directory for file view")

	flag.Parse()

	var conf = &Config{
		Logfile:   "",
		Loglevel:  "DEBUG",
		Logformat: `%{time:2006-01-02T15:04:05.000} %{module}::%{shortfunc} > %{level:.5s} - %{message}`,
		Checksum:  []string{"md5", "sha512"},
		Tempdir:   os.TempDir(),
		DBFolder:  filepath.Join(os.TempDir(), "bagarc"),
	}
	if err := LoadConfig(*configfile, conf); err != nil {
		log.Printf("cannot loadMap config file: %v", err)
	}

	if *basedir != "" {
		conf.BaseDir = *basedir
	}

	pr, pw := io.Pipe()
	defer pw.Close()

	logger, lf := common.CreateLogger("bagit", conf.Logfile, pw, conf.Loglevel, conf.Logformat)
	defer lf.Close()

	wsHandler := NewWSHandler(logger)
	go func() {
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			wsHandler.send("console", scanner.Text())
		}
	}()

	bconfig := badger.DefaultOptions(filepath.Clean(conf.DBFolder))

	// value log truncate required to run db. this might result in data loss #744
	// https://github.com/dgraph-io/badger/issues/744
	if runtime.GOOS == "windows" {
		bconfig = bconfig.WithTruncate(true)
	}

	//	bconfig.Logger = logger // use our logger...
	db, err := badger.Open(bconfig)
	if err != nil {
		logger.Fatalf("cannot open badger database: %v", err)
	}
	defer db.Close()

	dirmap := map[string]string{}

	router := mux.NewRouter()

	router.HandleFunc("/socket/echo", wsEcho())
	router.HandleFunc("/socket/{group}", wsHandler.wsGroup())

	fsSource := NewFilesystem("dirmap.source", true, "source", dirmap, db)
	for name, folder := range conf.Source {
		fsSource.addDir(name, folder)
	}
	router.HandleFunc("/sourcemap", fsSource.createGetDirmapHandler()).Methods("GET")
	router.HandleFunc("/sourcemap/{name}", fsSource.createGetDirmapHandler()).Methods("GET")
	router.HandleFunc("/sourcemap/{name}", fsSource.addDirmapHandler()).Methods("POST")
	router.HandleFunc("/sourcemap/{name}", fsSource.removeDirmapHandler()).Methods("DELETE")
	router.PathPrefix("/source").
		Handler(http.StripPrefix("/source", fsSource.createSubfolderHandler())).
		Methods("GET")

	destDirmap := map[string]string{}
	fsdest := NewFilesystem("dirmap.dest", false, "dest", destDirmap, db)
	for name, folder := range conf.Destination {
		fsdest.addDir(name, folder)
	}
	router.HandleFunc("/destmap", fsdest.createGetDirmapHandler()).Methods("GET")
	router.HandleFunc("/destmap/{name}", fsdest.createGetDirmapHandler()).Methods("GET")
	router.HandleFunc("/destmap/{name}", fsdest.addDirmapHandler()).Methods("POST")
	router.HandleFunc("/destmap/{name}", fsdest.removeDirmapHandler()).Methods("DELETE")
	router.PathPrefix("/dest").
		Handler(http.StripPrefix("/dest", fsdest.createSubfolderHandler())).
		Methods("GET")

	var f *os.File
	if conf.AccessLog == "" {
		f = os.Stderr
	} else {
		f, err = os.OpenFile(conf.AccessLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
	}
	defer f.Close()
	l := alogger{handle: f}
	headersOk := handlers.AllowedHeaders([]string{"Origin", "X-Requested-With", "Content-Type", "Accept", "Access-Control-Request-Method", "Authorization"})
	originsOk := handlers.AllowedOrigins([]string{"*"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS", "DELETE"})
	credentialsOk := handlers.AllowCredentials()
	//ignoreOptions := handlers.IgnoreOptions()

	// todo: correct cors handling!!!
	server := &http.Server{
		Addr: conf.Listen,
		Handler: accesslog.NewLoggingHandler(handlers.CORS(
			originsOk,
			headersOk,
			methodsOk,
			credentialsOk,
			//			ignoreOptions,
		)(router), l),
	}

	go func() {
		sigint := make(chan os.Signal, 1)

		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt)
		//		signal.Notify(sigint, syscall.SIGINT)
		signal.Notify(sigint, syscall.SIGTERM)

		<-sigint

		// We received an interrupt signal, shut down.
		logger.Infof("shutdown requested")
		if err = server.Shutdown(context.Background()); err != nil {
			logger.Errorf("error shutting down server: %v", err)
		}
	}()

	if conf.TLS {
		logger.Infof("listening on https://%s", conf.Listen)
		logger.Fatal(server.ListenAndServeTLS(conf.CertChain, conf.PrivateKey))
	} else {
		logger.Infof("listening on http://%s", conf.Listen)
		logger.Fatal(server.ListenAndServe())
	}
}
