package main

import (
	"github.com/BurntSushi/toml"
	"github.com/goph/emperror"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Endpoint struct {
	Host string
	Port int
}

func (e *Endpoint) UnmarshalText(text []byte) error {
	var err error
	var port string
	e.Host, port, err = net.SplitHostPort(string(text))
	if err == nil {
		var longPort int64
		longPort, err = strconv.ParseInt(port, 10, 64)
		if err != nil {
			return emperror.Wrapf(err, "cannot parse port %s of %s", port, string(text))
		}
		e.Port = int(longPort)
	}
	return err
}

type Forward struct {
	Local  *Endpoint
	Remote *Endpoint
}

type SSHTunnel struct {
	User       string             `toml:"user"`
	PrivateKey string             `toml:"privatekey"`
	Endpoint   *Endpoint          `toml:"endpoint"`
	Forward    map[string]Forward `toml:"forward"`
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type DBMySQL struct {
	DSN            string
	ConnMaxTimeout duration
	Schema         string
}

// main config structure for toml file
type BagitConfig struct {
	CertChain      string               `toml:"*certchain"`
	PrivateKey     string               `toml:"privatekey"`
	Listen         string               `toml:"listen"`
	TLS            bool                 `toml:"tls"`
	AccessLog      string               `toml:"accesslog"`
	Logfile        string               `toml:"logfile"`
	Loglevel       string               `toml:"loglevel"`
	Logformat      string               `toml:"logformat"`
	Checksum       []string             `toml:"checksum"`
	Tempdir        string               `toml:"tempdir"`
	KeyDir         string               `toml:"keydir"`
	Indexer        string               `toml:"indexer"`
	FixFilenames   bool                 `toml:"fixfilenames"`
	StoreOnly      []string             `toml:"nocompress"`
	Cleanup        bool                 `toml:"cleanup"`
	DBFolder       string               `toml:"dbfolder"`
	BaseDir        string               `toml:"basedir"`
	Tunnel         map[string]SSHTunnel `toml:"tunnel"`
	DB             DBMySQL              `toml:"db"`
	IngestLocation string               `toml:"ingestloc"`
}

func LoadBagitConfig(fp string, conf *BagitConfig) error {
	_, err := toml.DecodeFile(fp, conf)
	if err != nil {
		return emperror.Wrapf(err, "error loading config file %v", fp)
	}
	conf.BaseDir = strings.TrimRight(filepath.ToSlash(conf.BaseDir), "/")
	return nil
}
