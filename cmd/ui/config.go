package main

import (
	"github.com/BurntSushi/toml"
	"github.com/goph/emperror"
)

// main config structure for toml file
type Config struct {
	CertChain    string            `toml:"*certchain"`
	PrivateKey   string            `toml:"privatekey"`
	Listen       string            `toml:"listen"`
	TLS          bool              `toml:"tls"`
	AccessLog    string            `toml:"accesslog"`
	Logfile      string            `toml:"logfile"`
	Loglevel     string            `toml:"loglevel"`
	Logformat    string            `toml:"logformat"`
	Tempdir      string            `toml:"tempdir"`
	Checksum     []string          `toml:"checksum"`
	Siegfried    string            `toml:"siegfried"`
	FixFilenames bool              `toml:"fixfilenames"`
	StoreOnly    []string          `toml:"nocompress"`
	Cleanup      bool              `toml:"cleanup"`
	DBFolder     string            `toml:"dbfolder"`
	BaseDir      string            `toml:"basedir"`
	Source       map[string]string `toml:"source"`
	Destination  map[string]string `toml:"destination"`
}

func LoadConfig(filepath string, conf *Config) error {
	_, err := toml.DecodeFile(filepath, conf)
	if err != nil {
		return emperror.Wrapf(err, "error loading config file %v", filepath)
	}
	return nil
}
