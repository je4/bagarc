package common

import (
	"github.com/BurntSushi/toml"
	"github.com/goph/emperror"
)

type Cfg_database struct {
	ServerType string
	DSN        string
	ConnMax    int `toml:"connection_max"`
	Schema     string
}

// main config structure for toml file
type BagitConfig struct {
	CertChain    string   `toml:"*certchain"`
	PrivateKey   string   `toml:"privatekey"`
	Listen       string   `toml:"listen"`
	TLS          bool     `toml:"tls"`
	AccessLog    string   `toml:"accesslog"`
	Logfile      string   `toml:"logfile"`
	Loglevel     string   `toml:"loglevel"`
	Logformat    string   `toml:"logformat"`
	Checksum     []string `toml:"checksum"`
	Tempdir      string   `toml:"tempdir"`
	Indexer      string   `toml:"indexer"`
	FixFilenames bool     `toml:"fixfilenames"`
	StoreOnly    []string `toml:"nocompress"`
	Cleanup      bool     `toml:"cleanup"`
	DBFolder     string   `toml:"dbfolder"`
	BaseDir      string   `toml:"basedir"`
}

func LoadBagitConfig(filepath string, conf *BagitConfig) error {
	_, err := toml.DecodeFile(filepath, conf)
	if err != nil {
		return emperror.Wrapf(err, "error loading config file %v", filepath)
	}
	return nil
}
