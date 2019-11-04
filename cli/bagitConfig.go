package main

import (
	"github.com/BurntSushi/toml"
	"github.com/goph/emperror"
)

// main config structure for toml file
type BagitConfig struct {
	Logfile      string   `toml:"logfile"`
	Loglevel     string   `toml:"loglevel"`
	Logformat    string   `toml:"logformat"`
	Checksum     []string `toml:"checksum"`
	Tempdir      string   `toml:"tempdir"`
	Siegfried    string   `toml:"siegfried"`
	FixFilenames bool     `toml:"fixfilenames"`
}

func LoadBagitConfig(filepath string, conf *BagitConfig) error {
	_, err := toml.DecodeFile(filepath, conf)
	if err != nil {
		return emperror.Wrapf(err, "error loading config file %v", filepath)
	}
	return nil
}
