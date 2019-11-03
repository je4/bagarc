package common

import (
	"github.com/op/go-logging"
	"os"
)

type NullWriter struct{}

func (w *NullWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func CreateLogger(module string, logfile string, loglevel string, logformat string) (log *logging.Logger, lf *os.File) {
	log = logging.MustGetLogger(module)
	var err error
	if logfile != "" {
		lf, err = os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Errorf("Cannot open logfile %v: %v", logfile, err)
		}
		//defer lf.Close()

	} else {
		lf = os.Stderr
	}
	backend := logging.NewLogBackend(lf, "", 0)
	backendLeveled := logging.AddModuleLevel(backend)
	backendLeveled.SetLevel(logging.GetLevel(loglevel), "")

	logging.SetFormatter(logging.MustStringFormatter(logformat))
	logging.SetBackend(backendLeveled)

	return
}
