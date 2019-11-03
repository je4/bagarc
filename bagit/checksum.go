package bagit

import (
	"io"
	"sync"
)

type Checksum struct {
	sync.Mutex
	checksums []string
	cs map[string]string
	errors []error
	rws map[string]rwStruct
}

func (c *Checksum) setResult( csType, checksum string) {
	c.Lock()
	defer c.Unlock()

	c.cs[csType] = checksum
}

func (c *Checksum) setError( err error) {
	c.Lock()
	defer c.Unlock()

	c.errors = append(c.errors, err)
}

func (c *Checksum) startThreads( done chan bool) error {
	// create the map of all Checksum-pipes and start async process
	for _, csType := range c.checksums {
		rw := rwStruct{}
		rw.reader, rw.writer = io.Pipe()
		c.rws[csType] = rw
		go doChecksum(rw.reader, csType, c.setResult, c.setError, done)
	}
	return nil
}
