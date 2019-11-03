package bagit

import (
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/common"
	"golang.org/x/crypto/sha3"
	"hash"
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

// start Checksum process
// supported csType's: md5, sha256, sha512
func (c *Checksum) doChecksum(reader io.Reader, csType string, done chan bool) {
	// we should end in all cases
	defer func() { done <- true }()

	var sink hash.Hash
	switch csType {
	case "md5":
		sink = md5.New()
	case "sha256":
		sink = sha256.New()
	case "sha512":
		sink = sha512.New()
	case "sha3-256":
		sink = sha3.New256()
	case "sha3-384":
		sink = sha3.New384()
	case "sha3-512":
		sink = sha3.New512()
	default:
		c.setError(errors.New(fmt.Sprintf("invalid hash function %s", csType)))
		null := &common.NullWriter{}
		io.Copy(null, reader)
		return
	}
	if _, err := io.Copy(sink, reader); err != nil {
		c.setError(emperror.Wrapf(err, "cannot create checkum %s", csType))
		return
	}
	csString := fmt.Sprintf("%x", sink.Sum(nil))
	c.setResult(csType, csString)
}


func NewChecksum( checksums []string ) *Checksum {
	c := &Checksum{
		Mutex:     sync.Mutex{},
		checksums: checksums,
		cs:        map[string]string{},
		errors:    []error{},
		rws:       map[string]rwStruct{},
	}
	return c
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

func (c *Checksum) StartThreads( done chan bool) error {
	// create the map of all Checksum-pipes and start async process
	for _, csType := range c.checksums {
		rw := rwStruct{}
		rw.reader, rw.writer = io.Pipe()
		c.rws[csType] = rw
		go c.doChecksum(rw.reader, csType, done)
	}
	return nil
}

func (c *Checksum) CloseWriter() {
	for _, rw := range c.rws {
		rw.writer.Close()
	}
}

func (c *Checksum) GetErrors() []error {
	return c.errors
}

func (c *Checksum) GetPipes() map[string]rwStruct {
	return c.rws
}

func (c *Checksum) GetChecksums() map[string]string {
	return c.cs
}