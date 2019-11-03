package bagit

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goph/emperror"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type BagitFile struct {
	Path        string            `json:"path"`
	Checksum    map[string]string `json:"checksum"`
	Size        int64             `json:"size"`
	Siegfried   []SFMatches       `json:"siegfried,omitempty"`
	baseDir     string            `json:"-"`
	info        os.FileInfo       `json:"-"`
	resultMutex sync.Mutex        `json:"-"`
	errors      []error           `json:"-"`
}

type SFIdentifier struct {
	Name    string `json:"name,omitempty"`
	Details string `json:"details,omitempty"`
}

type SFMatches struct {
	Ns      string `json:"ns,omitempty"`
	Id      string `json:"id,omitempty"`
	Format  string `json:"format,omitempty"`
	Version string `json:"version,omitempty"`
	Mime    string `json:"mime,omitempty"`
	Basis   string `json:"basis,omitempty"`
	Warning string `json:"warning,omitempty"`
}

type SFFiles struct {
	Filename string      `json:"filename,omitempty"`
	Filesize int64       `json:"filesize,omitempty"`
	Modified string      `json:"modified,omitempty"`
	Errors   string      `json:"errors,omitempty"`
	Matches  []SFMatches `json:"matches,omitempty"`
}

type SF struct {
	Siegfried   string         `json:"siegfried,omitempty"`
	Scandate    string         `json:"scandate,omitempty"`
	Signature   string         `json:"signature,omitempty"`
	Created     string         `json:"created,omitempty"`
	Identifiers []SFIdentifier `json:"identfiers,omitempty"`
	Files       []SFFiles      `json:"files,omitempty"`
}

func NewBagitFile(baseDir, path string) (*BagitFile, error) {
	// first check existence etc.
	info, err := os.Stat(path)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot stat %v", path)
	}
	path = strings.TrimPrefix(filepath.ToSlash(path), baseDir)
	if path == "" {
		path = "."
	}
	var bf = &BagitFile{
		Path:     path,
		Size:     info.Size(),
		Checksum: map[string]string{},
		baseDir:  baseDir,
		info:     info,
		errors:   []error{},
	}
	return bf, nil
}

// string repesentation of a file
func (bf *BagitFile) String() string {
	return fmt.Sprintf("[%s]%s", bf.baseDir, bf.Path)
}

// true, if file is a directory
func (bf *BagitFile) IsDir() bool {
	return bf.info.IsDir()
}

// set error of an async process
func (bf *BagitFile) asyncError(err error) {
	bf.resultMutex.Lock()
	defer bf.resultMutex.Unlock()
	bf.errors = append(bf.errors, err)
}

// set result of async Checksum process
func (bf *BagitFile) checksumResult(csType, value string) {
	bf.resultMutex.Lock()
	defer bf.resultMutex.Unlock()
	bf.Checksum[csType] = value
}

func (bf *BagitFile) AddToZip(zipWriter *zip.Writer, checksum []string) error {
	fullpath := filepath.Join(bf.baseDir, bf.Path)
	fileToZip, err := os.Open(fullpath)
	if err != nil {
		return emperror.Wrapf(err, "cannot open %v", fullpath)
	}
	defer fileToZip.Close()

	// create channel to synchronize
	done := make(chan bool)
	defer close(done)

	// create the map of all Checksum-pipes and start async process
	rws := map[string]rwStruct{}
	for _, csType := range checksum {
		rw := rwStruct{}
		rw.reader, rw.writer = io.Pipe()
		rws[csType] = rw
		go doChecksum(rw.reader, csType, bf.checksumResult, bf.asyncError, done)
	}
	// and the zip pipe
	rw := rwStruct{}
	rw.reader, rw.writer = io.Pipe()
	rws["zip"] = rw
	go func() {
		// we should end in all cases
		defer func() { done <- true }()

		header, err := zip.FileInfoHeader(bf.info)
		if err != nil {
			bf.asyncError(emperror.Wrap(err, "cannot create zip.FileInfoHeader"))
		}
		// we write only to the data subfolder
		header.Name = filepath.Join("data", bf.Path)

		// make sure, that compression is ok
		header.Method = zip.Deflate

		zWriter, err := zipWriter.CreateHeader(header)
		if err != nil {
			bf.asyncError(emperror.Wrap(err, "cannot write header to zip"))
		}
		_, err = io.Copy(zWriter, rw.reader)
		if err != nil {
			bf.asyncError(emperror.Wrap(err, "cannot write file to zip"))
		}
	}()

	go func() {
		// close all writers at the end
		defer func() {
			for _, rw := range rws {
				defer rw.writer.Close()
			}
		}()
		// create list of writer
		writers := []io.Writer{}
		for _, rw := range rws {
			writers = append(writers, rw.writer)
		}

		mw := io.MultiWriter(writers...)

		if _, err := io.Copy(mw, fileToZip); err != nil {
			bf.asyncError(emperror.Wrap(err, "cannot write to zip"))
		}
	}()

	// wait until all checksums an zip are done
	for c := 0; c < len(rws); c++ {
		<-done
	}
	// do error handling
	if len(bf.errors) > 0 {
		errstr := ""
		for _, err := range bf.errors {
			errstr = fmt.Sprintf("%v: %v", errstr, err)
		}
		return errors.New(errstr)
	}
	return nil
}

func (bf *BagitFile) GetSiegfried(siegfried string) error {

	urlstring := strings.Replace(siegfried, "[[PATH]]", url.QueryEscape(filepath.Join(bf.baseDir, bf.Path)), -1)

	resp, err := http.Get(urlstring)
	if err != nil {
		return emperror.Wrapf(err, "cannot query siegfried - %v", urlstring)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return emperror.Wrapf(err, "status not ok - %v -> %v", urlstring, resp.Status)
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return emperror.Wrapf(err, "error reading body - %v", urlstring)
	}

	sf := SF{}
	err = json.Unmarshal(bodyBytes, &sf)
	if err != nil {
		return emperror.Wrapf(err, "error decoding json - %v", string(bodyBytes))
	}
	if len(sf.Files) == 0 {
		return emperror.Wrapf(err, "no file in sf result - %v", string(bodyBytes))
	}
	bf.Siegfried = sf.Files[0].Matches
	return nil
}
