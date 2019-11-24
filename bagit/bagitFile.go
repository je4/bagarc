package bagit

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/common"
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
	ZipPath     string            `json:"zippath"`
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

func NewBagitFile(baseDir, path string, fixFilename bool) (*BagitFile, error) {
	// first checkManifest existence etc.
	info, err := os.Stat(path)
	if err != nil {
		return nil, emperror.Wrapf(err, "cannot stat %v", path)
	}
	path = strings.TrimPrefix(filepath.ToSlash(path), baseDir)
	if path == "" {
		path = "."
	}
	newPath := path
	if fixFilename {
		newPath = common.FixFilename(path)
	}
	var bf = &BagitFile{
		Path:     path,
		ZipPath:  newPath,
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

func (bf *BagitFile) AddToZip(zipWriter *zip.Writer, checksum []string, compression uint16) error {
	fullpath := filepath.Join(bf.baseDir, bf.Path)
	fileToZip, err := os.Open(fullpath)
	if err != nil {
		return emperror.Wrapf(err, "cannot open %v", fullpath)
	}
	defer fileToZip.Close()

	header, err := zip.FileInfoHeader(bf.info)
	if err != nil {
		return emperror.Wrap(err, "cannot create zip.FileInfoHeader")
	}
	// we write only to the data subfolder
	header.Name = filepath.Join("data", bf.ZipPath)

	// make sure, that compression is ok
	header.Method = compression

	zWriter, err := zipWriter.CreateHeader(header)
	if err != nil {
		return emperror.Wrap(err, "cannot write header to zip")
	}

	bf.Checksum, err = ChecksumCopy(zWriter, fileToZip, checksum)
	if err != nil {
		return emperror.Wrapf(err, "cannot write file to zip")
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
