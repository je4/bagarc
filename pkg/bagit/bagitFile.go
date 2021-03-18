package bagit

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/v2/pkg/common"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type BagitFile struct {
	Path     string            `json:"path"`
	ZipPath  string            `json:"zippath"`
	Checksum map[string]string `json:"checksum"`
	Size     int64             `json:"size"`
	//Siegfried   []SFMatches       `json:"indexer,omitempty"`
	Indexer     map[string]interface{} `json:"indexer,omitempty"`
	baseDir     string                 `json:"-"`
	info        os.FileInfo            `json:"-"`
	resultMutex sync.Mutex             `json:"-"`
	errors      []error                `json:"-"`
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
	Siegfried   string         `json:"indexer,omitempty"`
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

func (bf *BagitFile) GetIndexer(indexer string) error {
	var query struct {
		Url          string   `json:"url"`
		Action       []string `json:"action,omitempty"`
		Downloadmime string   `json:"downloadmime,omitempty"`
		Headersize   int64    `json:"headersize,omitempty"`
	}
	var result map[string]interface{}

	query.Url = fmt.Sprintf("file:///%s", filepath.ToSlash(filepath.Join(bf.baseDir, bf.Path)))
	jsonstr, err := json.Marshal(query)
	if err != nil {
		return emperror.Wrapf(err, "cannot marshal json")
	}
	resp, err := http.Post(indexer, "application/json", bytes.NewBuffer(jsonstr))
	if err != nil {
		return emperror.Wrapf(err, "error calling call indexer")
	}
	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return emperror.Wrapf(err, "error reading indexer result")
	}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return emperror.Wrapf(err, "cannot unmarshal result")
	}

	bf.Indexer = result

	return nil
}