package bagit

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/goph/emperror"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"unicode"
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
		newPath = FixFilename(path)
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

func (bf *BagitFile) GetIndexer(indexer string, checks []string, fileMap map[string]string) error {
	var query struct {
		Url           string   `json:"url"`
		Actions       []string `json:"actions,omitempty"`
		ForceDownload string   `json:"forcedownload,omitempty"`
		Headersize    int64    `json:"headersize,omitempty"`
	}
	query.Actions = checks
	query.ForceDownload = ".*/.*"
	var result map[string]interface{}

	bd := bf.baseDir
	found := false

	if runtime.GOOS == "windows" {
		a := []rune(bd)
		a[0] = unicode.ToLower(a[0])
		bd = string(a)
	}

	for key, val := range fileMap {
		if strings.HasPrefix(bd, val) {
			ustr := fmt.Sprintf("file://%s/%s", key, url.PathEscape(strings.TrimLeft(filepath.ToSlash(filepath.Join(bd[len(val):], bf.Path)), "/")))
			u, err := url.Parse(ustr)
			if err != nil {
				return emperror.Wrapf(err, "cannot parse url %s", ustr)
			}
			query.Url = u.String()
			found = true
		}
	}
	if !found {
		return fmt.Errorf("path %s not in filemap", bd)
	}
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
