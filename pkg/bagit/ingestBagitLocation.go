package bagit

import (
	"fmt"
	"github.com/goph/emperror"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type IngestBagitLocation struct {
	ingest          *Ingest
	bagit           *IngestBagit
	location        *IngestLocation
	status, message string
	start, end      time.Time
}

func NewIngestBagitLocation(ingest *Ingest, bagit *IngestBagit, location *IngestLocation) (*IngestBagitLocation, error) {
	ibl := &IngestBagitLocation{ingest: ingest, bagit: bagit, location: location}
	return ibl, nil
}

func (ibl *IngestBagitLocation) SetData(status, message string, start, end time.Time) error {
	ibl.start = start
	ibl.end = end
	ibl.status = status
	ibl.message = message
	return nil
}

func (ibl *IngestBagitLocation) Exists() (bool, error) {
	o, err := ibl.ingest.bagitLocationLoad(ibl.bagit, ibl.location)
	if err != nil {
		return false, emperror.Wrapf(err, "cannot load bagitLocation %s - %s", ibl.bagit.name, ibl.location.name)
	}
	if o != nil {
		return o.status == "ok", nil
	} else {
		return false, nil
	}
}

func (ibl *IngestBagitLocation) Transfer(source *IngestBagitLocation) error {
	// can only handle file source
	if source.location.path.Scheme != "file" {
		return fmt.Errorf("cannot copy from %s location of %s", source.location.path.Scheme, source.location.name)
	}

	// build source path
	sourceFolder := strings.Trim(source.location.GetPath().Path, "/") + "/"
	if runtime.GOOS == "windows" {
		sourceFolder = strings.Replace(sourceFolder, "|", ":", -1)
	} else {
		sourceFolder = "/" + sourceFolder
	}
	sourcePath := filepath.Join(sourceFolder, ibl.bagit.name)

	// check existence of source
	info, err := os.Stat(sourcePath)
	if err != nil {
		return emperror.Wrapf(err, "cannot stat %s", sourcePath)
	}
	if info.IsDir() {
		return fmt.Errorf("source is a directory - %s", sourcePath)
	}

	ibl.start = time.Now()
	ibl.message = ""

	switch strings.ToLower(ibl.location.path.Scheme) {
	case "file":
		// build target path
		targetFolder := strings.Trim(ibl.location.GetPath().Path, "/") + "/"
		if runtime.GOOS == "windows" {
			targetFolder = strings.Replace(targetFolder, "|", ":", -1)
		} else {
			targetFolder = "/" + targetFolder
		}
		targetPath := filepath.Join(targetFolder, ibl.bagit.name)
		dest, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot create destination file %s", targetPath)
		}
		defer dest.Close()
		src, err := os.OpenFile(sourcePath, os.O_RDONLY, 0666)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot open source file %s", targetPath)
		}
		defer src.Close()
		size, err := io.Copy(dest, src)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot copy %s --> %s", sourcePath, targetPath)
		}
		ibl.message = fmt.Sprintf("copied %v bytes: %s --> %s", size, sourcePath, targetPath)
	case "sftp":
		targetUrlStr := strings.TrimRight(ibl.location.path.String(), "/") + "/" + ibl.bagit.name
		targetUrl, err := url.Parse(targetUrlStr)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot parse url %s", targetUrlStr)
		}
		size, err := ibl.ingest.sftp.PutFile(targetUrl, ibl.location.path.User.Username(), sourcePath)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot put %s --> %s", sourcePath, targetUrl.String())
		}
		ibl.message = fmt.Sprintf("copied %v bytes: %s --> %s", size, sourcePath, targetUrl.String())
	default:
		return fmt.Errorf("invalid target scheme %s", ibl.location.path.Scheme)
	}

	ibl.end = time.Now()
	ibl.status = "ok"
	return ibl.Store()
}

func (ibl *IngestBagitLocation) Store() error {
	return ibl.ingest.bagitLocationStore(ibl)
}
