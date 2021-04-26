package bagit

import (
	"crypto/aes"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"github.com/blend/go-sdk/crypto"
	"github.com/goph/emperror"
	"github.com/tidwall/transform"
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

func (ibl *IngestBagitLocation) createEncrypt(src io.Reader) ([]byte, []byte, io.Reader, error) {
	var err error
	key := ibl.bagit.GetKey()
	if key == nil {
		key, err = crypto.CreateKey(crypto.DefaultKeySize)
		if err != nil {
			return nil, nil, nil, emperror.Wrap(err, "cannot generate key")
		}
		if err := ibl.bagit.SetKey(key); err != nil {
			return nil, nil, nil, emperror.Wrap(err, "cannot write key")
		}
	}
	iv := ibl.bagit.GetIV()
	if iv == nil {
		iv = make([]byte, aes.BlockSize)
		_, err = rand.Read(iv)
		if err != nil {
			return nil, nil, nil, emperror.Wrap(err, "cannot create iv")
		}
		if err := ibl.bagit.SetIV(iv); err != nil {
			return nil, nil, nil, emperror.Wrap(err, "cannot write iv")
		}
	}
	enc, err := CreateEncoder(src, key, iv)
	if err != nil {
		return nil, nil, nil, emperror.Wrap(err, "cannot create stream encrypter")
	}
	var rbuf = make([]byte, 4096)
	r := transform.NewTransformer(func() ([]byte, error) {
		var err error
		n, err := enc.Read(rbuf)
		if err != nil {
			return nil, err
		}
		return rbuf[:n], nil
	})
	return key, iv, r, nil
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
	var message string
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
		if ibl.location.IsEncrypted() {
			targetPath += "." + encExt
		}
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
			return emperror.Wrapf(err, "cannot open source file %s", sourcePath)
		}
		defer src.Close()
		ibl.ingest.logger.Infof("copying %s --> %s", sourcePath, targetPath)
		var r io.Reader
		if ibl.location.IsEncrypted() {
			var key, iv []byte
			key, iv, r, err = ibl.createEncrypt(src)
			message = fmt.Sprintf("decrypt using openssl: \n openssl enc -aes-256-ctr -nosalt -d -in %s.%s -out /tmp/%s -K '%x' -iv '%x'", ibl.bagit.name, encExt, ibl.bagit.name, string(key), string(iv))
		} else {
			r = src
		}
		shaSink := sha512.New()
		w := io.MultiWriter(dest, shaSink)
		size, err := io.Copy(w, r)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot copy %s --> %s", sourcePath, targetPath)
		}
		ibl.message = fmt.Sprintf("copied %v bytes: %s --> %s", size, sourcePath, targetPath)
		ibl.ingest.logger.Infof("copying %v bytes", size)
		if message != "" {
			ibl.ingest.logger.Infof(message)
		}
		sha512Str := fmt.Sprintf("%x", shaSink.Sum(nil))
		if ibl.location.IsEncrypted() {
			if ibl.bagit.sha512_aes == "" {
				ibl.bagit.sha512_aes = sha512Str
				ibl.bagit.Store()
			} else {
				if sha512Str != ibl.bagit.sha512_aes {
					return emperror.Wrapf(err, "invalid checksum %s != %s", ibl.bagit.sha512_aes, sha512Str)
				}
			}
		} else {
			if sha512Str != ibl.bagit.sha512 {
				return emperror.Wrapf(err, "invalid checksum %s != %s", ibl.bagit.sha512, sha512Str)
			}
		}
	case "sftp":
		targetUrlStr := strings.TrimRight(ibl.location.path.String(), "/") + "/" + ibl.bagit.name
		if ibl.location.IsEncrypted() {
			targetUrlStr += "." + encExt
		}
		targetUrl, err := url.Parse(targetUrlStr)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot parse url %s", targetUrlStr)
		}
		ibl.ingest.logger.Infof("copying %s --> %s", sourcePath, targetUrl.String())

		src, err := os.OpenFile(sourcePath, os.O_RDONLY, 0666)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot open source file %s", sourcePath)
		}
		defer src.Close()
		var r io.Reader
		if ibl.location.IsEncrypted() {
			var key, iv []byte
			key, iv, r, err = ibl.createEncrypt(src)
			message = fmt.Sprintf("decrypt using openssl: \n openssl enc -aes-256-ctr -nosalt -d -in %s.%s -out /tmp/%s -K '%x' -iv '%x'", ibl.bagit.name, encExt, ibl.bagit.name, string(key), string(iv))
		} else {
			r = src
		}

		size, sha512Str, err := ibl.ingest.sftp.Put(targetUrl, ibl.location.path.User.Username(), r)
		if err != nil {
			ibl.status = "error"
			ibl.Store()
			return emperror.Wrapf(err, "cannot put %s --> %s", sourcePath, targetUrl.String())
		}
		ibl.message = fmt.Sprintf("copied %v bytes: %s --> %s", size, sourcePath, targetUrl.String())
		ibl.ingest.logger.Infof("copying %v bytes", size)
		if message != "" {
			ibl.ingest.logger.Infof(message)
		}
		if ibl.location.IsEncrypted() {
			if ibl.bagit.sha512_aes == "" {
				ibl.bagit.sha512_aes = sha512Str
				ibl.bagit.Store()
			} else {
				if sha512Str != ibl.bagit.sha512_aes {
					return emperror.Wrapf(err, "invalid checksum %s != %s", ibl.bagit.sha512_aes, sha512Str)
				}
			}
		} else {
			if sha512Str != ibl.bagit.sha512 {
				return emperror.Wrapf(err, "invalid checksum %s != %s", ibl.bagit.sha512, sha512Str)
			}
		}
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
