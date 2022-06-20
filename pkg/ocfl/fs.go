package ocfl

import (
	"io"
	"io/fs"
)

// OCFLFS for OCFL we need a fs.ReadDirFS plus Create function
type OCFLFS interface {
	fs.ReadDirFS
	Create(name string) (io.Writer, error)
}
