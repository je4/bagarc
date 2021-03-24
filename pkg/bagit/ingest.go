package bagit

import (
	"database/sql"
	"github.com/goph/emperror"
	"github.com/je4/bagarc/v2/pkg/ingest"
	"github.com/op/go-logging"
	"os"
	"path/filepath"
	"strings"
)

type BagitIngest struct {
	baseDir string
	logger  *logging.Logger
	ingest  *ingest.Ingest
}

func NewBagitIngest(baseDir string, db *sql.DB, dbschema string, logger *logging.Logger) (*BagitIngest, error) {
	i, err := ingest.NewIngest(db, dbschema)
	if err != nil {
		return nil, err
	}
	bi := &BagitIngest{
		baseDir: baseDir,
		logger:  logger,
		ingest:  i,
	}

	return bi, nil
}

func (bi *BagitIngest) Run() error {
	if err := filepath.Walk(bi.baseDir, func(path string, info os.FileInfo, err error) error {
		// ignore dot files
		if []rune(path)[0] == '.' {
			return nil
		}
		// ignore directories
		if info.IsDir() {
			return nil
		}
		// ignore files without .zip extension
		if !strings.HasSuffix(path, ".zip") {
			return nil
		}

		return nil
	}); err != nil {
		return emperror.Wrapf(err, "error walking %s", bi.baseDir)
	}
	return nil
}
