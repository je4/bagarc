package ingest

import "time"

type Bagit struct {
	id           int64
	name         string
	size         int64
	sha512       string
	sha512_aes   string
	report       string
	creator      string
	creationdate time.Time
}
