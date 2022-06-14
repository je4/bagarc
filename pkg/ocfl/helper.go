package ocfl

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"golang.org/x/crypto/sha3"
	"hash"
)

func getHash(csType string) (hash.Hash, error) {
	var sink hash.Hash
	switch csType {
	case "md5":
		sink = md5.New()
	case "sha1":
		sink = sha1.New()
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
		return nil, fmt.Errorf("unknown checksum %s", csType)
	}
	return sink, nil
}
