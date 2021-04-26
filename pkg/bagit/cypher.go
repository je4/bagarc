package bagit

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"github.com/blend/go-sdk/crypto"
	"github.com/blend/go-sdk/ex"
	"github.com/goph/emperror"
	"io"
)

func Encrypt(in io.Reader, out io.Writer) ([]byte, []byte, []byte, error) {
	key, err := crypto.CreateKey(crypto.DefaultKeySize)
	if err != nil {
		return nil, nil, nil, emperror.Wrap(err, "cannot generate key")
	}
	/*
		iv, err := crypto.CreateKey(crypto.IVSize	)
		if err != nil {
			return nil, nil, emperror.Wrap(err, "cannot generate iv")
		}
	*/

	enc, err := crypto.NewStreamEncrypter(key, in)
	if err != nil {
		return nil, nil, nil, emperror.Wrap(err, "cannot create stream encrypter")
	}

	if _, err := io.Copy(out, enc); err != nil {
		return nil, nil, nil, emperror.Wrap(err, "cannot encrypt")
	}

	return key, enc.IV, enc.Meta().Hash, nil
}

func CreateEncoder(in io.Reader, key, iv []byte) (*crypto.StreamEncrypter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ex.New(err)
	}
	stream := cipher.NewCTR(block, iv)
	mac := hmac.New(sha256.New, key)
	return &crypto.StreamEncrypter{
		Source: in,
		Block:  block,
		Stream: stream,
		Mac:    mac,
		IV:     iv,
	}, nil
}
