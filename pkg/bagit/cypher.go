package bagit

import (
	"github.com/blend/go-sdk/crypto"
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
