package bagit

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"github.com/goph/emperror"
	"github.com/op/go-logging"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type NullWriter struct{}

func (w *NullWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

type BlockReader struct {
	buf   []byte
	block cipher.BlockMode
	in    io.Reader
}

func NewBlockReader(blockMode cipher.BlockMode, reader io.Reader) *BlockReader {
	return &BlockReader{
		block: blockMode,
		in:    reader,
	}
}

func (b *BlockReader) Read(p []byte) (n int, err error) {
	toRead := len(p)
	mul := toRead / b.block.BlockSize()
	size := mul * b.block.BlockSize()
	if cap(b.buf) != size {
		b.buf = make([]byte, toRead, toRead)
	}

	read, err := b.in.Read(b.buf)
	if err != nil {
		return 0, err
	}

	if read < b.block.BlockSize() {
		return 0, io.ErrUnexpectedEOF
	}
	b.block.CryptBlocks(b.buf, b.buf)
	return copy(p, b.buf), nil
}

func EncryptAES256(dst io.Writer, src io.Reader) (key, iv []byte, err error) {

	key = make([]byte, 32)
	iv = make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, nil, emperror.Wrapf(err, "cannot generate 32 byte key for aes256")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, emperror.Wrapf(err, "cannot create aes256 cipher")
	}
	mode := cipher.NewCBCEncrypter(block, iv)
	br := NewBlockReader(mode, src)
	if _, err := io.Copy(dst, br); err != nil {
		return nil, nil, emperror.Wrap(err, "cannot encrypt data")
	}
	return
}

func SHA512(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", emperror.Wrapf(err, "cannot open %s", filename)
	}
	defer f.Close()
	sha := sha512.New()
	if _, err := io.Copy(sha, f); err != nil {
		return "", emperror.Wrapf(err, "cannot read/calculate checksum of %s", filename)
	}
	return fmt.Sprintf("%x", sha.Sum(nil)), nil
}

func CreateLogger(module string, logfile string, w *io.PipeWriter, loglevel string, logformat string) (log *logging.Logger, lf *os.File) {
	log = logging.MustGetLogger(module)
	var err error
	if logfile != "" {
		lf, err = os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Errorf("Cannot open logfile %v: %v", logfile, err)
		}
		//defer lf.Close()

	} else {
		lf = os.Stderr
	}
	var w2 io.Writer
	if w != nil {
		w2 = io.MultiWriter(w, lf)
	} else {
		w2 = lf
	}
	backend := logging.NewLogBackend(w2, "", 0)
	backendLeveled := logging.AddModuleLevel(backend)
	backendLeveled.SetLevel(logging.GetLevel(loglevel), "")

	logging.SetFormatter(logging.MustStringFormatter(logformat))
	logging.SetBackend(backendLeveled)

	return
}

/**********************************************************************
 * 1) Forbid/escape ASCII control characters (bytes 1-31 and 127) in filenames, including newline, escape, and tab.
 *    I know of no user or program that actually requires this capability. As far as I can tell, this capability
 *    exists only to make it hard to write correct software, to ease the job of attackers, and to create
 *    interoperability problems. Chuck it.
 * 2) Forbid/escape leading “-”. This way, you can always distinguish option flags from filenames, eliminating a host
 *    of stupid errors. Nobody in their right mind writes programs that depend on having dash-prefixed files on a Unix
 *    system. Even on Windows systems they’re a bad idea, because many programs use “-” instead of “/” to identify options.
 * 3) Forbid/escape filenames that aren’t a valid UTF-8 encoding. This way, filenames can always be correctly displayed.
 *    Trying to use environment values like LC_ALL (or other LC_* values) or LANG is just a hack that often fails. This
 *    will take time, as people slowly transition and minor tool problems get fixed, but I believe that transition is
 *    already well underway.
 * 4) Forbid/escape leading/trailing space characters — at least trailing spaces. Adjacent spaces are somewhat dodgy,
 *    too. These confuse users when they happen, with no utility. In particular, filenames that are only space characters
 *    are nothing but trouble. Some systems may want to go further and forbid space characters outright, but I doubt that’ll
 *    be acceptable everywhere, and with the other approaches these are less necessary. As noted above, an interesting
 *    alternative would be quietly convert (in the API) all spaces into unbreakable spaces.
 * 5) Forbid/escape “problematic” characters that get specially interpreted by shells, other interpreters (such as perl),
 *    and HTML/XML. This is less important, and I would expect this to happen (at most) on specific systems. With the steps
 *    above, a lot of programs and statements like “cat *” just work correctly. But funny characters cause troubles for shell
 *    scripts and perl, because they need to quote them when typing in commands.. and they often forget to do so. They can
 *    also be a cause for trouble when they’re passed down to other programs, especially if they run “exec” and so on. They’re
 *    also helpful for web applications, again, because the characters that should be escapes are sometimes not escaped. A short
 *    list would be “*”, “?”, and “[”; by eliminating those three characters and control characters from filenames, and removing
 *    the space character from IFS, you can process filenames in shells without quoting variable references — eliminating a
 *    common source of errors. Forbidding/escaping “<” and “>” would eliminate a source of nasty errors for perl programs, web
 *    applications, and anyone using HTML or XML. A more stringent list would be “*?:[]"<>|(){}&'!\;” (this is Glindra’s “safe”
 *    list with ampersand, single-quote, bang, backslash, and semicolon added). This list is probably a little extreme, but let’s
 *    try and see. As noted earlier, I’d need to go through a complete analysis of all characters for a final list; for security,
 *    you want to identify everything that is permissible, and disallow everything else, but its manifestation can be either way
 *    as long as you’ve considered all possible cases. But if this set can be determined locally, based on local requirements,
 *    there’s less need to get complete agreement on a list.
 * 6) Forbid/escape leading “~” (tilde). Shells specially interpret such filenames. This is definitely low priority.
 *
 * https://www.dwheeler.com/essays/fixing-unix-linux-filenames.html
 */
func FixFilename(fname string) string {
	rule_1_5 := regexp.MustCompile("[\x00-\x1F\x7F\n\r\t*?:\\[\\]\"<>|(){}&'!\\;]")
	rule_2_4_6 := regexp.MustCompile("^[\\s\\-~]*(.*?)\\s*$")

	fname = strings.ToValidUTF8(fname, "")

	names := strings.Split(fname, "/")
	result := []string{}

	for _, n := range names {
		n = rule_1_5.ReplaceAllString(n, "_")
		n = rule_2_4_6.ReplaceAllString(n, "$1")
		result = append(result, n)
	}

	fname = filepath.ToSlash(filepath.Join(result...))
	if len(result) > 0 {
		if result[0] == "" {
			fname = "/" + fname
		}

	}
	return fname
}
