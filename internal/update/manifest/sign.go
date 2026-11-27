package manifest

import (
	"bytes"
	"io"

	"aead.dev/minisign"
)

// Sign signs manifest bytes with a private key: prehashed (ED), with the
// trusted comment of the manifest. Only warren-release and tests call it;
// the updater never does.
func Sign(key minisign.PrivateKey, data []byte, m *Manifest) []byte {
	r := minisign.NewReader(bytes.NewReader(data))
	_, _ = io.Copy(io.Discard, r)
	return r.SignWithComments(key, m.TrustedComment(), "signature from a warren-release key")
}
