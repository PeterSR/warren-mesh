package manifest

import (
	"encoding/base64"
	"errors"
	"fmt"

	"aead.dev/minisign"
)

// Key is a minisign public key as it appears in a config: the base64 text
// "RW...".
type Key struct {
	Text string
	pub  minisign.PublicKey
	raw  [32]byte
}

// ParseKey reads a public key (algorithm Ed, 42 bytes). A key file's
// "untrusted comment" line is not accepted: the config holds the bare key.
func ParseKey(text string) (Key, error) {
	var k Key
	b, err := base64.StdEncoding.Strict().DecodeString(text)
	if err != nil || len(b) != 42 {
		return k, errors.New("a public key is 42 bytes of base64 (RW...)")
	}
	if err := k.pub.UnmarshalText([]byte(text)); err != nil {
		return k, errors.New("not a minisign public key")
	}
	copy(k.raw[:], b[10:])
	k.Text = text
	return k, nil
}

// Same reports whether two keys hold the same 32 key bytes, whatever their ids.
func (k Key) Same(o Key) bool { return k.raw == o.raw }

// Public is the library's key.
func (k Key) Public() minisign.PublicKey { return k.pub }

// Keys are a component's keys by channel.
type Keys map[string][]Key

// Verify checks a manifest and its detached signature for one configured
// component. Only that component's keys for the channel the manifest names are
// tried; the component name must then equal the configured one; only the
// prehashed algorithm is accepted; and the signed trusted comment must repeat
// the body's identity. The body is the authority; the comment is a guard.
func Verify(data, sig []byte, component string, keys Keys, maxSize int64) (*Manifest, error) {
	m, err := Parse(data)
	if err != nil {
		return nil, err
	}
	if err := m.Check(maxSize); err != nil {
		return nil, err
	}
	var s minisign.Signature
	if err := s.UnmarshalText(sig); err != nil {
		return nil, errors.New("the signature is not a minisign signature")
	}
	if s.Algorithm != minisign.HashEdDSA {
		return nil, errors.New("only prehashed (ED) signatures are accepted")
	}
	candidates := keys[m.Channel]
	if len(candidates) == 0 {
		return nil, fmt.Errorf("component %s holds no %s key", component, m.Channel)
	}
	verified := false
	for _, k := range candidates {
		if k.pub.ID() == s.KeyID && minisign.Verify(k.pub, data, sig) {
			verified = true
			break
		}
	}
	if !verified {
		return nil, fmt.Errorf("the signature does not verify with any %s key of %s", m.Channel, component)
	}
	if m.Component != component {
		return nil, fmt.Errorf("the manifest is for component %q, not %q", m.Component, component)
	}
	if s.TrustedComment != m.TrustedComment() {
		return nil, errors.New("the signed comment does not match the manifest")
	}
	return m, nil
}
