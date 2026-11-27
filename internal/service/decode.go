package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// Bodies that a peer or a service chose are decoded one element at a time,
// so that no array is built before its limits are known: json.Unmarshal would
// make a slice of every element of a body first (a 4 MiB body of empty
// entries is hundreds of thousands of them) and only then could the count be
// looked at. Here the count, the reserved names and the size of each value are
// checked as each element is read, before anything is kept.

// errLimit is a limit of a body that was passed: it is not a syntax error.
type errLimit struct{ msg string }

func (e *errLimit) Error() string { return e.msg }

// walkObject reads the one JSON object in body (null is an empty object) and
// calls field with each key, for it to read the value from dec. Nothing but
// white space may follow the object.
func walkObject(body []byte, field func(key string, dec *json.Decoder) error) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok == nil {
		return trailing(dec)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return errors.New("the body is not a JSON object")
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := kt.(string)
		if !ok {
			return errors.New("not an object key")
		}
		if err := field(key, dec); err != nil {
			return err
		}
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return err
	}
	return trailing(dec)
}

func trailing(dec *json.Decoder) error {
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("unexpected data after the JSON value")
	}
	return nil
}

// walkArray reads a JSON array from dec (null is an empty one) and calls
// item with the index of each element, for it to read the element from dec.
// It reports whether the array was there (not null).
func walkArray(dec *json.Decoder, item func(i int) error) (bool, error) {
	tok, err := dec.Token()
	if err != nil {
		return false, err
	}
	if tok == nil {
		return false, nil
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return false, errors.New("expected an array")
	}
	for i := 0; dec.More(); i++ {
		if err := item(i); err != nil {
			return false, err
		}
	}
	_, err = dec.Token() // the closing bracket
	return true, err
}

// skipValue reads and drops the next value.
func skipValue(dec *json.Decoder) error {
	var skip json.RawMessage
	return dec.Decode(&skip)
}

// ---- PUT ----

// putEntry is one entry of a PUT.
type putEntry struct {
	Name      string          `json:"name"`
	Namespace string          `json:"namespace"`
	Data      json.RawMessage `json:"data"`
}

type putRequest struct {
	Entries  []putEntry
	Reserved []string
}

// decodePut reads a PUT body, refusing more than MaxPutEntries entries, more
// than MaxPutReserved reserved names and any data over MaxEntryData as soon as
// they are met. The size that counts for data is that of the compact form,
// which is what is served.
func decodePut(body []byte) (*putRequest, error) {
	var req putRequest
	err := walkObject(body, func(key string, dec *json.Decoder) error {
		switch {
		case strings.EqualFold(key, "entries"):
			req.Entries = nil
			_, err := walkArray(dec, func(i int) error {
				if i >= MaxPutEntries {
					return &errLimit{fmt.Sprintf("at most %d entries per PUT", MaxPutEntries)}
				}
				var e putEntry
				if err := dec.Decode(&e); err != nil {
					return err
				}
				if len(e.Data) > 0 {
					var c bytes.Buffer
					if err := json.Compact(&c, e.Data); err != nil {
						return &errLimit{fmt.Sprintf("entry %q: data is not JSON", e.Name)}
					}
					e.Data = c.Bytes()
				}
				if len(e.Data) > MaxEntryData {
					return &errLimit{fmt.Sprintf("entry %q: data is over %d bytes", e.Name, MaxEntryData)}
				}
				req.Entries = append(req.Entries, e)
				return nil
			})
			return err
		case strings.EqualFold(key, "reserved"):
			req.Reserved = nil
			_, err := walkArray(dec, func(i int) error {
				if i >= MaxPutReserved {
					return &errLimit{fmt.Sprintf("at most %d reserved names per PUT", MaxPutReserved)}
				}
				var r string
				if err := dec.Decode(&r); err != nil {
					return err
				}
				req.Reserved = append(req.Reserved, r)
				return nil
			})
			return err
		}
		return skipValue(dec)
	})
	if err != nil {
		return nil, err
	}
	return &req, nil
}

// putError turns what decodePut returned into the answer.
func putError(err error) *apierr.Error {
	var l *errLimit
	if errors.As(err, &l) {
		return apierr.New(apierr.BadRequest, l.msg)
	}
	return apierr.New(apierr.BadRequest, "the body is not the expected JSON: "+err.Error())
}

// ---- lists from peers ----

// peerList is a decoded list answer of a peer, already vouched for.
type peerList struct {
	Node, Version, Service string
	HasEntries             bool
	Entries                []listed
	Reserved               []string
}

// decodePeerList reads a list answer of node and vouches for it as it goes
// (see vouchEntry): only what may be used is kept, at most MaxListEntries
// entries and MaxPutReserved reserved names, and the rest is counted by drop.
func decodePeerList(b []byte, node string, drop func(what, reason string)) (*peerList, error) {
	if len(b) > MaxListBody {
		return nil, fmt.Errorf("%w: the answer is over %d bytes", errListTooBig, MaxListBody)
	}
	l := &peerList{}
	v := newVoucher(node, drop)
	err := walkObject(b, func(key string, dec *json.Decoder) error {
		switch {
		case strings.EqualFold(key, "node"):
			return dec.Decode(&l.Node)
		case strings.EqualFold(key, "version"):
			return dec.Decode(&l.Version)
		case strings.EqualFold(key, "service"):
			return dec.Decode(&l.Service)
		case strings.EqualFold(key, "entries"):
			*v = *newVoucher(node, drop) // a repeated key: the last one wins
			var e wireEntry              // one, used for every element: nothing is built up
			present, err := walkArray(dec, func(int) error {
				e = wireEntry{}
				if err := dec.Decode(&e); err != nil {
					return err
				}
				v.add(e)
				return nil
			})
			l.HasEntries = present
			return err
		case strings.EqualFold(key, "reserved"):
			l.Reserved = nil // a repeated key: the last one wins
			tok, err := dec.Token()
			if err != nil || tok == nil {
				return err
			}
			if d, ok := tok.(json.Delim); !ok || d != '{' {
				return errors.New("reserved is not an object")
			}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return err
				}
				ns, _ := kt.(string)
				if ns != node {
					drop("reserved names for "+ns, "namespace")
					// Read as names, though not kept: a value that is not a
					// list of strings is malformed whoever it is for.
					if _, err := walkArray(dec, func(int) error {
						var nm string
						return dec.Decode(&nm)
					}); err != nil {
						return err
					}
					continue
				}
				l.Reserved = nil // the same namespace twice: the last one wins
				if _, err := walkArray(dec, func(int) error {
					var nm string
					if err := dec.Decode(&nm); err != nil {
						return err
					}
					if len(l.Reserved) >= MaxPutReserved || !grammar.EntryName(nm) {
						drop("reserved name "+nm, "reserved")
						return nil
					}
					l.Reserved = append(l.Reserved, nm)
					return nil
				}); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		}
		return skipValue(dec)
	})
	if err != nil {
		return nil, fmt.Errorf("malformed answer: %w", err)
	}
	l.Entries = v.out
	return l, nil
}

// decodeServices reads the answer to a services poll of a peer: the names of
// the services that are valid and not repeated, the first MaxPeerServices of
// them. listed is whether the services were there.
func decodeServices(b []byte) (node, version string, names []string, listed bool, err error) {
	if len(b) > MaxListBody {
		return "", "", nil, false, fmt.Errorf("%w: the answer is over %d bytes", errListTooBig, MaxListBody)
	}
	seen := map[string]bool{}
	err = walkObject(b, func(key string, dec *json.Decoder) error {
		switch {
		case strings.EqualFold(key, "node"):
			return dec.Decode(&node)
		case strings.EqualFold(key, "version"):
			return dec.Decode(&version)
		case strings.EqualFold(key, "services"):
			var err error
			listed, err = walkArray(dec, func(int) error {
				var sv string
				if err := dec.Decode(&sv); err != nil {
					return err
				}
				if len(names) < MaxPeerServices && grammar.Service(sv) && !seen[sv] {
					seen[sv] = true
					names = append(names, sv)
				}
				return nil
			})
			return err
		}
		return skipValue(dec)
	})
	if err != nil {
		err = fmt.Errorf("malformed answer: %w", err)
	}
	return
}
