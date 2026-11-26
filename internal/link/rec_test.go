package link

import (
	"net/http"
	"net/http/httptest"
)

type rec struct{ *httptest.ResponseRecorder }

func newRec() rec { return rec{httptest.NewRecorder()} }

func (r rec) result() *http.Response { return r.Result() }
