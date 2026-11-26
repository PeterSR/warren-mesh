package conformance

import (
	"os"
	"path/filepath"

	"github.com/PeterSR/warren-mesh/client"
)

func entryOf(name, data string) client.Entry {
	e := client.Entry{Name: name}
	if data != "" {
		e.Data = []byte(data)
	}
	return e
}

func attachPath(e *env, name string) string {
	return filepath.Join(e.in.Dir, "n1", "attach", name, "echo.sock")
}

func mkdir(e *env, name string) error {
	return os.Mkdir(filepath.Join(e.in.Dir, "n1", "attach", name), 0o755)
}
