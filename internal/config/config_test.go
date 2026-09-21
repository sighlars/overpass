package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "overpass.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValidAppliesDefaults(t *testing.T) {
	path := writeTemp(t, `{"backends": [{"url": "http://localhost:9001"}]}`)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Listen != ":8080" || c.AdminListen != ":9090" || c.Strategy != "round_robin" {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if c.Backends[0].HealthPath != "/" || c.Backends[0].MaxFails != 2 {
		t.Fatalf("backend defaults not applied: %+v", c.Backends[0])
	}
}

func TestLoadRejects(t *testing.T) {
	cases := map[string]string{
		"no backends":  `{"backends": []}`,
		"bad strategy": `{"strategy": "magic", "backends": [{"url": "http://x"}]}`,
		"bad url":      `{"backends": [{"url": "://oops"}]}`,
		"ftp scheme":   `{"backends": [{"url": "ftp://x"}]}`,
		"bad json":     `{not json`,
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeTemp(t, content)); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("expected error, got nil")
	}
}
