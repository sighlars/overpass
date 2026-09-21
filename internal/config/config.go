// Package config loads and validates overpass runtime configuration.
// Configuration is plain JSON (stdlib only) with sane defaults for
// anything optional, so a minimal file with just backends works.
package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// BackendConfig describes one upstream server.
type BackendConfig struct {
	URL           string `json:"url"`
	HealthPath    string `json:"health_path"`
	MaxFails      int    `json:"max_fails"`
	FailTimeoutMs int    `json:"fail_timeout_ms"`
}

// Config is the full runtime configuration.
type Config struct {
	Listen          string          `json:"listen"`
	AdminListen     string          `json:"admin_listen"`
	Strategy        string          `json:"strategy"`
	MaxAttempts     int             `json:"max_attempts"`
	HealthEveryMs   int             `json:"health_every_ms"`
	HealthTimeoutMs int             `json:"health_timeout_ms"`
	Backends        []BackendConfig `json:"backends"`
}

// Load reads path, decodes JSON, applies defaults and validates.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

var validStrategies = map[string]bool{
	"round_robin": true,
	"least_conn":  true,
	"random":      true,
}

// Validate fills defaults and rejects unusable configurations.
func (c *Config) Validate() error {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.AdminListen == "" {
		// Localhost-only on purpose: the admin listener exposes
		// drain/undrain. Bind 0.0.0.0 explicitly to expose it.
		c.AdminListen = "127.0.0.1:9090"
	}
	if c.Strategy == "" {
		c.Strategy = "round_robin"
	}
	if !validStrategies[c.Strategy] {
		return fmt.Errorf("unknown strategy %q (want round_robin|least_conn|random)", c.Strategy)
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = 2
	}
	if c.HealthEveryMs <= 0 {
		c.HealthEveryMs = 5000
	}
	if c.HealthTimeoutMs <= 0 {
		c.HealthTimeoutMs = 2000
	}
	if len(c.Backends) == 0 {
		return fmt.Errorf("config needs at least one backend")
	}
	for i := range c.Backends {
		b := &c.Backends[i]
		u, err := url.Parse(b.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("backend %d: invalid url %q", i, b.URL)
		}
		if b.HealthPath == "" {
			b.HealthPath = "/"
		}
		if !strings.HasPrefix(b.HealthPath, "/") {
			return fmt.Errorf("backend %d: health_path must start with /", i)
		}
		if b.MaxFails < 1 {
			b.MaxFails = 2
		}
		if b.FailTimeoutMs <= 0 {
			b.FailTimeoutMs = 10000
		}
	}
	return nil
}
