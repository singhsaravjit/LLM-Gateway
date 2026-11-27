package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Core gateway settings. Component settings (limiter, budget, queue, cache
// backend) are still read in their new* functions until #30.
type config struct {
	Port            string
	ProviderURL     string
	ProviderAPIKey  string
	ProviderTimeout time.Duration // one provider attempt
	RequestTimeout  time.Duration // whole provider step, including retries
	ShutdownTimeout time.Duration
	MaxCheapChars   int
	CacheEnabled    bool
	CacheTTL        time.Duration
}

func loadConfig() (config, error) {
	var errs []error
	ms := func(key string, def int) time.Duration {
		return time.Duration(parseInt(key, def, &errs)) * time.Millisecond
	}
	c := config{
		Port:            env("PORT", "8080"),
		ProviderURL:     env("PROVIDER_URL", "http://localhost:9000"),
		ProviderAPIKey:  os.Getenv("PROVIDER_API_KEY"),
		ProviderTimeout: ms("PROVIDER_TIMEOUT_MS", 30000),
		RequestTimeout:  ms("REQUEST_TIMEOUT_MS", 60000),
		ShutdownTimeout: ms("SHUTDOWN_TIMEOUT_MS", 20000),
		MaxCheapChars:   parseInt("ROUTER_MAX_CHEAP_CHARS", 500, &errs),
		CacheEnabled:    parseBool("CACHE_ENABLED", true, &errs),
		CacheTTL:        time.Duration(parseInt("CACHE_TTL_SECONDS", 300, &errs)) * time.Second,
	}
	errs = append(errs, c.validate()...)
	return c, errors.Join(errs...)
}

func (c config) validate() []error {
	var errs []error
	if p, err := strconv.Atoi(c.Port); err != nil || p < 1 || p > 65535 {
		errs = append(errs, fmt.Errorf("PORT %q is not a valid port", c.Port))
	}
	if u, err := url.Parse(c.ProviderURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, fmt.Errorf("PROVIDER_URL %q must look like http://host:port", c.ProviderURL))
	}
	if c.ProviderTimeout <= 0 {
		errs = append(errs, errors.New("PROVIDER_TIMEOUT_MS must be > 0"))
	}
	if c.RequestTimeout < c.ProviderTimeout {
		errs = append(errs, errors.New("REQUEST_TIMEOUT_MS must be >= PROVIDER_TIMEOUT_MS"))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT_MS must be > 0"))
	}
	if c.MaxCheapChars < 0 {
		errs = append(errs, errors.New("ROUTER_MAX_CHEAP_CHARS must be >= 0"))
	}
	if c.CacheEnabled && c.CacheTTL <= 0 {
		errs = append(errs, errors.New("CACHE_TTL_SECONDS must be > 0 when the cache is on"))
	}
	return errs
}

func parseInt(key string, def int, errs *[]error) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not a whole number", key, v))
		return def
	}
	return n
}

func parseBool(key string, def bool, errs *[]error) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not true or false", key, v))
		return def
	}
	return b
}
