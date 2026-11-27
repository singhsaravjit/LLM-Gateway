package main

import (
	"strings"
	"testing"
	"time"
)

func clearConfigEnv(t *testing.T) {
	for _, k := range []string{"PORT", "PROVIDER_URL", "PROVIDER_API_KEY", "PROVIDER_TIMEOUT_MS", "REQUEST_TIMEOUT_MS",
		"SHUTDOWN_TIMEOUT_MS", "ROUTER_MAX_CHEAP_CHARS", "CACHE_ENABLED", "CACHE_TTL_SECONDS"} {
		t.Setenv(k, "")
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	clearConfigEnv(t)
	c, err := loadConfig()
	if err != nil {
		t.Fatalf("defaults should be valid: %v", err)
	}
	if c.Port != "8080" || c.ProviderTimeout != 30*time.Second || c.RequestTimeout != 60*time.Second ||
		!c.CacheEnabled || c.CacheTTL != 5*time.Minute || c.MaxCheapChars != 500 {
		t.Errorf("unexpected defaults: %+v", c)
	}
}

func TestLoadConfigReportsEveryBadValue(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("PROVIDER_TIMEOUT_MS", "3o000")
	t.Setenv("PROVIDER_URL", "localhost:9000")
	t.Setenv("CACHE_ENABLED", "yes please")
	t.Setenv("PORT", "99999")
	_, err := loadConfig()
	if err == nil {
		t.Fatal("want an error for bad config")
	}
	for _, k := range []string{"PROVIDER_TIMEOUT_MS", "PROVIDER_URL", "CACHE_ENABLED", "PORT"} {
		if !strings.Contains(err.Error(), k) {
			t.Errorf("error does not mention %s: %v", k, err)
		}
	}
}

func TestLoadConfigRequestTimeoutBelowProviderTimeout(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("PROVIDER_TIMEOUT_MS", "5000")
	t.Setenv("REQUEST_TIMEOUT_MS", "1000")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "REQUEST_TIMEOUT_MS") {
		t.Errorf("want REQUEST_TIMEOUT_MS error, got %v", err)
	}
}
