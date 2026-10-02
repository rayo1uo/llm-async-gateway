package config

import (
	"testing"
	"time"
)

func TestLoadEnvAndFlags(t *testing.T) {
	t.Setenv("GATEWAY_ADDR", ":9090")
	t.Setenv("MAX_CONCURRENCY", "4")
	t.Setenv("RESERVED_BATCH_SLOTS", "1")
	t.Setenv("UPSTREAM_URL", "http://upstream:8080")
	cfg, err := Load([]string{"-poll-interval", "10ms"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":9090" || cfg.MaxConcurrency != 4 || cfg.PollInterval != 10*time.Millisecond {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.UpstreamURL != "http://upstream:8080" {
		t.Fatalf("upstream = %s", cfg.UpstreamURL)
	}
}

func TestValidateReservedSlots(t *testing.T) {
	cfg := Default()
	cfg.ReservedBatchSlots = cfg.MaxConcurrency
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected reserved slots to be rejected when they consume every slot")
	}
}

func TestLoadBadDuration(t *testing.T) {
	t.Setenv("LEASE_TTL", "nope")
	if _, err := Load(nil); err == nil {
		t.Fatal("expected duration parse error")
	}
}
