// Package config loads gateway settings from flags and environment variables.
package config

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config is the process configuration. Zero values are not used; call Default or Load.
type Config struct {
	Addr                    string
	RedisAddr               string
	RedisPassword           string
	RedisDB                 int
	KeyPrefix               string
	UpstreamURL             string
	MaxConcurrency          int
	ReservedBatchSlots      int
	RateLimitRPS            float64
	RateBurst               int
	LeaseTTL                time.Duration
	PollInterval            time.Duration
	RequestTimeout          time.Duration
	DefaultCompletionWindow time.Duration
	DefaultNearlineDeadline time.Duration
	BatchEnqueueWindow      int
	BatchReserveEvery       int
	AgingSlack              time.Duration
	ResultTTL               time.Duration
	MaxFileBytes            int64
	RetryBase               time.Duration
	RetryMax                time.Duration
	MaxAttempts             int
	LogLevel                string
}

// Default returns demo-friendly settings.
func Default() Config {
	return Config{
		Addr:                    ":8080",
		RedisAddr:               "127.0.0.1:6379",
		KeyPrefix:               "lag",
		UpstreamURL:             "http://127.0.0.1:8090",
		MaxConcurrency:          8,
		ReservedBatchSlots:      1,
		RateLimitRPS:            50,
		RateBurst:               16,
		LeaseTTL:                30 * time.Second,
		PollInterval:            50 * time.Millisecond,
		RequestTimeout:          60 * time.Second,
		DefaultCompletionWindow: 24 * time.Hour,
		DefaultNearlineDeadline: 5 * time.Minute,
		BatchEnqueueWindow:      32,
		BatchReserveEvery:       5,
		AgingSlack:              2 * time.Minute,
		ResultTTL:               48 * time.Hour,
		MaxFileBytes:            10 << 20,
		RetryBase:               200 * time.Millisecond,
		RetryMax:                30 * time.Second,
		MaxAttempts:             8,
		LogLevel:                "info",
	}
}

// Load reads environment variables, then applies flags from args.
// Flags win when the user passes them explicitly because the flag default is
// the value already taken from the environment.
func Load(args []string) (Config, error) {
	cfg := Default()
	var err error
	if cfg.Addr, err = envStr("GATEWAY_ADDR", cfg.Addr); err != nil {
		return Config{}, err
	}
	if cfg.RedisAddr, err = envStr("REDIS_ADDR", cfg.RedisAddr); err != nil {
		return Config{}, err
	}
	if cfg.RedisPassword, err = envStr("REDIS_PASSWORD", cfg.RedisPassword); err != nil {
		return Config{}, err
	}
	if cfg.RedisDB, err = envInt("REDIS_DB", cfg.RedisDB); err != nil {
		return Config{}, err
	}
	if cfg.KeyPrefix, err = envStr("KEY_PREFIX", cfg.KeyPrefix); err != nil {
		return Config{}, err
	}
	if cfg.UpstreamURL, err = envStr("UPSTREAM_URL", cfg.UpstreamURL); err != nil {
		return Config{}, err
	}
	if cfg.MaxConcurrency, err = envInt("MAX_CONCURRENCY", cfg.MaxConcurrency); err != nil {
		return Config{}, err
	}
	if cfg.ReservedBatchSlots, err = envInt("RESERVED_BATCH_SLOTS", cfg.ReservedBatchSlots); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitRPS, err = envFloat("RATE_LIMIT_RPS", cfg.RateLimitRPS); err != nil {
		return Config{}, err
	}
	if cfg.RateBurst, err = envInt("RATE_BURST", cfg.RateBurst); err != nil {
		return Config{}, err
	}
	if cfg.LeaseTTL, err = envDuration("LEASE_TTL", cfg.LeaseTTL); err != nil {
		return Config{}, err
	}
	if cfg.PollInterval, err = envDuration("POLL_INTERVAL", cfg.PollInterval); err != nil {
		return Config{}, err
	}
	if cfg.RequestTimeout, err = envDuration("REQUEST_TIMEOUT", cfg.RequestTimeout); err != nil {
		return Config{}, err
	}
	if cfg.DefaultCompletionWindow, err = envDuration("DEFAULT_COMPLETION_WINDOW", cfg.DefaultCompletionWindow); err != nil {
		return Config{}, err
	}
	if cfg.DefaultNearlineDeadline, err = envDuration("DEFAULT_NEARLINE_DEADLINE", cfg.DefaultNearlineDeadline); err != nil {
		return Config{}, err
	}
	if cfg.BatchEnqueueWindow, err = envInt("BATCH_ENQUEUE_WINDOW", cfg.BatchEnqueueWindow); err != nil {
		return Config{}, err
	}
	if cfg.BatchReserveEvery, err = envInt("BATCH_RESERVE_EVERY", cfg.BatchReserveEvery); err != nil {
		return Config{}, err
	}
	if cfg.AgingSlack, err = envDuration("AGING_SLACK", cfg.AgingSlack); err != nil {
		return Config{}, err
	}
	if cfg.ResultTTL, err = envDuration("RESULT_TTL", cfg.ResultTTL); err != nil {
		return Config{}, err
	}
	if cfg.MaxFileBytes, err = envInt64("MAX_FILE_BYTES", cfg.MaxFileBytes); err != nil {
		return Config{}, err
	}
	if cfg.RetryBase, err = envDuration("RETRY_BASE", cfg.RetryBase); err != nil {
		return Config{}, err
	}
	if cfg.RetryMax, err = envDuration("RETRY_MAX", cfg.RetryMax); err != nil {
		return Config{}, err
	}
	if cfg.MaxAttempts, err = envInt("MAX_ATTEMPTS", cfg.MaxAttempts); err != nil {
		return Config{}, err
	}
	if cfg.LogLevel, err = envStr("LOG_LEVEL", cfg.LogLevel); err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "HTTP listen address")
	fs.StringVar(&cfg.RedisAddr, "redis-addr", cfg.RedisAddr, "Redis address host:port")
	fs.StringVar(&cfg.RedisPassword, "redis-password", cfg.RedisPassword, "Redis password")
	fs.IntVar(&cfg.RedisDB, "redis-db", cfg.RedisDB, "Redis database index")
	fs.StringVar(&cfg.KeyPrefix, "key-prefix", cfg.KeyPrefix, "Redis key prefix")
	fs.StringVar(&cfg.UpstreamURL, "upstream-url", cfg.UpstreamURL, "OpenAI-compatible inference base URL")
	fs.IntVar(&cfg.MaxConcurrency, "max-concurrency", cfg.MaxConcurrency, "maximum in-flight upstream calls")
	fs.IntVar(&cfg.ReservedBatchSlots, "reserved-batch-slots", cfg.ReservedBatchSlots, "concurrency slots held back for batch")
	fs.Float64Var(&cfg.RateLimitRPS, "rate-limit-rps", cfg.RateLimitRPS, "dispatch admission rate (requests/second)")
	fs.IntVar(&cfg.RateBurst, "rate-burst", cfg.RateBurst, "dispatch token bucket burst")
	fs.DurationVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "visibility timeout for a claimed request")
	fs.DurationVar(&cfg.PollInterval, "poll-interval", cfg.PollInterval, "dispatcher and controller poll interval")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "per-attempt upstream timeout")
	fs.DurationVar(&cfg.DefaultCompletionWindow, "default-completion-window", cfg.DefaultCompletionWindow, "batch completion window when omitted")
	fs.DurationVar(&cfg.DefaultNearlineDeadline, "default-nearline-deadline", cfg.DefaultNearlineDeadline, "nearline deadline when omitted")
	fs.IntVar(&cfg.BatchEnqueueWindow, "batch-enqueue-window", cfg.BatchEnqueueWindow, "max queued+in-flight lines per batch")
	fs.IntVar(&cfg.BatchReserveEvery, "batch-reserve-every", cfg.BatchReserveEvery, "claim a batch request at least this often")
	fs.DurationVar(&cfg.AgingSlack, "aging-slack", cfg.AgingSlack, "promote batch work whose deadline is within this slack")
	fs.DurationVar(&cfg.ResultTTL, "result-ttl", cfg.ResultTTL, "TTL for stored nearline and unit results")
	fs.Int64Var(&cfg.MaxFileBytes, "max-file-bytes", cfg.MaxFileBytes, "maximum uploaded file size")
	fs.DurationVar(&cfg.RetryBase, "retry-base", cfg.RetryBase, "base delay for exponential backoff")
	fs.DurationVar(&cfg.RetryMax, "retry-max", cfg.RetryMax, "cap on exponential backoff")
	fs.IntVar(&cfg.MaxAttempts, "max-attempts", cfg.MaxAttempts, "maximum upstream attempts before a request fails")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "slog level: debug, info, warn, error")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks ranges and URL shape.
func (c Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("addr is required")
	}
	if c.RedisAddr == "" {
		return fmt.Errorf("redis address is required")
	}
	if c.KeyPrefix == "" {
		return fmt.Errorf("key prefix is required")
	}
	u, err := url.Parse(c.UpstreamURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("upstream url %q is invalid", c.UpstreamURL)
	}
	if c.MaxConcurrency < 1 {
		return fmt.Errorf("max concurrency must be >= 1")
	}
	if c.ReservedBatchSlots < 0 || c.ReservedBatchSlots >= c.MaxConcurrency {
		return fmt.Errorf("reserved batch slots must be in [0, max concurrency)")
	}
	if c.RateLimitRPS <= 0 {
		return fmt.Errorf("rate limit must be > 0")
	}
	if c.RateBurst < 1 {
		return fmt.Errorf("rate burst must be >= 1")
	}
	if c.LeaseTTL <= 0 || c.PollInterval <= 0 || c.RequestTimeout <= 0 {
		return fmt.Errorf("lease, poll interval, and request timeout must be > 0")
	}
	if c.DefaultCompletionWindow <= 0 || c.DefaultNearlineDeadline <= 0 {
		return fmt.Errorf("default windows must be > 0")
	}
	if c.BatchEnqueueWindow < 1 {
		return fmt.Errorf("batch enqueue window must be >= 1")
	}
	if c.BatchReserveEvery < 0 {
		return fmt.Errorf("batch reserve every must be >= 0")
	}
	if c.AgingSlack < 0 || c.ResultTTL <= 0 || c.MaxFileBytes < 1 {
		return fmt.Errorf("aging slack, result ttl, or max file bytes is invalid")
	}
	if c.RetryBase <= 0 || c.RetryMax < c.RetryBase {
		return fmt.Errorf("retry delays are invalid")
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("max attempts must be >= 1")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log level %q is invalid", c.LogLevel)
	}
	return nil
}

func envStr(key, def string) (string, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	return v, nil
}

func envInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envInt64(key string, def int64) (int64, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envFloat(key string, def float64) (float64, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
