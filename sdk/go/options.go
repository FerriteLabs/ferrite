package ferrite

import (
	"crypto/tls"
	"time"
)

// Option configures the Ferrite client.
type Option func(*options)

type options struct {
	password   string
	db         int
	tlsConfig  *tls.Config
	poolSize   int
	timeout    time.Duration
	maxRetries int
	retryBackoff time.Duration
}

func defaultOptions() *options {
	return &options{
		db:           0,
		poolSize:     10,
		timeout:      5 * time.Second,
		maxRetries:   3,
		retryBackoff: 100 * time.Millisecond,
	}
}

// WithPassword sets the authentication password.
func WithPassword(password string) Option {
	return func(o *options) {
		o.password = password
	}
}

// WithDB selects the database index.
func WithDB(db int) Option {
	return func(o *options) {
		o.db = db
	}
}

// WithTLS enables TLS with the given configuration.
func WithTLS(cfg *tls.Config) Option {
	return func(o *options) {
		o.tlsConfig = cfg
	}
}

// WithPoolSize sets the maximum number of connections in the pool.
func WithPoolSize(size int) Option {
	return func(o *options) {
		o.poolSize = size
	}
}

// WithTimeout sets the connection and command timeout.
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		o.timeout = d
	}
}

// WithRetry configures retry behavior for failed commands.
func WithRetry(maxRetries int, backoff time.Duration) Option {
	return func(o *options) {
		o.maxRetries = maxRetries
		o.retryBackoff = backoff
	}
}
