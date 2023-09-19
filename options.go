package taskqueue

import (
	"context"
	"crypto/tls"
	"net/url"
	"time"
)

type Options struct {
	Host      string
	Username  string
	Password  string
	TLSConfig *tls.Config
	URI       string
	Context   context.Context
	Timeout   time.Duration
	NoRetry   bool
}

type Option func(*Options)

// WithHost sets the Redis connection host.
func WithHost(host string) Option {
	return func(opts *Options) {
		opts.Host = host
	}
}

// WithUsername sets the username to use when authenticating to Redis.
func WithUsername(username string) Option {
	return func(opts *Options) {
		opts.Username = username
	}
}

// WithPassword sets the password to use when authenticating to Redis.
func WithPassword(password string) Option {
	return func(opts *Options) {
		opts.Password = password
	}
}

// WithTLSConfig sets the TLS configuration of the Redis instance.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(opts *Options) {
		opts.TLSConfig = tlsConfig
	}
}

// WithURI sets the Redis URI connection string.
func WithURI(uri string) Option {
	return func(opts *Options) {
		opts.URI = uri
	}
}

// WithContext sets the Redis context object.
func WithContext(ctx context.Context) Option {
	return func(opts *Options) {
		opts.Context = ctx
	}
}

// WithTimeout sets the Redis read/write timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		opts.Timeout = timeout
	}
}

// WithNoRetry enables or disables task retry, which re-adds the task back to the queue if an error
// occurs while retrieving it.
func WithNoRetry() Option {
	return func(opts *Options) {
		opts.NoRetry = true
	}
}

func getOptions(setters []Option) (*Options, error) {
	options := &Options{
		Host:      "localhost:6379",
		TLSConfig: nil,
		Context:   context.Background(),
		Timeout:   time.Second * 3,
		NoRetry:   false,
	}
	for _, setter := range setters {
		setter(options)
	}
	if options.URI != "" {
		parsed, err := url.Parse(options.URI)
		if err != nil {
			return nil, err
		}
		username := parsed.User.Username()
		password, _ := parsed.User.Password()
		WithHost(parsed.Host)(options)
		WithUsername(username)(options)
		WithPassword(password)(options)
	}
	return options, nil
}
