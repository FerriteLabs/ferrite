package ferrite

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Conn represents a single connection to a Ferrite server.
type Conn struct {
	netConn net.Conn
	writer  *respWriter
	reader  *respReader
	created time.Time
}

func newConn(addr string, timeout time.Duration, tlsConfig *tls.Config) (*Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	var netConn net.Conn
	var err error

	if tlsConfig != nil {
		netConn, err = tls.DialWithDialer(&dialer, "tcp", addr, tlsConfig)
	} else {
		netConn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("ferrite: connect to %s: %w", addr, err)
	}

	return &Conn{
		netConn: netConn,
		writer:  newRESPWriter(netConn),
		reader:  newRESPReader(netConn),
		created: time.Now(),
	}, nil
}

func (c *Conn) close() error {
	return c.netConn.Close()
}

func (c *Conn) setDeadline(d time.Time) error {
	return c.netConn.SetDeadline(d)
}

// Pool manages a pool of connections to a Ferrite server.
type Pool struct {
	mu        sync.Mutex
	addr      string
	opts      *options
	conns     chan *Conn
	size      int
	maxSize   int
	closed    bool
}

func newPool(addr string, opts *options) (*Pool, error) {
	p := &Pool{
		addr:    addr,
		opts:    opts,
		conns:   make(chan *Conn, opts.poolSize),
		maxSize: opts.poolSize,
	}

	// Pre-create one connection to validate connectivity
	conn, err := p.dial()
	if err != nil {
		return nil, err
	}
	p.conns <- conn
	p.size = 1

	return p, nil
}

func (p *Pool) dial() (*Conn, error) {
	return newConn(p.addr, p.opts.timeout, p.opts.tlsConfig)
}

// Get retrieves a connection from the pool, creating a new one if needed.
func (p *Pool) Get() (*Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("ferrite: pool is closed")
	}
	p.mu.Unlock()

	// Try to get an idle connection
	select {
	case conn := <-p.conns:
		return conn, nil
	default:
	}

	// Create a new connection if under the limit
	p.mu.Lock()
	if p.size < p.maxSize {
		p.size++
		p.mu.Unlock()
		conn, err := p.dial()
		if err != nil {
			p.mu.Lock()
			p.size--
			p.mu.Unlock()
			return nil, err
		}
		return conn, nil
	}
	p.mu.Unlock()

	// Wait for an available connection
	conn := <-p.conns
	return conn, nil
}

// Put returns a connection to the pool.
func (p *Pool) Put(conn *Conn) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.close()
		return
	}
	p.mu.Unlock()

	select {
	case p.conns <- conn:
	default:
		// Pool is full, discard the connection
		conn.close()
		p.mu.Lock()
		p.size--
		p.mu.Unlock()
	}
}

// Close closes all connections in the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	close(p.conns)
	var firstErr error
	for conn := range p.conns {
		if err := conn.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
