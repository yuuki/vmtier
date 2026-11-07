package catalog

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// PostgreSQLIdleTimeout bounds each blocked transport read/write, including
// lib/pq's separate cancellation connection. It is not a whole-operation SLO:
// progressing transfers, retries, and multiple statements can take longer.
const PostgreSQLIdleTimeout = 5 * time.Second

type postgresDialer struct {
	timeout time.Duration
}

func (d postgresDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d postgresDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d postgresDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: d.timeout, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &postgresDeadlineConn{Conn: conn, idle: d.timeout}, nil
}

// postgresDeadlineConn retains explicit deadlines supplied by the driver while
// adding a fresh idle deadline for each I/O. An idle pooled connection therefore
// does not inherit an already expired idle deadline when reused. Timing out
// closes the socket, preventing a half-read protocol stream from being reused.
type postgresDeadlineConn struct {
	net.Conn
	idle  time.Duration
	mu    sync.Mutex
	read  time.Time
	write time.Time
}

func (c *postgresDeadlineConn) deadline(explicit time.Time) time.Time {
	limit := time.Now().Add(c.idle)
	if !explicit.IsZero() && explicit.Before(limit) {
		return explicit
	}
	return limit
}

func (c *postgresDeadlineConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	err := c.Conn.SetReadDeadline(c.deadline(c.read))
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}
	n, err := c.Conn.Read(b)
	c.closeOnTimeout(err)
	return n, err
}

func (c *postgresDeadlineConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	err := c.Conn.SetWriteDeadline(c.deadline(c.write))
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}
	n, err := c.Conn.Write(b)
	c.closeOnTimeout(err)
	return n, err
}

func (c *postgresDeadlineConn) closeOnTimeout(err error) {
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		_ = c.Conn.Close()
	}
}

func (c *postgresDeadlineConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read, c.write = t, t
	return c.Conn.SetDeadline(c.deadline(t))
}

func (c *postgresDeadlineConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read = t
	return c.Conn.SetReadDeadline(c.deadline(t))
}

func (c *postgresDeadlineConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.write = t
	return c.Conn.SetWriteDeadline(c.deadline(t))
}
