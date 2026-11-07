package catalog

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestPostgresTransportBoundsBlackholedIO(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			conn := &postgresDeadlineConn{Conn: client, idle: 30 * time.Millisecond}
			defer conn.Close()
			start := time.Now()
			var err error
			if operation == "read" {
				_, err = conn.Read(make([]byte, 1))
			} else {
				_, err = conn.Write([]byte{1})
			}
			var networkError net.Error
			if !errors.As(err, &networkError) || !networkError.Timeout() {
				t.Fatalf("blackholed %s did not time out: %v", operation, err)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("blackholed %s took %s", operation, elapsed)
			}
			if _, err := server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
				t.Fatalf("timed-out socket was not closed: %v", err)
			}
		})
	}
}

func TestPostgresTransportPreservesExplicitDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	conn := &postgresDeadlineConn{Conn: client, idle: time.Second}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := conn.Read(make([]byte, 1))
	var networkError net.Error
	if !errors.As(err, &networkError) || !networkError.Timeout() || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("read ignored tighter driver deadline: %v", err)
	}
}

func TestPostgresTransportRenewsIdleDeadlineOnReuse(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	conn := &postgresDeadlineConn{Conn: client, idle: 20 * time.Millisecond}
	defer conn.Close()
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	// A connection can be pooled longer than its last transport idle deadline.
	time.Sleep(40 * time.Millisecond)
	done := make(chan error, 1)
	go func() {
		_, err := server.Write([]byte{7})
		done <- err
	}()
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil || buf[0] != 7 {
		t.Fatalf("idle pooled connection retained expired deadline: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
