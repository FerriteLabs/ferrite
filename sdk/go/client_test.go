package ferrite

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// --- Mock TCP server helper ---

func startMockServer(t *testing.T, handler func(conn net.Conn)) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock server: %v", err)
	}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handler(conn)
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

// --- RESP Writer Tests ---

func TestRespWriteCommand(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "SET command",
			args: []string{"SET", "key", "value"},
			want: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		},
		{
			name: "GET command",
			args: []string{"GET", "mykey"},
			want: "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n",
		},
		{
			name: "PING command",
			args: []string{"PING"},
			want: "*1\r\n$4\r\nPING\r\n",
		},
		{
			name: "empty argument",
			args: []string{"SET", "key", ""},
			want: "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := newRESPWriter(&buf)
			if err := w.WriteCommand(tt.args...); err != nil {
				t.Fatalf("WriteCommand() error = %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("WriteCommand() =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
}

// --- RESP Reader Tests ---

func TestRespReadSimpleString(t *testing.T) {
	r := newRESPReader(strings.NewReader("+OK\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue() error = %v", err)
	}
	if val != "OK" {
		t.Errorf("ReadValue() = %q, want %q", val, "OK")
	}
}

func TestRespReadError(t *testing.T) {
	r := newRESPReader(strings.NewReader("-ERR message\r\n"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("ReadValue() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "ERR message") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "ERR message")
	}
}

func TestRespReadInteger(t *testing.T) {
	r := newRESPReader(strings.NewReader(":1000\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue() error = %v", err)
	}
	n, ok := val.(int64)
	if !ok {
		t.Fatalf("ReadValue() type = %T, want int64", val)
	}
	if n != 1000 {
		t.Errorf("ReadValue() = %d, want %d", n, 1000)
	}
}

func TestRespReadBulkString(t *testing.T) {
	r := newRESPReader(strings.NewReader("$5\r\nhello\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue() error = %v", err)
	}
	if val != "hello" {
		t.Errorf("ReadValue() = %q, want %q", val, "hello")
	}
}

func TestRespReadNull(t *testing.T) {
	r := newRESPReader(strings.NewReader("$-1\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue() error = %v", err)
	}
	if val != nil {
		t.Errorf("ReadValue() = %v, want nil", val)
	}
}

func TestRespReadArray(t *testing.T) {
	r := newRESPReader(strings.NewReader("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue() error = %v", err)
	}
	arr, ok := val.([]interface{})
	if !ok {
		t.Fatalf("ReadValue() type = %T, want []interface{}", val)
	}
	if len(arr) != 2 {
		t.Fatalf("array length = %d, want 2", len(arr))
	}
	if arr[0] != "foo" {
		t.Errorf("arr[0] = %q, want %q", arr[0], "foo")
	}
	if arr[1] != "bar" {
		t.Errorf("arr[1] = %q, want %q", arr[1], "bar")
	}
}

// --- Options Tests ---

func TestOptionsDefaults(t *testing.T) {
	opts := defaultOptions()

	if opts.poolSize != 10 {
		t.Errorf("poolSize = %d, want 10", opts.poolSize)
	}
	if opts.timeout != 5*time.Second {
		t.Errorf("timeout = %v, want 5s", opts.timeout)
	}
	if opts.maxRetries != 3 {
		t.Errorf("maxRetries = %d, want 3", opts.maxRetries)
	}
	if opts.retryBackoff != 100*time.Millisecond {
		t.Errorf("retryBackoff = %v, want 100ms", opts.retryBackoff)
	}
	if opts.db != 0 {
		t.Errorf("db = %d, want 0", opts.db)
	}
	if opts.password != "" {
		t.Errorf("password = %q, want empty", opts.password)
	}
	if opts.tlsConfig != nil {
		t.Error("tlsConfig should be nil by default")
	}
}

func TestOptionsFunctional(t *testing.T) {
	opts := defaultOptions()
	WithPassword("secret")(opts)
	WithDB(2)(opts)
	WithPoolSize(20)(opts)
	WithTimeout(10 * time.Second)(opts)
	WithRetry(5, 200*time.Millisecond)(opts)

	if opts.password != "secret" {
		t.Errorf("password = %q, want %q", opts.password, "secret")
	}
	if opts.db != 2 {
		t.Errorf("db = %d, want 2", opts.db)
	}
	if opts.poolSize != 20 {
		t.Errorf("poolSize = %d, want 20", opts.poolSize)
	}
	if opts.timeout != 10*time.Second {
		t.Errorf("timeout = %v, want 10s", opts.timeout)
	}
	if opts.maxRetries != 5 {
		t.Errorf("maxRetries = %d, want 5", opts.maxRetries)
	}
	if opts.retryBackoff != 200*time.Millisecond {
		t.Errorf("retryBackoff = %v, want 200ms", opts.retryBackoff)
	}
}

// --- Pool Tests ---

func TestPoolCreateClose(t *testing.T) {
	addr, closeFn := startMockServer(t, func(conn net.Conn) {
		// Keep connection open until closed by client
		buf := make([]byte, 1024)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				return
			}
		}
	})
	defer closeFn()

	opts := defaultOptions()
	opts.poolSize = 2

	pool, err := newPool(addr, opts)
	if err != nil {
		t.Fatalf("newPool() error = %v", err)
	}

	// Should have one pre-created connection
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("pool.Get() error = %v", err)
	}
	if conn == nil {
		t.Fatal("pool.Get() returned nil connection")
	}

	pool.Put(conn)

	if err := pool.Close(); err != nil {
		t.Fatalf("pool.Close() error = %v", err)
	}

	// After close, Get should fail
	_, err = pool.Get()
	if err == nil {
		t.Error("pool.Get() after Close() should return error")
	}
}

// --- Client Integration Tests (with mock server) ---

func TestClientPing(t *testing.T) {
	addr, closeFn := startMockServer(t, func(conn net.Conn) {
		reader := newRESPReader(conn)
		writer := newRESPWriter(conn)
		for {
			_, err := reader.ReadValue()
			if err != nil {
				return
			}
			// Respond with +PONG
			conn.Write([]byte("+PONG\r\n"))
			_ = writer
		}
	})
	defer closeFn()

	client, err := NewClient(addr, WithRetry(0, 0))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Errorf("Ping() error = %v", err)
	}
}

func TestClientSetGet(t *testing.T) {
	addr, closeFn := startMockServer(t, func(conn net.Conn) {
		reader := newRESPReader(conn)
		for {
			val, err := reader.ReadValue()
			if err != nil {
				return
			}
			arr, ok := val.([]interface{})
			if !ok || len(arr) == 0 {
				conn.Write([]byte("-ERR invalid command\r\n"))
				continue
			}
			cmd, _ := arr[0].(string)
			switch strings.ToUpper(cmd) {
			case "SET":
				conn.Write([]byte("+OK\r\n"))
			case "GET":
				conn.Write([]byte("$5\r\nhello\r\n"))
			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	})
	defer closeFn()

	client, err := NewClient(addr, WithRetry(0, 0))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test SET
	if err := client.Set(ctx, "key", "hello", 0); err != nil {
		t.Errorf("Set() error = %v", err)
	}

	// Test GET
	val, err := client.Get(ctx, "key")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}
	if val != "hello" {
		t.Errorf("Get() = %q, want %q", val, "hello")
	}
}
