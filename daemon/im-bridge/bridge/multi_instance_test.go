package bridge

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// startMockSocket starts a minimal Unix socket server that responds to JSON-RPC pings.
// Returns the socket path and a cancel function to stop the server.
func startMockSocket(t *testing.T) (string, func()) {
	t.Helper()

	f, err := os.CreateTemp("", "cmux-test-*.sock")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", path, err)
	}

	stop := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stop:
					return
				default:
					return
				}
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					_ = buf[:n]
					// Always respond with a successful ping
					resp := `{"jsonrpc":"2.0","id":1,"result":{"ok":true}}` + "\n"
					c.Write([]byte(resp))
				}
			}(conn)
		}
	}()

	cancel := func() {
		close(stop)
		ln.Close()
		os.Remove(path)
	}
	return path, cancel
}

func TestNewInstanceManager(t *testing.T) {
	im := NewInstanceManager()
	if im == nil {
		t.Fatal("NewInstanceManager returned nil")
	}
	if im.instances == nil {
		t.Fatal("instances map is nil")
	}
	if im.defaultID != "" {
		t.Fatalf("expected empty defaultID, got %q", im.defaultID)
	}
}

func TestRegisterAndRoute(t *testing.T) {
	path, cancel := startMockSocket(t)
	defer cancel()

	im := NewInstanceManager()
	inst, err := im.Register(path)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if inst == nil {
		t.Fatal("Register returned nil instance")
	}
	if inst.SocketPath != path {
		t.Errorf("expected socket path %s, got %s", path, inst.SocketPath)
	}
	if !inst.Healthy {
		t.Error("expected instance to be healthy after registration")
	}

	client, err := im.Route(path)
	if err != nil {
		t.Fatalf("Route failed: %v", err)
	}
	if client == nil {
		t.Fatal("Route returned nil client")
	}
}

func TestRouteUnknownID(t *testing.T) {
	im := NewInstanceManager()
	_, err := im.Route("nonexistent-id")
	if err == nil {
		t.Fatal("expected error for unknown instance ID, got nil")
	}
}

func TestRouteUnhealthyInstance(t *testing.T) {
	path, cancel := startMockSocket(t)
	defer cancel()

	im := NewInstanceManager()
	inst, err := im.Register(path)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Mark instance as unhealthy
	im.mu.Lock()
	inst.Healthy = false
	im.mu.Unlock()

	_, err = im.Route(path)
	if err == nil {
		t.Fatal("expected error for unhealthy instance, got nil")
	}
}

func TestList(t *testing.T) {
	path1, cancel1 := startMockSocket(t)
	defer cancel1()
	path2, cancel2 := startMockSocket(t)
	defer cancel2()

	im := NewInstanceManager()
	if _, err := im.Register(path1); err != nil {
		t.Fatalf("Register path1 failed: %v", err)
	}
	if _, err := im.Register(path2); err != nil {
		t.Fatalf("Register path2 failed: %v", err)
	}

	list := im.List()
	if len(list) != 2 {
		t.Errorf("expected 2 instances, got %d", len(list))
	}
}

func TestSetDefaultAndDefault(t *testing.T) {
	path1, cancel1 := startMockSocket(t)
	defer cancel1()
	path2, cancel2 := startMockSocket(t)
	defer cancel2()

	im := NewInstanceManager()
	if _, err := im.Register(path1); err != nil {
		t.Fatalf("Register path1 failed: %v", err)
	}
	if _, err := im.Register(path2); err != nil {
		t.Fatalf("Register path2 failed: %v", err)
	}

	// Default should be path1 (first registered)
	client, err := im.Default()
	if err != nil {
		t.Fatalf("Default failed: %v", err)
	}
	if client == nil {
		t.Fatal("Default returned nil client")
	}

	// Switch default to path2
	if err := im.SetDefault(path2); err != nil {
		t.Fatalf("SetDefault failed: %v", err)
	}

	// Now default should point to path2's client
	im.mu.RLock()
	if im.defaultID != path2 {
		t.Errorf("expected defaultID %s, got %s", path2, im.defaultID)
	}
	im.mu.RUnlock()
}

func TestSetDefaultUnknown(t *testing.T) {
	im := NewInstanceManager()
	if err := im.SetDefault("no-such-id"); err == nil {
		t.Fatal("expected error for unknown ID, got nil")
	}
}

func TestConcurrentRegisterAndRoute(t *testing.T) {
	const n = 10
	paths := make([]string, n)
	cancels := make([]func(), n)
	for i := 0; i < n; i++ {
		p, c := startMockSocket(t)
		paths[i] = p
		cancels[i] = c
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	im := NewInstanceManager()
	var wg sync.WaitGroup

	// Register all sockets concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			im.Register(path) //nolint:errcheck
		}(paths[i])
	}
	wg.Wait()

	// Route concurrently
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			im.Route(path) //nolint:errcheck
		}(paths[i])
	}
	wg.Wait()
}

func TestStartHealthLoop(t *testing.T) {
	path, cancel := startMockSocket(t)
	defer cancel()

	im := NewInstanceManager()
	if _, err := im.Register(path); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	ctx, ctxCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer ctxCancel()

	im.StartHealthLoop(ctx, 50*time.Millisecond)

	// Wait for at least one health check cycle
	time.Sleep(120 * time.Millisecond)

	list := im.List()
	if len(list) == 0 {
		t.Fatal("expected at least one instance")
	}
	if !list[0].Healthy {
		t.Error("expected instance to remain healthy after health loop")
	}
}

func TestDiscoverSkipsRealSockets(t *testing.T) {
	// Discover against a system without any real cmux sockets running.
	// The call should not panic and should return without error (or a non-fatal error).
	im := NewInstanceManager()
	// We don't assert no error here because some CI/dev machines might have sockets.
	_ = im.Discover()
}
