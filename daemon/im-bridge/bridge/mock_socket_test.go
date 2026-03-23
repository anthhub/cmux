package bridge

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"testing"
)

func makeShortUnixSocketPath(t *testing.T) string {
	t.Helper()
	// Use /tmp directly to avoid long paths from t.TempDir() that exceed
	// Unix socket path length limits (108 bytes on macOS).
	f, err := os.CreateTemp("/tmp", "cmux-test-*.sock")
	if err != nil {
		t.Fatalf("create temp socket path: %v", err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	t.Cleanup(func() { os.Remove(path) })
	return path
}

// startMockCmuxSocket starts a Unix socket that handles JSON-RPC v2.
// handler receives method and params, returns result JSON or error.
// Returns socket path.
func startMockCmuxSocket(t *testing.T, handler func(method string, params json.RawMessage) (json.RawMessage, error)) string {
	t.Helper()
	sockPath := makeShortUnixSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleMockConnection(conn, handler)
		}
	}()
	return sockPath
}

func handleMockConnection(conn net.Conn, handler func(string, json.RawMessage) (json.RawMessage, error)) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
			ID      int64           `json:"id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		result, handlerErr := handler(req.Method, req.Params)
		var resp []byte
		if handlerErr != nil {
			resp, _ = json.Marshal(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"error":   map[string]interface{}{"code": "error", "message": handlerErr.Error()},
			})
		} else {
			resp, _ = json.Marshal(map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  json.RawMessage(result),
			})
		}
		resp = append(resp, '\n')
		conn.Write(resp)
	}
}
