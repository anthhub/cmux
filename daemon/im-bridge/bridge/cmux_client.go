package bridge

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// CmuxClient communicates with a cmux instance via its Unix socket (JSON-RPC v2).
type CmuxClient struct {
	socketPath string
	conn       net.Conn
	reader     *bufio.Reader
	requestID  atomic.Int64
	mu         sync.Mutex
}

// jsonRPCRequest is the JSON-RPC 2.0 request format.
type jsonRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      int64       `json:"id"`
}

// jsonRPCResponse is the JSON-RPC 2.0 response format.
type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// WorkspaceInfo represents a cmux workspace.
type WorkspaceInfo struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Title string `json:"title"`
	Ref   string `json:"ref"`
}

// SurfaceInfo represents a cmux surface (terminal/browser panel).
type SurfaceInfo struct {
	ID    string `json:"id"`
	Ref   string `json:"ref"`
	Type  string `json:"type"`
	Title string `json:"title"`
}

// API response wrappers (cmux returns nested structures)
type workspaceCreateResult struct {
	WorkspaceID  string `json:"workspace_id"`
	WorkspaceRef string `json:"workspace_ref"`
}

type workspaceListResult struct {
	Workspaces []WorkspaceInfo `json:"workspaces"`
}

type surfaceListResult struct {
	Surfaces    []SurfaceInfo `json:"surfaces"`
	WorkspaceID string        `json:"workspace_id"`
}

// NewCmuxClient creates a new client. If socketPath is empty, auto-discovers.
func NewCmuxClient(socketPath string) *CmuxClient {
	if socketPath == "" {
		socketPath = discoverSocketPath()
	}
	return &CmuxClient{socketPath: socketPath}
}

// Connect establishes connection to the cmux socket.
func (c *CmuxClient) Connect() error {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("failed to connect to cmux socket %s: %w", c.socketPath, err)
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	return nil
}

// Close closes the socket connection.
func (c *CmuxClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Call sends a JSON-RPC v2 request and returns the result.
func (c *CmuxClient) Call(method string, params interface{}) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		if err := c.Connect(); err != nil {
			return nil, err
		}
	}

	id := c.requestID.Add(1)
	req := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send request (newline-delimited)
	data = append(data, '\n')
	if _, err := c.conn.Write(data); err != nil {
		c.conn = nil // reset connection on write error
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Read response (may be large, read until newline)
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		c.conn = nil
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	line = bytes.TrimSpace(line)

	var resp jsonRPCResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("cmux error [%s]: %s", resp.Error.Code, resp.Error.Message)
	}

	return resp.Result, nil
}

// Ping tests the connection.
func (c *CmuxClient) Ping() error {
	_, err := c.Call("system.ping", nil)
	return err
}

// CreateWorkspace creates a new workspace.
func (c *CmuxClient) CreateWorkspace(name string) (*WorkspaceInfo, error) {
	params := map[string]interface{}{}
	if name != "" {
		params["name"] = name
	}
	result, err := c.Call("workspace.create", params)
	if err != nil {
		return nil, err
	}
	var resp workspaceCreateResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse workspace.create response: %w", err)
	}
	return &WorkspaceInfo{
		ID:  resp.WorkspaceID,
		Ref: resp.WorkspaceRef,
	}, nil
}

// ListWorkspaces returns all workspaces.
func (c *CmuxClient) ListWorkspaces() ([]WorkspaceInfo, error) {
	result, err := c.Call("workspace.list", nil)
	if err != nil {
		return nil, err
	}
	var resp workspaceListResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse workspace.list response: %w", err)
	}
	return resp.Workspaces, nil
}

// ListSurfaces returns surfaces in a workspace.
func (c *CmuxClient) ListSurfaces(workspaceID string) ([]SurfaceInfo, error) {
	params := map[string]interface{}{}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	result, err := c.Call("surface.list", params)
	if err != nil {
		return nil, err
	}
	var resp surfaceListResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse surface.list response: %w", err)
	}
	return resp.Surfaces, nil
}

// CreateSurface creates a new surface (tab) in a workspace.
func (c *CmuxClient) CreateSurface(workspaceID string) (*SurfaceInfo, error) {
	params := map[string]interface{}{
		"workspace_id": workspaceID,
	}
	result, err := c.Call("surface.create", params)
	if err != nil {
		return nil, err
	}
	var s SurfaceInfo
	if err := json.Unmarshal(result, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// SendText sends text to a terminal surface.
func (c *CmuxClient) SendText(surfaceID, text string) error {
	_, err := c.Call("surface.send_text", map[string]interface{}{
		"id":   surfaceID,
		"text": text,
	})
	return err
}

// readTextResult is the response from surface.read_text.
type readTextResult struct {
	Text      string `json:"text"`
	SurfaceID string `json:"surface_id"`
}

// ReadText reads the current screen content of a surface.
func (c *CmuxClient) ReadText(surfaceID string) (string, error) {
	result, err := c.Call("surface.read_text", map[string]interface{}{
		"id": surfaceID,
	})
	if err != nil {
		return "", err
	}
	var resp readTextResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return "", fmt.Errorf("failed to parse read_text response: %w", err)
	}
	return resp.Text, nil
}

// FocusSurface focuses a specific surface.
func (c *CmuxClient) FocusSurface(surfaceID string) error {
	_, err := c.Call("surface.focus", map[string]interface{}{
		"id": surfaceID,
	})
	return err
}

// discoverSocketPath finds the cmux socket path.
func discoverSocketPath() string {
	// 1. Environment variable
	if path := os.Getenv("CMUX_SOCKET"); path != "" {
		return path
	}
	if path := os.Getenv("CMUX_SOCKET_PATH"); path != "" {
		return path
	}

	// 2. Stable default path
	home, _ := os.UserHomeDir()
	stablePath := filepath.Join(home, "Library", "Application Support", "cmux", "cmux.sock")
	if _, err := os.Stat(stablePath); err == nil {
		return stablePath
	}

	// 3. Fallback
	return "/tmp/cmux.sock"
}
