package bridge

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CmuxClient communicates with a cmux instance via its Unix socket (JSON-RPC v2).
type CmuxClient struct {
	socketPath           string
	conn                 net.Conn
	reader               *bufio.Reader
	requestID            atomic.Int64
	mu                   sync.Mutex
	lastReconnectAttempt time.Time
	reconnectBackoff     time.Duration
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
	ID       string `json:"id"`
	Title    string `json:"title"`
	Ref      string `json:"ref"`
	Index    int    `json:"index"`
	Selected bool   `json:"selected"`
}

// WindowInfo represents a cmux window.
type WindowInfo struct {
	ID                   string `json:"id"`
	Ref                  string `json:"ref"`
	Index                int    `json:"index"`
	Key                  bool   `json:"key"`
	Visible              bool   `json:"visible"`
	WorkspaceCount       int    `json:"workspace_count"`
	SelectedWorkspaceID  string `json:"selected_workspace_id"`
	SelectedWorkspaceRef string `json:"selected_workspace_ref"`
}

// SurfaceInfo represents a cmux surface (terminal/browser panel).
type SurfaceInfo struct {
	ID      string `json:"id"`
	Ref     string `json:"ref"`
	Type    string `json:"type"`
	Title   string `json:"title"`
	Focused bool   `json:"focused"`
	Index   int    `json:"index"`
}

// PaneInfo represents a pane inside a workspace.
type PaneInfo struct {
	ID           string `json:"id"`
	Ref          string `json:"ref"`
	Focused      bool   `json:"focused"`
	SurfaceCount int    `json:"surface_count"`
}

// API response wrappers (cmux returns nested structures)
type workspaceCreateResult struct {
	WorkspaceID  string `json:"workspace_id"`
	WorkspaceRef string `json:"workspace_ref"`
}

type windowListResult struct {
	Windows []WindowInfo `json:"windows"`
}

type workspaceListResult struct {
	WindowID   string          `json:"window_id"`
	WindowRef  string          `json:"window_ref"`
	Workspaces []WorkspaceInfo `json:"workspaces"`
}

// WorkspaceListing ties a workspace to the window it was discovered in.
type WorkspaceListing struct {
	WindowID  string
	WindowRef string
	Workspace WorkspaceInfo
}

type surfaceCreateResult struct {
	SurfaceID  string `json:"surface_id"`
	SurfaceRef string `json:"surface_ref"`
	PaneID     string `json:"pane_id"`
	PaneRef    string `json:"pane_ref"`
}

type surfaceListResult struct {
	Surfaces    []SurfaceInfo `json:"surfaces"`
	WorkspaceID string        `json:"workspace_id"`
}

type paneListResult struct {
	Panes       []PaneInfo `json:"panes"`
	WorkspaceID string     `json:"workspace_id"`
}

type readTextResult struct {
	Text      string `json:"text"`
	SurfaceID string `json:"surface_id"`
}

type browserScreenshotResult struct {
	WorkspaceID string `json:"workspace_id"`
	SurfaceID   string `json:"surface_id"`
	Path        string `json:"path"`
	URL         string `json:"url"`
	PNGBase64   string `json:"png_base64"`
}

type panelSnapshotResult struct {
	SurfaceID     string `json:"surface_id"`
	ChangedPixels int    `json:"changed_pixels"`
	Width         int    `json:"width"`
	Height        int    `json:"height"`
	Path          string `json:"path"`
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectLocked()
}

// Close closes the socket connection.
func (c *CmuxClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	if c.conn != nil {
		err = c.conn.Close()
	}
	c.conn = nil
	c.reader = nil
	return err
}

// Call sends a JSON-RPC v2 request and returns the result.
func (c *CmuxClient) Call(method string, params interface{}) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.connectLocked(); err != nil {
		return nil, err
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

	data = append(data, '\n')
	if _, err := c.conn.Write(data); err != nil {
		c.resetLocked()
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		c.resetLocked()
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

func (c *CmuxClient) connectLocked() error {
	if c.conn != nil {
		return nil
	}

	if !c.lastReconnectAttempt.IsZero() && c.reconnectBackoff > 0 {
		if wait := time.Until(c.lastReconnectAttempt.Add(c.reconnectBackoff)); wait > 0 {
			time.Sleep(wait)
		}
	}

	conn, err := net.Dial("unix", c.socketPath)
	c.lastReconnectAttempt = time.Now()
	if err != nil {
		if c.reconnectBackoff == 0 {
			c.reconnectBackoff = time.Second
		} else {
			c.reconnectBackoff *= 2
			if c.reconnectBackoff > 30*time.Second {
				c.reconnectBackoff = 30 * time.Second
			}
		}
		return fmt.Errorf("failed to connect to cmux socket %s: %w", c.socketPath, err)
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.reconnectBackoff = 0
	return nil
}

func (c *CmuxClient) resetLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.reader = nil
	if c.reconnectBackoff == 0 {
		c.reconnectBackoff = time.Second
	} else {
		c.reconnectBackoff *= 2
		if c.reconnectBackoff > 30*time.Second {
			c.reconnectBackoff = 30 * time.Second
		}
	}
	c.lastReconnectAttempt = time.Now()
}

// Ping tests the connection.
func (c *CmuxClient) Ping() error {
	_, err := c.Call("system.ping", nil)
	return err
}

// CreateWorkspace creates a new workspace.
func (c *CmuxClient) CreateWorkspace(name string) (*WorkspaceInfo, error) {
	result, err := c.Call("workspace.create", map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	var resp workspaceCreateResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse workspace.create response: %w", err)
	}
	workspace := &WorkspaceInfo{
		ID:  resp.WorkspaceID,
		Ref: resp.WorkspaceRef,
	}
	if name != "" {
		if err := c.RenameWorkspace(workspace.ID, name); err != nil {
			return nil, err
		}
		workspace.Title = name
	}
	return workspace, nil
}

// ListWorkspaces returns all workspaces.
func (c *CmuxClient) ListWorkspaces() ([]WorkspaceInfo, error) {
	return c.ListWorkspacesForWindow("")
}

// ListWorkspacesForWindow returns workspaces for a specific window.
func (c *CmuxClient) ListWorkspacesForWindow(windowID string) ([]WorkspaceInfo, error) {
	params := map[string]interface{}{}
	if windowID != "" {
		params["window_id"] = windowID
	}
	result, err := c.Call("workspace.list", params)
	if err != nil {
		return nil, err
	}
	var resp workspaceListResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse workspace.list response: %w", err)
	}
	return resp.Workspaces, nil
}

// ListWindows returns all windows and their selected workspace context.
func (c *CmuxClient) ListWindows() ([]WindowInfo, error) {
	result, err := c.Call("window.list", nil)
	if err != nil {
		return nil, err
	}
	var resp windowListResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse window.list response: %w", err)
	}
	return resp.Windows, nil
}

// ListAllWorkspaces discovers workspaces across every window.
func (c *CmuxClient) ListAllWorkspaces() ([]WorkspaceListing, error) {
	windows, err := c.ListWindows()
	if err != nil {
		return nil, err
	}

	listings := make([]WorkspaceListing, 0)
	for _, window := range windows {
		workspaces, err := c.ListWorkspacesForWindow(window.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to list workspaces for window %s: %w", window.ID, err)
		}
		for _, workspace := range workspaces {
			listings = append(listings, WorkspaceListing{
				WindowID:  window.ID,
				WindowRef: window.Ref,
				Workspace: workspace,
			})
		}
	}

	return listings, nil
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

// ListPanes returns panes in a workspace.
func (c *CmuxClient) ListPanes(workspaceID string) ([]PaneInfo, error) {
	params := map[string]interface{}{}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	result, err := c.Call("pane.list", params)
	if err != nil {
		return nil, err
	}
	var resp paneListResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse pane.list response: %w", err)
	}
	return resp.Panes, nil
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
	var resp surfaceCreateResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse surface.create response: %w", err)
	}
	return &SurfaceInfo{
		ID:   resp.SurfaceID,
		Ref:  resp.SurfaceRef,
		Type: "terminal",
	}, nil
}

// SendText sends text to a terminal surface.
func (c *CmuxClient) SendText(workspaceID, surfaceID, text string) error {
	params := map[string]interface{}{
		"surface_id": surfaceID,
		"text":       text,
	}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	_, err := c.Call("surface.send_text", params)
	return err
}

// SendKey sends a key chord to a surface, e.g. "ctrl+c".
func (c *CmuxClient) SendKey(workspaceID, surfaceID, key string) error {
	params := map[string]interface{}{
		"surface_id": surfaceID,
		"key":        key,
	}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	_, err := c.Call("surface.send_key", params)
	return err
}

// ReadText reads the current screen content of a surface.
func (c *CmuxClient) ReadText(workspaceID, surfaceID string) (string, error) {
	return c.ReadTextLines(workspaceID, surfaceID, 0)
}

// ReadTextLines reads the current screen content of a surface with optional scrollback lines.
func (c *CmuxClient) ReadTextLines(workspaceID, surfaceID string, lines int) (string, error) {
	params := map[string]interface{}{
		"surface_id": surfaceID,
	}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	if lines > 0 {
		params["lines"] = lines
		params["scrollback"] = true
	}
	result, err := c.Call("surface.read_text", params)
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
func (c *CmuxClient) FocusSurface(workspaceID, surfaceID string) error {
	params := map[string]interface{}{
		"surface_id": surfaceID,
	}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	_, err := c.Call("surface.focus", params)
	return err
}

// CloseSurface closes a specific surface.
func (c *CmuxClient) CloseSurface(workspaceID, surfaceID string) error {
	params := map[string]interface{}{
		"surface_id": surfaceID,
	}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	_, err := c.Call("surface.close", params)
	return err
}

// SplitSurface splits the current surface and returns the newly created surface.
func (c *CmuxClient) SplitSurface(workspaceID, surfaceID, direction string) (*SurfaceInfo, error) {
	params := map[string]interface{}{
		"surface_id": surfaceID,
		"direction":  direction,
	}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	result, err := c.Call("surface.split", params)
	if err != nil {
		return nil, err
	}
	var resp surfaceCreateResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse surface.split response: %w", err)
	}
	return &SurfaceInfo{
		ID:   resp.SurfaceID,
		Ref:  resp.SurfaceRef,
		Type: "terminal",
	}, nil
}

// RenameWorkspace sets a stable title for an IM-managed workspace.
func (c *CmuxClient) RenameWorkspace(workspaceID, title string) error {
	_, err := c.Call("workspace.rename", map[string]interface{}{
		"workspace_id": workspaceID,
		"title":        title,
	})
	return err
}

// SelectWorkspace switches the active workspace.
func (c *CmuxClient) SelectWorkspace(workspaceID string) error {
	_, err := c.Call("workspace.select", map[string]interface{}{
		"workspace_id": workspaceID,
	})
	return err
}

// RenameSurface sets a stable title for a surface so IM sessions can be recovered by name.
func (c *CmuxClient) RenameSurface(workspaceID, surfaceID, title string) error {
	params := map[string]interface{}{
		"surface_id": surfaceID,
		"action":     "rename",
		"title":      title,
	}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	_, err := c.Call("tab.action", params)
	if err == nil || workspaceID == "" || !strings.Contains(err.Error(), "Tab not found") {
		return err
	}

	delete(params, "workspace_id")
	_, retryErr := c.Call("tab.action", params)
	return retryErr
}

// BrowserScreenshot captures a browser surface.
func (c *CmuxClient) BrowserScreenshot(workspaceID, surfaceID string) (*browserScreenshotResult, error) {
	params := map[string]interface{}{
		"surface_id": surfaceID,
	}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	result, err := c.Call("browser.screenshot", params)
	if err != nil {
		return nil, err
	}
	var resp browserScreenshotResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse browser.screenshot response: %w", err)
	}
	return &resp, nil
}

// OpenBrowserSplit opens a browser split anchored to the current surface/workspace.
func (c *CmuxClient) OpenBrowserSplit(workspaceID, surfaceID, url string) (*SurfaceInfo, error) {
	params := map[string]interface{}{}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	if surfaceID != "" {
		params["surface_id"] = surfaceID
	}
	if url != "" {
		params["url"] = url
	}
	result, err := c.Call("browser.open_split", params)
	if err != nil {
		return nil, err
	}
	var resp surfaceCreateResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse browser.open_split response: %w", err)
	}
	return &SurfaceInfo{
		ID:   resp.SurfaceID,
		Ref:  resp.SurfaceRef,
		Type: "browser",
	}, nil
}

// NavigateBrowser navigates an existing browser surface.
func (c *CmuxClient) NavigateBrowser(surfaceID, url string) error {
	_, err := c.Call("browser.navigate", map[string]interface{}{
		"surface_id": surfaceID,
		"url":        url,
	})
	return err
}

// SystemTree returns the raw system.tree payload.
func (c *CmuxClient) SystemTree(workspaceID string) (json.RawMessage, error) {
	params := map[string]interface{}{}
	if workspaceID != "" {
		params["workspace_id"] = workspaceID
	}
	return c.Call("system.tree", params)
}

// DebugPanelSnapshot captures a surface image in DEBUG builds.
func (c *CmuxClient) DebugPanelSnapshot(surfaceID, label string) (*panelSnapshotResult, error) {
	params := map[string]interface{}{
		"surface_id": surfaceID,
	}
	if label != "" {
		params["label"] = label
	}
	result, err := c.Call("debug.panel_snapshot", params)
	if err != nil {
		return nil, err
	}
	var resp panelSnapshotResult
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse debug.panel_snapshot response: %w", err)
	}
	return &resp, nil
}

// discoverSocketPath finds the cmux socket path.
func discoverSocketPath() string {
	if path := os.Getenv("CMUX_SOCKET"); path != "" {
		return path
	}
	if path := os.Getenv("CMUX_SOCKET_PATH"); path != "" {
		return path
	}

	home, _ := os.UserHomeDir()
	stablePath := filepath.Join(home, "Library", "Application Support", "cmux", "cmux.sock")
	if _, err := os.Stat(stablePath); err == nil {
		return stablePath
	}

	return "/tmp/cmux.sock"
}
