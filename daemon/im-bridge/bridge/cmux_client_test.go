package bridge

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestCall_SendsCorrectJSONRPC(t *testing.T) {
	var captured struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		ID      int64           `json:"id"`
	}
	var mu sync.Mutex

	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		captured.Method = method
		captured.Params = params
		// We set jsonrpc and id from the raw request in the handler for simplicity;
		// the mock already parsed them. Just verify method/params here.
		mu.Unlock()
		return json.RawMessage(`{"ok":true}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	_, err := client.Call("test.method", map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if captured.Method != "test.method" {
		t.Errorf("method = %q, want %q", captured.Method, "test.method")
	}

	var p map[string]string
	if err := json.Unmarshal(captured.Params, &p); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if p["key"] != "value" {
		t.Errorf("params[key] = %q, want %q", p["key"], "value")
	}
}

func TestCall_ParsesSuccessResponse(t *testing.T) {
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"answer":42}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	result, err := client.Call("test.method", nil)
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}

	var parsed map[string]int
	if err := json.Unmarshal(result, &parsed); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if parsed["answer"] != 42 {
		t.Errorf("answer = %d, want 42", parsed["answer"])
	}
}

func TestCall_ParsesErrorResponse(t *testing.T) {
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("something went wrong")
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	_, err := client.Call("test.method", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "something went wrong") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "something went wrong")
	}
}

func TestFocusSurface_UsesSurfaceIdParam(t *testing.T) {
	var capturedParams json.RawMessage
	var mu sync.Mutex

	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		capturedParams = params
		mu.Unlock()
		return json.RawMessage(`{}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	err := client.FocusSurface("ws-1", "surf-123")
	if err != nil {
		t.Fatalf("FocusSurface failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	var p map[string]string
	if err := json.Unmarshal(capturedParams, &p); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if p["surface_id"] != "surf-123" {
		t.Errorf("surface_id = %q, want %q", p["surface_id"], "surf-123")
	}
	if p["workspace_id"] != "ws-1" {
		t.Errorf("workspace_id = %q, want %q", p["workspace_id"], "ws-1")
	}
}

func TestSendText_UsesSurfaceIdParam(t *testing.T) {
	var capturedParams json.RawMessage
	var mu sync.Mutex

	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		capturedParams = params
		mu.Unlock()
		return json.RawMessage(`{}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	err := client.SendText("ws-1", "surf-456", "hello world")
	if err != nil {
		t.Fatalf("SendText failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	var p map[string]string
	if err := json.Unmarshal(capturedParams, &p); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if p["surface_id"] != "surf-456" {
		t.Errorf("surface_id = %q, want %q", p["surface_id"], "surf-456")
	}
	if p["text"] != "hello world" {
		t.Errorf("text = %q, want %q", p["text"], "hello world")
	}
}

func TestReadText_UsesSurfaceIdParam(t *testing.T) {
	var capturedParams json.RawMessage
	var mu sync.Mutex

	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		capturedParams = params
		mu.Unlock()
		return json.RawMessage(`{"text":"screen content","surface_id":"surf-789"}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	text, err := client.ReadText("ws-1", "surf-789")
	if err != nil {
		t.Fatalf("ReadText failed: %v", err)
	}
	if text != "screen content" {
		t.Errorf("text = %q, want %q", text, "screen content")
	}

	mu.Lock()
	defer mu.Unlock()

	var p map[string]string
	if err := json.Unmarshal(capturedParams, &p); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if p["surface_id"] != "surf-789" {
		t.Errorf("surface_id = %q, want %q", p["surface_id"], "surf-789")
	}
}

func TestCreateSurface_ParsesCorrectFields(t *testing.T) {
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"surface_id":"s-1","surface_ref":"ref-1","pane_id":"p-1","pane_ref":"pref-1"}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	info, err := client.CreateSurface("ws-1")
	if err != nil {
		t.Fatalf("CreateSurface failed: %v", err)
	}
	if info.ID != "s-1" {
		t.Errorf("ID = %q, want %q", info.ID, "s-1")
	}
	if info.Ref != "ref-1" {
		t.Errorf("Ref = %q, want %q", info.Ref, "ref-1")
	}
	if info.Type != "terminal" {
		t.Errorf("Type = %q, want %q", info.Type, "terminal")
	}
}

func TestPing_Success(t *testing.T) {
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method != "system.ping" {
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
		return json.RawMessage(`{"pong":true}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	if err := client.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestCreateWorkspace_Success(t *testing.T) {
	callCount := 0
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		callCount++
		switch method {
		case "workspace.create":
			return json.RawMessage(`{"workspace_id":"ws-new","workspace_ref":"ref-new"}`), nil
		case "workspace.rename":
			return json.RawMessage(`{}`), nil
		default:
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	ws, err := client.CreateWorkspace("my-workspace")
	if err != nil {
		t.Fatalf("CreateWorkspace failed: %v", err)
	}
	if ws.ID != "ws-new" {
		t.Errorf("ID = %q, want %q", ws.ID, "ws-new")
	}
	if ws.Title != "my-workspace" {
		t.Errorf("Title = %q, want %q", ws.Title, "my-workspace")
	}
}

func TestListWorkspaces_Success(t *testing.T) {
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"workspaces":[{"id":"ws-1","title":"First","index":0},{"id":"ws-2","title":"Second","index":1}]}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	workspaces, err := client.ListWorkspaces()
	if err != nil {
		t.Fatalf("ListWorkspaces failed: %v", err)
	}
	if len(workspaces) != 2 {
		t.Fatalf("len = %d, want 2", len(workspaces))
	}
	if workspaces[0].ID != "ws-1" {
		t.Errorf("workspaces[0].ID = %q, want %q", workspaces[0].ID, "ws-1")
	}
	if workspaces[1].Title != "Second" {
		t.Errorf("workspaces[1].Title = %q, want %q", workspaces[1].Title, "Second")
	}
}

func TestListWorkspacesForWindow_UsesWindowID(t *testing.T) {
	var capturedParams json.RawMessage
	var mu sync.Mutex

	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method != "workspace.list" {
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
		mu.Lock()
		capturedParams = params
		mu.Unlock()
		return json.RawMessage(`{"window_id":"win-1","workspaces":[{"id":"ws-1","title":"First","index":0}]}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	workspaces, err := client.ListWorkspacesForWindow("win-1")
	if err != nil {
		t.Fatalf("ListWorkspacesForWindow failed: %v", err)
	}
	if len(workspaces) != 1 {
		t.Fatalf("len = %d, want 1", len(workspaces))
	}
	if workspaces[0].ID != "ws-1" {
		t.Fatalf("workspaces[0].ID = %q, want %q", workspaces[0].ID, "ws-1")
	}

	mu.Lock()
	defer mu.Unlock()

	var p map[string]string
	if err := json.Unmarshal(capturedParams, &p); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if p["window_id"] != "win-1" {
		t.Fatalf("window_id = %q, want %q", p["window_id"], "win-1")
	}
}

func TestListAllWorkspaces_AggregatesAcrossWindows(t *testing.T) {
	var mu sync.Mutex
	callCount := 0

	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		defer mu.Unlock()
		callCount++

		switch method {
		case "window.list":
			return json.RawMessage(`{"windows":[{"id":"win-1","ref":"ref-win-1","index":0,"key":true,"visible":true,"workspace_count":1,"selected_workspace_id":"ws-1","selected_workspace_ref":"ref-ws-1"},{"id":"win-2","ref":"ref-win-2","index":1,"key":false,"visible":true,"workspace_count":1,"selected_workspace_id":"ws-2","selected_workspace_ref":"ref-ws-2"}]}`), nil
		case "workspace.list":
			var p map[string]string
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, err
			}
			switch p["window_id"] {
			case "win-1":
				return json.RawMessage(`{"window_id":"win-1","workspaces":[{"id":"ws-1","title":"First","index":0}]}`), nil
			case "win-2":
				return json.RawMessage(`{"window_id":"win-2","workspaces":[{"id":"ws-2","title":"Second","index":0}]}`), nil
			default:
				return nil, fmt.Errorf("unexpected window_id: %s", p["window_id"])
			}
		default:
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	listings, err := client.ListAllWorkspaces()
	if err != nil {
		t.Fatalf("ListAllWorkspaces failed: %v", err)
	}
	if len(listings) != 2 {
		t.Fatalf("len = %d, want 2", len(listings))
	}
	if listings[0].WindowID != "win-1" || listings[0].Workspace.ID != "ws-1" {
		t.Fatalf("listings[0] = %+v, want win-1/ws-1", listings[0])
	}
	if listings[1].WindowID != "win-2" || listings[1].Workspace.Title != "Second" {
		t.Fatalf("listings[1] = %+v, want win-2/Second", listings[1])
	}
	if callCount != 3 {
		t.Fatalf("callCount = %d, want 3", callCount)
	}
}

func TestRenameSurface_RetriesWithoutWorkspaceIDOnTabNotFound(t *testing.T) {
	var mu sync.Mutex
	callCount := 0
	var paramsByCall []json.RawMessage

	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method != "tab.action" {
			return nil, fmt.Errorf("unexpected method: %s", method)
		}

		mu.Lock()
		callCount++
		paramsByCall = append(paramsByCall, append(json.RawMessage(nil), params...))
		currentCall := callCount
		mu.Unlock()

		if currentCall == 1 {
			return nil, fmt.Errorf("Tab not found")
		}
		return json.RawMessage(`{}`), nil
	})

	client := NewCmuxClient(sockPath)
	defer client.Close()

	if err := client.RenameSurface("ws-1", "surf-1", "Renamed"); err != nil {
		t.Fatalf("RenameSurface failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if callCount != 2 {
		t.Fatalf("callCount = %d, want 2", callCount)
	}

	var first, second map[string]string
	if err := json.Unmarshal(paramsByCall[0], &first); err != nil {
		t.Fatalf("unmarshal first params: %v", err)
	}
	if err := json.Unmarshal(paramsByCall[1], &second); err != nil {
		t.Fatalf("unmarshal second params: %v", err)
	}
	if first["workspace_id"] != "ws-1" {
		t.Fatalf("first workspace_id = %q, want %q", first["workspace_id"], "ws-1")
	}
	if _, ok := second["workspace_id"]; ok {
		t.Fatalf("second call unexpectedly included workspace_id: %v", second)
	}
	if second["surface_id"] != "surf-1" || second["title"] != "Renamed" {
		t.Fatalf("second params = %v, want surface_id/title preserved", second)
	}
}

func TestReconnect_WithBackoff(t *testing.T) {
	// Use a non-existent socket path
	client := NewCmuxClient("/tmp/nonexistent-test-socket.sock")
	defer client.Close()

	_, err := client.Call("test", nil)
	if err == nil {
		t.Fatal("expected error for non-existent socket")
	}
	if !strings.Contains(err.Error(), "failed to connect") {
		t.Errorf("error = %q, want to contain 'failed to connect'", err.Error())
	}

	// Second call should also fail but backoff should be set
	_, err = client.Call("test", nil)
	if err == nil {
		t.Fatal("expected error on second call")
	}
}
