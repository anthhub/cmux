package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/channels"
	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

type sessionTestChannel struct {
	name     string
	maxLen   int
	messages []channels.OutboundMessage
	handler  func(channels.InboundMessage)
	mu       sync.Mutex
}

func newSessionTestChannel(name string) *sessionTestChannel {
	return &sessionTestChannel{name: name, maxLen: 4000}
}

func (c *sessionTestChannel) Name() string                { return c.name }
func (c *sessionTestChannel) Start(context.Context) error { return nil }
func (c *sessionTestChannel) Stop() error                 { return nil }
func (c *sessionTestChannel) IsRunning() bool             { return true }
func (c *sessionTestChannel) OnMessage(handler func(channels.InboundMessage)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler = handler
}
func (c *sessionTestChannel) Send(_ string, msg channels.OutboundMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, msg)
	return nil
}
func (c *sessionTestChannel) MaxMessageLength() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxLen
}
func (c *sessionTestChannel) SetMaxMessageLength(maxLen int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxLen = maxLen
}
func (c *sessionTestChannel) Messages() []channels.OutboundMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]channels.OutboundMessage, len(c.messages))
	copy(cp, c.messages)
	return cp
}

// WaitForMessages polls until at least n messages are received or timeout elapses.
func (c *sessionTestChannel) WaitForMessages(t *testing.T, n int, timeout time.Duration) []channels.OutboundMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs := c.Messages()
		if len(msgs) >= n {
			return msgs
		}
		time.Sleep(5 * time.Millisecond)
	}
	msgs := c.Messages()
	if len(msgs) < n {
		t.Fatalf("timeout waiting for %d messages, got %d", n, len(msgs))
	}
	return msgs
}

func newTestSessionManager(t *testing.T, handler func(method string, params json.RawMessage) (json.RawMessage, error)) (*SessionManager, *sessionTestChannel, *CmuxClient) {
	t.Helper()

	sockPath := startMockCmuxSocket(t, handler)
	cmux := NewCmuxClient(sockPath)

	channelMgr := channels.NewManager()
	channel := newSessionTestChannel("test")
	channelMgr.Register(channel)

	sm := NewSessionManager(context.Background(), cmux, channelMgr, config.AIConfig{
		DefaultAgent: AgentTypeClaude,
	})

	return sm, channel, cmux
}

func TestSyncAgentSessions_AddsSurfaceWithoutBlocking(t *testing.T) {
	sm, _, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method != "surface.list" {
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
		return json.RawMessage(`{
			"surfaces": [
				{"id":"surf-1","ref":"surf-ref-1","type":"terminal","title":"default","focused":true,"index":0}
			],
			"workspace_id":"ws-1"
		}`), nil
	})

	agent := &Agent{
		Name:        "alpha",
		DefaultType: AgentTypeCodex,
		Sessions:    make(map[string]*Session),
	}
	agent.setWorkspaceID("ws-1")

	done := make(chan error, 1)
	go func() {
		done <- sm.syncAgentSessions(agent)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("syncAgentSessions failed: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("syncAgentSessions blocked")
	}

	session := agent.ActiveSession()
	if session == nil {
		t.Fatal("expected active session after sync")
	}
	if session.ID != "surf-1" {
		t.Fatalf("session.ID = %q, want surf-1", session.ID)
	}
	if session.agentType() != AgentTypeCodex {
		t.Fatalf("session.agentType = %q, want %q", session.agentType(), AgentTypeCodex)
	}
}

func TestEnsureAgentWorkspace_ReusesWorkspaceByTitleWhenCachedIDMissing(t *testing.T) {
	createCalls := 0
	sm, _, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "workspace.list":
			return json.RawMessage(`{
				"workspaces": [
					{"id":"ws-recovered","title":"IM user-1 / alpha","ref":"ref-1","index":0,"selected":false}
				]
			}`), nil
		case "workspace.create":
			createCalls++
			return json.RawMessage(`{"workspace_id":"ws-created","workspace_ref":"ref-created"}`), nil
		case "workspace.rename":
			return json.RawMessage(`{}`), nil
		default:
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
	})

	agent := &Agent{
		Name:        "alpha",
		DefaultType: AgentTypeClaude,
		Sessions:    make(map[string]*Session),
		WorkspaceID: "ws-stale",
	}

	created, err := sm.ensureAgentWorkspace("user-1", agent)
	if err != nil {
		t.Fatalf("ensureAgentWorkspace failed: %v", err)
	}
	if created {
		t.Fatal("expected existing workspace to be reused")
	}
	if createCalls != 0 {
		t.Fatalf("workspace.create called %d times, want 0", createCalls)
	}
	if got := agent.workspaceID(); got != "ws-recovered" {
		t.Fatalf("workspaceID = %q, want %q", got, "ws-recovered")
	}
}

func TestEnsureAgentWorkspace_ReusesWorkspaceAcrossWindows(t *testing.T) {
	createCalls := 0
	sm, _, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "window.list":
			return json.RawMessage(`{
				"windows": [
					{"id":"win-a","ref":"window:1","index":0,"key":true,"visible":true,"workspace_count":1,"selected_workspace_id":"ws-a","selected_workspace_ref":"workspace:1"},
					{"id":"win-b","ref":"window:2","index":1,"key":false,"visible":true,"workspace_count":1,"selected_workspace_id":"ws-b","selected_workspace_ref":"workspace:2"}
				]
			}`), nil
		case "workspace.list":
			var p map[string]string
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, err
			}
			switch p["window_id"] {
			case "win-a":
				return json.RawMessage(`{
					"window_id":"win-a",
					"workspaces": [
						{"id":"ws-a","title":"Scratch","ref":"workspace:1","index":0,"selected":true}
					]
				}`), nil
			case "win-b":
				return json.RawMessage(`{
					"window_id":"win-b",
					"workspaces": [
						{"id":"ws-recovered","title":"IM user-1 / alpha","ref":"workspace:2","index":0,"selected":false}
					]
				}`), nil
			default:
				return nil, fmt.Errorf("unexpected window_id: %q", p["window_id"])
			}
		case "workspace.create":
			createCalls++
			return json.RawMessage(`{"workspace_id":"ws-created","workspace_ref":"ref-created"}`), nil
		case "workspace.rename":
			return json.RawMessage(`{}`), nil
		default:
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
	})

	agent := &Agent{
		Name:        "alpha",
		DefaultType: AgentTypeClaude,
		Sessions:    make(map[string]*Session),
		WorkspaceID: "ws-stale",
	}

	created, err := sm.ensureAgentWorkspace("user-1", agent)
	if err != nil {
		t.Fatalf("ensureAgentWorkspace failed: %v", err)
	}
	if created {
		t.Fatal("expected existing cross-window workspace to be reused")
	}
	if createCalls != 0 {
		t.Fatalf("workspace.create called %d times, want 0", createCalls)
	}
	if got := agent.workspaceID(); got != "ws-recovered" {
		t.Fatalf("workspaceID = %q, want %q", got, "ws-recovered")
	}
}

func TestEnsureAgentWorkspace_SerializesConcurrentCreation(t *testing.T) {
	var (
		mu                sync.Mutex
		workspaceCreated  bool
		createCalls       int
		listCalls         int
		firstListEntered  = make(chan struct{})
		secondListEntered = make(chan struct{})
		releaseFirstList  = make(chan struct{})
	)

	sm, _, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "window.list":
			return json.RawMessage(`{
				"windows": [
					{"id":"win-a","ref":"window:1","index":0,"key":true,"visible":true,"workspace_count":1,"selected_workspace_id":"ws-a","selected_workspace_ref":"workspace:1"}
				]
			}`), nil
		case "workspace.list":
			mu.Lock()
			listCalls++
			callNumber := listCalls
			created := workspaceCreated
			mu.Unlock()

			if callNumber == 1 {
				close(firstListEntered)
				<-releaseFirstList
				mu.Lock()
				created = workspaceCreated
				mu.Unlock()
			} else {
				close(secondListEntered)
			}

			if created {
				return json.RawMessage(`{
					"window_id":"win-a",
					"workspaces": [
						{"id":"ws-created","title":"IM user-1 / alpha","ref":"workspace:2","index":1,"selected":false}
					]
				}`), nil
			}
			return json.RawMessage(`{"window_id":"win-a","workspaces":[]}`), nil
		case "workspace.create":
			mu.Lock()
			createCalls++
			workspaceCreated = true
			mu.Unlock()
			return json.RawMessage(`{"workspace_id":"ws-created","workspace_ref":"ref-created"}`), nil
		case "workspace.rename":
			return json.RawMessage(`{}`), nil
		default:
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
	})

	agent := &Agent{
		Name:        "alpha",
		DefaultType: AgentTypeClaude,
		Sessions:    make(map[string]*Session),
	}

	resultCh := make(chan struct {
		created bool
		err     error
	}, 2)

	go func() {
		created, err := sm.ensureAgentWorkspace("user-1", agent)
		resultCh <- struct {
			created bool
			err     error
		}{created: created, err: err}
	}()

	select {
	case <-firstListEntered:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first ensureAgentWorkspace call did not reach workspace.list")
	}

	go func() {
		created, err := sm.ensureAgentWorkspace("user-1", agent)
		resultCh <- struct {
			created bool
			err     error
		}{created: created, err: err}
	}()

	select {
	case <-secondListEntered:
		t.Fatal("second ensureAgentWorkspace call reached workspace.list before the first completed")
	case <-time.After(75 * time.Millisecond):
	}

	close(releaseFirstList)

	first := <-resultCh
	second := <-resultCh
	if first.err != nil {
		t.Fatalf("first ensureAgentWorkspace failed: %v", first.err)
	}
	if second.err != nil {
		t.Fatalf("second ensureAgentWorkspace failed: %v", second.err)
	}
	if !first.created && !second.created {
		t.Fatal("expected one concurrent call to create the workspace")
	}

	mu.Lock()
	gotCreateCalls := createCalls
	mu.Unlock()
	if gotCreateCalls != 1 {
		t.Fatalf("workspace.create called %d times, want 1", gotCreateCalls)
	}
	if got := agent.workspaceID(); got != "ws-created" {
		t.Fatalf("workspaceID = %q, want %q", got, "ws-created")
	}
}

func TestReply_SplitsMessagesByChannelLimit(t *testing.T) {
	sm, channel, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("unexpected cmux call: %s", method)
	})
	channel.SetMaxMessageLength(5)

	sm.reply(channels.InboundMessage{
		ChannelName: "test",
		ChatID:      "chat-1",
	}, "1234567890")

	messages := channel.WaitForMessages(t, 2, 2*time.Second)
	if len(messages) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(messages))
	}
	if messages[0].Text != "12345" {
		t.Fatalf("messages[0] = %q, want %q", messages[0].Text, "12345")
	}
	if messages[1].Text != "67890" {
		t.Fatalf("messages[1] = %q, want %q", messages[1].Text, "67890")
	}
}

func TestReply_PreservesContextToken(t *testing.T) {
	sm, channel, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("unexpected cmux call: %s", method)
	})

	sm.reply(channels.InboundMessage{
		ChannelName:  "test",
		ChatID:       "chat-1",
		ContextToken: "ctx-123",
	}, "hello")

	messages := channel.WaitForMessages(t, 1, 2*time.Second)
	if len(messages) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(messages))
	}
	if messages[0].ContextToken != "ctx-123" {
		t.Fatalf("ContextToken = %q, want %q", messages[0].ContextToken, "ctx-123")
	}
}

func TestHandleResetSession_SendsCtrlCAndRelaunches(t *testing.T) {
	var sentTexts []string
	sm, channel, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method == "surface.send_text" {
			var p map[string]string
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, err
			}
			sentTexts = append(sentTexts, p["text"])
			return json.RawMessage(`{}`), nil
		}
		return json.RawMessage(`{}`), nil
	})

	session := &Session{
		ID:                "surf-1",
		SurfaceID:         "surf-1",
		Name:              "default",
		AgentType:         AgentTypeClaude,
		ProviderSessionID: "claude-session-1",
	}
	agent := &Agent{
		Name:         "alpha",
		DefaultType:  AgentTypeClaude,
		WorkspaceID:  "ws-1",
		Sessions:     map[string]*Session{session.ID: session},
		SessionOrder: []string{session.ID},
		ActiveSID:    session.ID,
	}

	sm.handleResetSession(agent, channels.InboundMessage{ChannelName: "test", ChatID: "chat-1"})

	// Should send Ctrl+C and rely on the next turn to invoke Claude print mode.
	if len(sentTexts) < 1 {
		t.Fatalf("expected at least 1 send_text call, got %d", len(sentTexts))
	}
	if sentTexts[0] != "\x03" {
		t.Fatalf("first send_text = %q, want ctrl-c", sentTexts[0])
	}
	if len(sentTexts) > 1 {
		t.Fatalf("unexpected extra send_text after reset: %q", sentTexts[1])
	}

	// Provider session ID should be cleared
	if got := session.providerSessionID(); got != "" {
		t.Fatalf("providerSessionID = %q, want empty", got)
	}

	messages := channel.WaitForMessages(t, 1, 2*time.Second)
	if len(messages) == 0 {
		t.Fatal("expected reset reply")
	}
	last := messages[len(messages)-1].Text
	if !strings.Contains(last, "Reset session context: default") {
		t.Fatalf("reply = %q, want reset confirmation", last)
	}
}

func TestHandleCloseSession_ClosesSessionSurface(t *testing.T) {
	var (
		mu     sync.Mutex
		closed bool
	)

	sm, channel, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "surface.send_text":
			return json.RawMessage(`{}`), nil
		case "surface.list":
			mu.Lock()
			isClosed := closed
			mu.Unlock()
			if isClosed {
				return json.RawMessage(`{"surfaces":[],"workspace_id":"ws-1"}`), nil
			}
			return json.RawMessage(`{
				"surfaces": [
					{"id":"surf-1","ref":"surf-ref-1","type":"terminal","title":"default","focused":true,"index":0}
				],
				"workspace_id":"ws-1"
			}`), nil
		case "surface.close":
			mu.Lock()
			closed = true
			mu.Unlock()
			return json.RawMessage(`{}`), nil
		default:
			return nil, fmt.Errorf("unexpected method: %s", method)
		}
	})

	session := &Session{
		ID:          "surf-1",
		SurfaceID:   "surf-1",
		Name:        "default",
		SurfaceType: "terminal",
		AgentType:   AgentTypeClaude,
	}
	agent := &Agent{
		Name:         "alpha",
		DefaultType:  AgentTypeClaude,
		WorkspaceID:  "ws-1",
		Sessions:     map[string]*Session{session.ID: session},
		SessionOrder: []string{session.ID},
		ActiveSID:    session.ID,
	}

	sm.handleCloseSession(agent, channels.InboundMessage{ChannelName: "test", ChatID: "chat-1"})

	messages := channel.WaitForMessages(t, 1, 2*time.Second)
	if len(messages) == 0 {
		t.Fatal("expected close reply")
	}
	last := messages[len(messages)-1].Text
	if !strings.Contains(last, "Closed session: default") {
		t.Fatalf("reply = %q, want close confirmation", last)
	}
}

func TestHandleFastCommand_CodexReportsUnsupported(t *testing.T) {
	sm, channel, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("unexpected cmux call: %s", method)
	})

	session := &Session{ID: "surf-1", Name: "default", AgentType: AgentTypeCodex, Model: "gpt-5.4"}
	agent := &Agent{
		Name:         "alpha",
		DefaultType:  AgentTypeCodex,
		Sessions:     map[string]*Session{session.ID: session},
		SessionOrder: []string{session.ID},
		ActiveSID:    session.ID,
	}

	sm.handleFastCommand(agent, channels.InboundMessage{ChannelName: "test", ChatID: "chat-1"}, []string{"on"})

	if session.fast() {
		t.Fatal("expected fast mode to remain disabled for codex")
	}
	if got := session.model(); got != "gpt-5.4" {
		t.Fatalf("model = %q, want %q", got, "gpt-5.4")
	}

	messages := channel.WaitForMessages(t, 1, 2*time.Second)
	last := messages[len(messages)-1].Text
	if !strings.Contains(last, "Fast mode currently maps to Claude presets only") {
		t.Fatalf("reply = %q, want codex unsupported message", last)
	}
}

func TestHandleThinkCommand_CodexReportsUnsupported(t *testing.T) {
	sm, channel, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("unexpected cmux call: %s", method)
	})

	session := &Session{ID: "surf-1", Name: "default", AgentType: AgentTypeCodex}
	agent := &Agent{
		Name:         "alpha",
		DefaultType:  AgentTypeCodex,
		Sessions:     map[string]*Session{session.ID: session},
		SessionOrder: []string{session.ID},
		ActiveSID:    session.ID,
	}

	sm.handleThinkCommand(agent, channels.InboundMessage{ChannelName: "test", ChatID: "chat-1"}, []string{"high"})

	if got := session.effort(); got != "" {
		t.Fatalf("effort = %q, want empty", got)
	}

	messages := channel.WaitForMessages(t, 1, 2*time.Second)
	last := messages[len(messages)-1].Text
	if !strings.Contains(last, "Thinking effort is currently supported only for Claude sessions") {
		t.Fatalf("reply = %q, want codex unsupported message", last)
	}
}
