package bridge

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestReply_SplitsMessagesByChannelLimit(t *testing.T) {
	sm, channel, _ := newTestSessionManager(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("unexpected cmux call: %s", method)
	})
	channel.SetMaxMessageLength(5)

	sm.reply(channels.InboundMessage{
		ChannelName: "test",
		ChatID:      "chat-1",
	}, "1234567890")

	messages := channel.Messages()
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
