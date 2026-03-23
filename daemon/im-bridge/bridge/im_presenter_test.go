package bridge

import (
	"context"
	"sync"
	"testing"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/channels"
)

// mockChannel implements channels.Channel, StreamingCapable, and MessageLengthProvider
type mockChannel struct {
	name    string
	handler func(channels.InboundMessage)
	mu      sync.Mutex
	sent    []mockSentMsg
	edited  []mockEditedMsg
	running bool
}

type mockSentMsg struct {
	chatID string
	msg    channels.OutboundMessage
	msgID  string
}

type mockEditedMsg struct {
	chatID    string
	messageID string
	msg       channels.OutboundMessage
}

func newMockChannel(name string) *mockChannel {
	return &mockChannel{name: name}
}

func (m *mockChannel) Name() string                                    { return m.name }
func (m *mockChannel) Start(_ context.Context) error                   { m.running = true; return nil }
func (m *mockChannel) Stop() error                                     { m.running = false; return nil }
func (m *mockChannel) OnMessage(handler func(channels.InboundMessage)) { m.handler = handler }

func (m *mockChannel) Send(chatID string, msg channels.OutboundMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, mockSentMsg{chatID: chatID, msg: msg, msgID: ""})
	return nil
}

func (m *mockChannel) SendStreaming(chatID string, msg channels.OutboundMessage) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := "msg-1"
	if len(m.sent) > 0 {
		id = "msg-2"
	}
	m.sent = append(m.sent, mockSentMsg{chatID: chatID, msg: msg, msgID: id})
	return id, nil
}

func (m *mockChannel) EditStreaming(chatID, messageID string, msg channels.OutboundMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.edited = append(m.edited, mockEditedMsg{chatID: chatID, messageID: messageID, msg: msg})
	return nil
}

func (m *mockChannel) MaxMessageLength() int { return 4000 }

func (m *mockChannel) getSent() []mockSentMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]mockSentMsg, len(m.sent))
	copy(cp, m.sent)
	return cp
}

func setupPresenter(t *testing.T, verbose bool) (*IMPresenter, *mockChannel) {
	t.Helper()
	mock := newMockChannel("test")
	mgr := channels.NewManager()
	mgr.Register(mock)
	presenter := NewIMPresenter(mgr, "test", "chat-1", verbose)
	return presenter, mock
}

func TestIMPresenter_TextEvent_SendsMessage(t *testing.T) {
	presenter, mock := setupPresenter(t, false)

	presenter.HandleEvent(StreamEvent{
		Type:    StreamEventText,
		Content: "Hello from Claude",
	})
	presenter.Close()

	sent := mock.getSent()
	if len(sent) == 0 {
		t.Fatal("expected at least one sent message")
	}
	found := false
	for _, s := range sent {
		if s.msg.Text == "Hello from Claude" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected message with text 'Hello from Claude', got %v", sent)
	}
}

func TestIMPresenter_Close_FlushesBuffer(t *testing.T) {
	presenter, mock := setupPresenter(t, false)

	// Send a delta text event (will be buffered)
	presenter.HandleEvent(StreamEvent{
		Type:    StreamEventText,
		Content: "buffered text",
		Delta:   true,
	})

	// Before close, buffer might not have flushed
	presenter.Close()

	sent := mock.getSent()
	if len(sent) == 0 {
		t.Fatal("expected buffered text to be flushed on Close")
	}
	found := false
	for _, s := range sent {
		if s.msg.Text == "buffered text" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected flushed text 'buffered text', got %v", sent)
	}
}

func TestIMPresenter_VerboseShowsToolUse(t *testing.T) {
	presenter, mock := setupPresenter(t, true)

	presenter.HandleEvent(StreamEvent{
		Type:    StreamEventToolUse,
		Content: "Read /tmp/file.go",
	})
	presenter.Close()

	sent := mock.getSent()
	if len(sent) == 0 {
		t.Fatal("expected tool_use message in verbose mode")
	}
	found := false
	for _, s := range sent {
		if s.msg.Text != "" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected at least one non-empty message for tool_use in verbose mode")
	}
}

func TestIMPresenter_NormalHidesToolUse(t *testing.T) {
	presenter, mock := setupPresenter(t, false)

	presenter.HandleEvent(StreamEvent{
		Type:    StreamEventToolUse,
		Content: "Read /tmp/file.go",
	})
	presenter.Close()

	sent := mock.getSent()
	if len(sent) != 0 {
		t.Errorf("expected no messages in non-verbose mode for tool_use, got %d", len(sent))
	}
}
