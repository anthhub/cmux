package channels

import (
	"context"
	"sync"
)

// MockChannel implements Channel, StreamingCapable, and MessageLengthProvider for testing.
type MockChannel struct {
	name     string
	messages []OutboundMessage
	edited   []editedRecord
	handler  func(InboundMessage)
	running  bool
	maxLen   int
	mu       sync.Mutex
}

type editedRecord struct {
	ChatID    string
	MessageID string
	Msg       OutboundMessage
}

func NewMockChannel(name string) *MockChannel {
	return &MockChannel{name: name, maxLen: 4000}
}

func (m *MockChannel) Name() string { return m.name }

func (m *MockChannel) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = true
	return nil
}

func (m *MockChannel) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	return nil
}

func (m *MockChannel) Send(chatID string, msg OutboundMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	return nil
}

func (m *MockChannel) OnMessage(handler func(InboundMessage)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = handler
}

func (m *MockChannel) SendStreaming(chatID string, msg OutboundMessage) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	return "msg-1", nil
}

func (m *MockChannel) EditStreaming(chatID, messageID string, msg OutboundMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.edited = append(m.edited, editedRecord{ChatID: chatID, MessageID: messageID, Msg: msg})
	return nil
}

func (m *MockChannel) MaxMessageLength() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maxLen > 0 {
		return m.maxLen
	}
	return 4000
}

func (m *MockChannel) SetMaxMessageLength(maxLen int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.maxLen = maxLen
}

// SimulateInbound triggers the registered handler with a message.
func (m *MockChannel) SimulateInbound(msg InboundMessage) {
	m.mu.Lock()
	h := m.handler
	m.mu.Unlock()
	if h != nil {
		h(msg)
	}
}

// Messages returns a copy of sent messages.
func (m *MockChannel) Messages() []OutboundMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]OutboundMessage, len(m.messages))
	copy(cp, m.messages)
	return cp
}

// IsRunning returns the running state.
func (m *MockChannel) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}
