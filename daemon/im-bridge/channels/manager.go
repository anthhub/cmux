package channels

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// Manager manages multiple IM channels and routes messages.
type Manager struct {
	channels map[string]Channel
	handler  func(msg InboundMessage)
	mu       sync.RWMutex
}

// NewManager creates a new channel manager.
func NewManager() *Manager {
	return &Manager{
		channels: make(map[string]Channel),
	}
}

// Register adds a channel to the manager.
func (m *Manager) Register(ch Channel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channels[ch.Name()] = ch
	ch.OnMessage(func(msg InboundMessage) {
		msg.ChannelName = ch.Name()
		m.mu.RLock()
		h := m.handler
		m.mu.RUnlock()
		if h != nil {
			h(msg)
		}
	})
}

// OnMessage sets the global message handler for all channels.
func (m *Manager) OnMessage(handler func(msg InboundMessage)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = handler
}

// Start starts all registered channels.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, ch := range m.channels {
		log.Printf("[channels] starting %s", name)
		if err := ch.Start(ctx); err != nil {
			return fmt.Errorf("failed to start channel %s: %w", name, err)
		}
		log.Printf("[channels] %s started", name)
	}
	return nil
}

// Stop stops all registered channels.
func (m *Manager) Stop() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, ch := range m.channels {
		log.Printf("[channels] stopping %s", name)
		if err := ch.Stop(); err != nil {
			log.Printf("[channels] error stopping %s: %v", name, err)
		}
	}
}

// Send sends a message to a specific channel and chat.
func (m *Manager) Send(channelName, chatID string, msg OutboundMessage) error {
	m.mu.RLock()
	ch, ok := m.channels[channelName]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("channel %s not found", channelName)
	}
	return ch.Send(chatID, msg)
}

// SendTyping starts a typing indicator when supported by the channel.
func (m *Manager) SendTyping(channelName, chatID string) (func(), error) {
	m.mu.RLock()
	ch, ok := m.channels[channelName]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("channel %s not found", channelName)
	}

	typing, ok := ch.(TypingCapable)
	if !ok {
		return func() {}, nil
	}
	return typing.SendTyping(chatID)
}

// SendStreaming sends a message and returns its editable message ID when supported.
func (m *Manager) SendStreaming(channelName, chatID string, msg OutboundMessage) (string, error) {
	m.mu.RLock()
	ch, ok := m.channels[channelName]
	m.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("channel %s not found", channelName)
	}

	streaming, ok := ch.(StreamingCapable)
	if !ok {
		if err := ch.Send(chatID, msg); err != nil {
			return "", err
		}
		return "", nil
	}
	return streaming.SendStreaming(chatID, msg)
}

// EditStreaming updates a previously sent streaming message.
func (m *Manager) EditStreaming(channelName, chatID, messageID string, msg OutboundMessage) error {
	m.mu.RLock()
	ch, ok := m.channels[channelName]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("channel %s not found", channelName)
	}

	streaming, ok := ch.(StreamingCapable)
	if !ok {
		return fmt.Errorf("channel %s does not support message edits", channelName)
	}
	return streaming.EditStreaming(chatID, messageID, msg)
}

// MaxMessageLength returns a safe maximum length for outbound text.
func (m *Manager) MaxMessageLength(channelName string) int {
	const fallback = 4000

	m.mu.RLock()
	ch, ok := m.channels[channelName]
	m.mu.RUnlock()
	if !ok {
		return fallback
	}

	if provider, ok := ch.(MessageLengthProvider); ok {
		if max := provider.MaxMessageLength(); max > 0 {
			return max
		}
	}
	return fallback
}
