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
		if m.handler != nil {
			m.handler(msg)
		}
	})
}

// OnMessage sets the global message handler for all channels.
func (m *Manager) OnMessage(handler func(msg InboundMessage)) {
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
