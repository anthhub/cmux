package channels

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestManager_RegisterAndRoute(t *testing.T) {
	mgr := NewManager()
	mock := NewMockChannel("test")
	mgr.Register(mock)

	err := mgr.Send("test", "chat-1", OutboundMessage{Text: "hello"})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	msgs := mock.Messages()
	if len(msgs) != 1 {
		t.Fatalf("len = %d, want 1", len(msgs))
	}
	if msgs[0].Text != "hello" {
		t.Errorf("Text = %q, want %q", msgs[0].Text, "hello")
	}
}

func TestManager_SendUnknownChannel(t *testing.T) {
	mgr := NewManager()
	err := mgr.Send("nonexistent", "chat-1", OutboundMessage{Text: "hello"})
	if err == nil {
		t.Fatal("expected error for unknown channel")
	}
}

func TestManager_OnMessageHandler(t *testing.T) {
	mgr := NewManager()
	mock := NewMockChannel("test")

	var received InboundMessage
	var mu sync.Mutex
	done := make(chan struct{})

	mgr.OnMessage(func(msg InboundMessage) {
		mu.Lock()
		received = msg
		mu.Unlock()
		close(done)
	})
	mgr.Register(mock)

	mock.SimulateInbound(InboundMessage{
		ChatID: "chat-1",
		UserID: "user-1",
		Text:   "test message",
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message handler")
	}

	mu.Lock()
	defer mu.Unlock()

	if received.Text != "test message" {
		t.Errorf("Text = %q, want %q", received.Text, "test message")
	}
	if received.ChannelName != "test" {
		t.Errorf("ChannelName = %q, want %q", received.ChannelName, "test")
	}
}

func TestManager_SendTypingFallback(t *testing.T) {
	mgr := NewManager()
	// Use a channel that does NOT implement TypingCapable
	mock := &nonTypingChannel{name: "basic"}
	mgr.Register(mock)

	stop, err := mgr.SendTyping("basic", "chat-1")
	if err != nil {
		t.Fatalf("SendTyping failed: %v", err)
	}
	// Non-typing channel returns a no-op stop func
	if stop == nil {
		t.Fatal("stop func should not be nil for non-typing channel")
	}
	stop() // should not panic
}

func TestManager_MaxMessageLength(t *testing.T) {
	mgr := NewManager()
	mock := NewMockChannel("test")
	mgr.Register(mock)

	got := mgr.MaxMessageLength("test")
	if got != 4000 {
		t.Errorf("MaxMessageLength = %d, want 4000", got)
	}

	// Unknown channel should return fallback
	got = mgr.MaxMessageLength("unknown")
	if got != 4000 {
		t.Errorf("MaxMessageLength(unknown) = %d, want 4000", got)
	}
}

func TestManager_StartStop(t *testing.T) {
	mgr := NewManager()
	mock := NewMockChannel("test")
	mgr.Register(mock)

	ctx := context.Background()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !mock.IsRunning() {
		t.Error("expected channel to be running after Start")
	}

	mgr.Stop()
	if mock.IsRunning() {
		t.Error("expected channel to be stopped after Stop")
	}
}

// nonTypingChannel is a minimal Channel that does NOT implement TypingCapable.
type nonTypingChannel struct {
	name    string
	handler func(InboundMessage)
}

func (c *nonTypingChannel) Name() string                           { return c.name }
func (c *nonTypingChannel) Start(_ context.Context) error          { return nil }
func (c *nonTypingChannel) Stop() error                            { return nil }
func (c *nonTypingChannel) Send(_ string, _ OutboundMessage) error { return nil }
func (c *nonTypingChannel) OnMessage(h func(InboundMessage))       { c.handler = h }
