package channels

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

const workerQueueSize = 64

// outboundWork is a unit of work queued to a channel worker.
type outboundWork struct {
	chatID string
	msg    OutboundMessage
}

// channelWorker is a per-channel goroutine that serialises outbound sends with retry.
type channelWorker struct {
	ch     Channel
	queue  chan outboundWork
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newChannelWorker(ctx context.Context, ch Channel) *channelWorker {
	wctx, cancel := context.WithCancel(ctx)
	return &channelWorker{
		ch:     ch,
		queue:  make(chan outboundWork, workerQueueSize),
		ctx:    wctx,
		cancel: cancel,
	}
}

func (w *channelWorker) start() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-w.ctx.Done():
				return
			case work, ok := <-w.queue:
				if !ok {
					return
				}
				w.sendWithRetry(work)
			}
		}
	}()
}

func (w *channelWorker) stop() {
	w.cancel()
}

// enqueue tries to queue work. If the queue is full it falls back to a direct send.
func (w *channelWorker) enqueue(work outboundWork) {
	select {
	case w.queue <- work:
	default:
		// Queue full: degrade to synchronous send on the caller goroutine.
		if err := w.ch.Send(work.chatID, work.msg); err != nil {
			log.Printf("[channels/%s] sync fallback send error: %v", w.ch.Name(), err)
		}
	}
}

// sendWithRetry calls ch.Send with retry semantics based on the error type.
func (w *channelWorker) sendWithRetry(work outboundWork) {
	const maxRetries = 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := w.ch.Send(work.chatID, work.msg)
		if err == nil {
			return
		}

		switch {
		case errors.Is(err, ErrNotRunning):
			// Channel is stopped; discard the message.
			log.Printf("[channels/%s] channel not running, discarding message", w.ch.Name())
			return

		case errors.Is(err, ErrRateLimit):
			if attempt >= maxRetries {
				log.Printf("[channels/%s] rate limit exceeded after %d retries", w.ch.Name(), attempt)
				return
			}
			log.Printf("[channels/%s] rate limited, retrying in 1s (attempt %d/%d)", w.ch.Name(), attempt+1, maxRetries)
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(time.Second):
			}

		case errors.Is(err, ErrTemporary):
			if attempt >= maxRetries {
				log.Printf("[channels/%s] temporary error after %d retries: %v", w.ch.Name(), attempt, err)
				return
			}
			// Exponential backoff: 500ms, 1s, 2s … capped at 8s.
			backoff := time.Duration(500<<uint(attempt)) * time.Millisecond
			if backoff > 8*time.Second {
				backoff = 8 * time.Second
			}
			log.Printf("[channels/%s] temporary error, retrying in %s (attempt %d/%d): %v",
				w.ch.Name(), backoff, attempt+1, maxRetries, err)
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(backoff):
			}

		default:
			// Non-retryable error; log and discard.
			log.Printf("[channels/%s] send error (non-retryable): %v", w.ch.Name(), err)
			return
		}
	}
}

// Manager manages multiple IM channels and routes messages.
type Manager struct {
	channels map[string]Channel
	workers  map[string]*channelWorker
	handler  func(msg InboundMessage)
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewManager creates a new channel manager.
func NewManager() *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		channels: make(map[string]Channel),
		workers:  make(map[string]*channelWorker),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Register adds a channel to the manager and starts its worker goroutine.
func (m *Manager) Register(ch Channel) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.channels[ch.Name()] = ch

	// Start a dedicated worker for this channel.
	w := newChannelWorker(m.ctx, ch)
	w.start()
	m.workers[ch.Name()] = w

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

	var started []string

	for name, ch := range m.channels {
		log.Printf("[channels] starting %s", name)
		if err := ch.Start(ctx); err != nil {
			// Rollback: stop all previously started channels
			for _, startedName := range started {
				if sch, ok := m.channels[startedName]; ok {
					log.Printf("[channels] rolling back %s", startedName)
					sch.Stop()
				}
			}
			return fmt.Errorf("failed to start channel %s: %w", name, err)
		}
		started = append(started, name)
		log.Printf("[channels] %s started", name)
	}
	return nil
}

// Stop stops all registered channels and their workers.
func (m *Manager) Stop() {
	m.cancel() // signal all workers to stop

	m.mu.RLock()
	workers := make([]*channelWorker, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w)
	}
	for name, ch := range m.channels {
		log.Printf("[channels] stopping %s", name)
		if err := ch.Stop(); err != nil {
			log.Printf("[channels] error stopping %s: %v", name, err)
		}
	}
	m.mu.RUnlock()

	// Wait for all worker goroutines to finish.
	for _, w := range workers {
		w.wg.Wait()
	}
}

// Send enqueues a message to the named channel's worker.
func (m *Manager) Send(channelName, chatID string, msg OutboundMessage) error {
	m.mu.RLock()
	_, ok := m.channels[channelName]
	w := m.workers[channelName]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("channel %s not found", channelName)
	}
	w.enqueue(outboundWork{chatID: chatID, msg: msg})
	return nil
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
