package bridge

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/channels"
)

const (
	streamFlushInterval = 500 * time.Millisecond
	streamFlushChars    = 200
)

type presentedPart struct {
	MessageID string
	Text      string
}

// IMPresenter renders normalized stream events to IM messages.
type IMPresenter struct {
	channel     *channels.Manager
	channelName string
	chatID      string
	verbose     bool

	mu            sync.Mutex
	buffer        strings.Builder
	parts         []presentedPart
	typingStop    func()
	lastFlush     time.Time
	sawTextDelta  bool
	lastSentChars int
}

// NewIMPresenter creates a new presenter for one chat turn.
func NewIMPresenter(channel *channels.Manager, channelName, chatID string, verbose bool) *IMPresenter {
	return &IMPresenter{
		channel:     channel,
		channelName: channelName,
		chatID:      chatID,
		verbose:     verbose,
	}
}

// HandleEvent applies one stream event.
func (p *IMPresenter) HandleEvent(event StreamEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch event.Type {
	case StreamEventText:
		if event.Delta {
			p.sawTextDelta = true
			p.ensureTypingLocked()
			p.buffer.WriteString(event.Content)
		} else if !p.sawTextDelta && event.Content != "" {
			p.ensureTypingLocked()
			p.buffer.Reset()
			p.buffer.WriteString(event.Content)
		}
		p.maybeFlushLocked(false)
	case StreamEventThinking:
		if p.verbose && strings.TrimSpace(event.Content) != "" {
			p.sendStandaloneLocked("Thinking:\n" + strings.TrimSpace(event.Content))
		}
	case StreamEventToolUse:
		if p.verbose && strings.TrimSpace(event.Content) != "" {
			p.sendStandaloneLocked("Tool:\n" + strings.TrimSpace(event.Content))
		}
	case StreamEventToolResult:
		if p.verbose && strings.TrimSpace(event.Content) != "" {
			p.sendStandaloneLocked("Tool Result:\n" + strings.TrimSpace(event.Content))
		}
	case StreamEventControlRequest:
		message := strings.TrimSpace(event.Content)
		if message == "" {
			message = "Approval required. Reply with /approve once supported."
		}
		p.sendStandaloneLocked(message)
	case StreamEventError:
		if strings.TrimSpace(event.Content) != "" {
			p.sendStandaloneLocked("Error: " + strings.TrimSpace(event.Content))
		}
	case StreamEventResult:
		p.maybeFlushLocked(true)
	}
}

// Close flushes any remaining buffered text and stops the typing indicator.
func (p *IMPresenter) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maybeFlushLocked(true)
	p.stopTypingLocked()
}

func (p *IMPresenter) maybeFlushLocked(force bool) {
	text := p.buffer.String()
	if text == "" {
		return
	}

	if !force {
		if time.Since(p.lastFlush) < streamFlushInterval && len(text)-p.lastSentChars < streamFlushChars {
			return
		}
	}

	p.flushLocked(text)
	p.lastFlush = time.Now()
	p.lastSentChars = len(text)
}

func (p *IMPresenter) flushLocked(text string) {
	parts := splitForIM(text, p.channel.MaxMessageLength(p.channelName))
	if len(parts) == 0 {
		return
	}

	for idx, partText := range parts {
		msg := channels.OutboundMessage{Text: partText, Format: "text"}
		if idx < len(p.parts) {
			if p.parts[idx].Text == partText {
				continue
			}
			if p.parts[idx].MessageID != "" {
				if err := p.channel.EditStreaming(p.channelName, p.chatID, p.parts[idx].MessageID, msg); err == nil {
					p.parts[idx].Text = partText
					continue
				}
			}
		}

		messageID, err := p.channel.SendStreaming(p.channelName, p.chatID, msg)
		if err != nil {
			continue
		}
		if idx < len(p.parts) {
			p.parts[idx] = presentedPart{MessageID: messageID, Text: partText}
		} else {
			p.parts = append(p.parts, presentedPart{MessageID: messageID, Text: partText})
		}
	}
}

func (p *IMPresenter) ensureTypingLocked() {
	if p.typingStop != nil {
		return
	}
	stop, err := p.channel.SendTyping(p.channelName, p.chatID)
	if err == nil {
		p.typingStop = stop
	}
}

func (p *IMPresenter) stopTypingLocked() {
	if p.typingStop != nil {
		p.typingStop()
		p.typingStop = nil
	}
}

func (p *IMPresenter) sendStandaloneLocked(text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	_ = p.channel.Send(p.channelName, p.chatID, channels.OutboundMessage{
		Text:   text,
		Format: "text",
	})
}

func splitForIM(text string, maxLen int) []string {
	if maxLen <= 0 || len(text) <= maxLen {
		return []string{text}
	}

	var chunks []string
	lines := strings.Split(text, "\n")
	var current strings.Builder
	inCodeFence := false
	codeFenceHeader := "```"

	flush := func() {
		if current.Len() == 0 {
			return
		}
		chunk := current.String()
		if inCodeFence {
			chunk += "\n```"
		}
		chunks = append(chunks, chunk)
		current.Reset()
		if inCodeFence {
			current.WriteString(codeFenceHeader)
			current.WriteByte('\n')
		}
	}

	for _, line := range lines {
		lineWithBreak := line + "\n"
		if current.Len()+len(lineWithBreak) > maxLen && current.Len() > 0 {
			flush()
		}
		// If a single line exceeds maxLen, hard-split it.
		for len(lineWithBreak) > maxLen {
			take := maxLen - current.Len()
			if take <= 0 {
				flush()
				take = maxLen
			}
			if take > len(lineWithBreak) {
				take = len(lineWithBreak)
			}
			current.WriteString(lineWithBreak[:take])
			lineWithBreak = lineWithBreak[take:]
			if current.Len() >= maxLen {
				flush()
			}
		}
		current.WriteString(lineWithBreak)
		if strings.HasPrefix(line, "```") {
			if inCodeFence {
				inCodeFence = false
				codeFenceHeader = "```"
			} else {
				inCodeFence = true
				if strings.TrimSpace(line) != "" {
					codeFenceHeader = line
				}
			}
		}
	}

	if current.Len() > 0 {
		chunk := strings.TrimSuffix(current.String(), "\n")
		if inCodeFence {
			chunk += "\n```"
		}
		chunks = append(chunks, chunk)
	}

	if len(chunks) <= 1 {
		return chunks
	}

	numbered := make([]string, 0, len(chunks))
	total := len(chunks)
	for idx, chunk := range chunks {
		numbered = append(numbered, fmt.Sprintf("(%d/%d)\n%s", idx+1, total, chunk))
	}
	return numbered
}
