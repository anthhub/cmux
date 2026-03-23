package channels

import "context"

// Channel defines the interface for IM platform adapters.
// Each platform (Telegram, Slack, Feishu, etc.) implements this interface.
type Channel interface {
	Name() string
	Start(ctx context.Context) error
	Stop() error
	Send(chatID string, msg OutboundMessage) error
	OnMessage(handler func(msg InboundMessage))
}

// TypingCapable channels can keep a typing indicator active until stopped.
type TypingCapable interface {
	SendTyping(chatID string) (stop func(), err error)
}

// StreamingCapable channels support message creation plus in-place edits.
type StreamingCapable interface {
	SendStreaming(chatID string, msg OutboundMessage) (messageID string, err error)
	EditStreaming(chatID, messageID string, msg OutboundMessage) error
}

// MessageLengthProvider reports the largest safe outbound message length.
type MessageLengthProvider interface {
	MaxMessageLength() int
}

// InboundMessage represents a message received from an IM platform.
type InboundMessage struct {
	ChannelName string
	ChatID      string
	UserID      string
	Text        string
	Attachments []Attachment
}

// OutboundMessage represents a message to be sent to an IM platform.
type OutboundMessage struct {
	Text        string
	Format      string // "text" | "markdown" | "code"
	Attachments []Attachment
}

// Attachment represents a file or image attachment.
type Attachment struct {
	Type     string // "image" | "file"
	Data     []byte
	Filename string
	MimeType string
}
