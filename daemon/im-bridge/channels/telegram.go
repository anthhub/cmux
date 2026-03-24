package channels

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
	tele "gopkg.in/telebot.v4"
)

func init() {
	RegisterFactory("telegram", func(cfg interface{}) (Channel, error) {
		tc, ok := cfg.(*config.TelegramConfig)
		if !ok {
			return nil, fmt.Errorf("telegram factory: expected *config.TelegramConfig, got %T", cfg)
		}
		return NewTelegramChannel(tc.BotToken)
	})
}

// TelegramChannel implements Channel for Telegram.
type TelegramChannel struct {
	*BaseChannel
	bot       *tele.Bot
	handlerMu sync.Mutex
	handler   func(msg InboundMessage)
}

// NewTelegramChannel creates a Telegram channel adapter.
func NewTelegramChannel(token string) (*TelegramChannel, error) {
	pref := tele.Settings{
		Token:  token,
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	}

	bot, err := tele.NewBot(pref)
	if err != nil {
		return nil, fmt.Errorf("failed to create telegram bot: %w", err)
	}

	tc := &TelegramChannel{
		BaseChannel: NewBaseChannel("telegram", 4096, nil),
		bot:         bot,
	}

	// Handle all text messages
	bot.Handle(tele.OnText, func(c tele.Context) error {
		tc.handlerMu.Lock()
		h := tc.handler
		tc.handlerMu.Unlock()
		if h == nil {
			return nil
		}

		h(InboundMessage{
			ChatID: strconv.FormatInt(c.Chat().ID, 10),
			UserID: strconv.FormatInt(c.Sender().ID, 10),
			Text:   c.Text(),
		})
		return nil
	})

	return tc, nil
}

func (tc *TelegramChannel) Start(_ context.Context) error {
	log.Printf("[telegram] bot @%s starting", tc.bot.Me.Username)
	tc.SetRunning(true)
	go tc.bot.Start()
	return nil
}

func (tc *TelegramChannel) Stop() error {
	tc.SetRunning(false)
	tc.bot.Stop()
	return nil
}

func (tc *TelegramChannel) Send(chatID string, msg OutboundMessage) error {
	_, err := tc.send(chatID, msg)
	return err
}

func (tc *TelegramChannel) SendStreaming(chatID string, msg OutboundMessage) (string, error) {
	sent, err := tc.send(chatID, msg)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(sent.ID), nil
}

func (tc *TelegramChannel) EditStreaming(chatID, messageID string, msg OutboundMessage) error {
	chatIDInt, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid chat ID: %w", err)
	}

	opts := &tele.SendOptions{}
	text := msg.Text
	if msg.Format == "markdown" || msg.Format == "code" {
		opts.ParseMode = tele.ModeMarkdownV2
		text = escapeMarkdownV2(msg.Text)
	}

	stored := tele.StoredMessage{
		MessageID: messageID,
		ChatID:    chatIDInt,
	}
	_, err = tc.bot.Edit(stored, text, opts)
	if err != nil && opts.ParseMode != "" {
		_, err = tc.bot.Edit(stored, msg.Text)
	}
	return err
}

func (tc *TelegramChannel) SendTyping(chatID string) (func(), error) {
	id, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid chat ID: %w", err)
	}

	chat := &tele.Chat{ID: id}
	if err := tc.bot.Notify(chat, tele.Typing); err != nil {
		return nil, err
	}

	stopCh := make(chan struct{})
	var once sync.Once
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[telegram] typing goroutine panic: %v", r)
			}
		}()
		ticker := time.NewTicker(4 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if err := tc.bot.Notify(chat, tele.Typing); err != nil {
					log.Printf("[telegram] typing notify failed: %v", err)
				}
			}
		}
	}()

	return func() {
		once.Do(func() { close(stopCh) })
	}, nil
}

func (tc *TelegramChannel) send(chatID string, msg OutboundMessage) (*tele.Message, error) {
	id, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid chat ID: %w", err)
	}

	chat := &tele.Chat{ID: id}

	// Handle all attachments
	for _, att := range msg.Attachments {
		switch att.Type {
		case "image":
			photo := &tele.Photo{
				File:    tele.FromReader(bytesReader(att.Data)),
				Caption: msg.Text,
			}
			if _, err := tc.bot.Send(chat, photo); err != nil {
				return nil, fmt.Errorf("send photo: %w", err)
			}
		case "file":
			doc := &tele.Document{
				File:     tele.FromReader(bytesReader(att.Data)),
				FileName: att.Filename,
				Caption:  msg.Text,
			}
			if _, err := tc.bot.Send(chat, doc); err != nil {
				return nil, fmt.Errorf("send document: %w", err)
			}
		}
	}

	// Send text if present
	if msg.Text == "" {
		return nil, nil
	}

	opts := &tele.SendOptions{}
	text := msg.Text
	if msg.Format == "markdown" || msg.Format == "code" {
		opts.ParseMode = tele.ModeMarkdownV2
		text = escapeMarkdownV2(msg.Text)
	}

	sent, err := tc.bot.Send(chat, text, opts)
	if err != nil && opts.ParseMode != "" {
		// Fallback to plain text if escaped markdown still fails
		sent, err = tc.bot.Send(chat, msg.Text)
	}
	return sent, err
}

func (tc *TelegramChannel) OnMessage(handler func(msg InboundMessage)) {
	tc.handlerMu.Lock()
	defer tc.handlerMu.Unlock()
	tc.handler = handler
}

// escapeMarkdownV2 escapes special characters for Telegram MarkdownV2 parse mode,
// preserving content inside code blocks (``` and `) unchanged.
func escapeMarkdownV2(text string) string {
	const specialChars = `_*[]()~` + "`" + `>#+-=|{}.!`

	var b strings.Builder
	b.Grow(len(text))

	i := 0
	for i < len(text) {
		// Check for fenced code block ```
		if i+2 < len(text) && text[i:i+3] == "```" {
			end := strings.Index(text[i+3:], "```")
			if end >= 0 {
				b.WriteString(text[i : i+3+end+3])
				i += 3 + end + 3
				continue
			}
		}
		// Check for inline code `
		if text[i] == '`' {
			end := strings.IndexByte(text[i+1:], '`')
			if end >= 0 {
				b.WriteString(text[i : i+1+end+1])
				i += 1 + end + 1
				continue
			}
		}
		// Escape special characters outside code
		if strings.ContainsRune(specialChars, rune(text[i])) {
			b.WriteByte('\\')
		}
		b.WriteByte(text[i])
		i++
	}
	return b.String()
}

// bytesReader wraps []byte for telebot file upload.
type bytesReaderImpl struct {
	data []byte
	pos  int
}

func bytesReader(data []byte) *bytesReaderImpl {
	return &bytesReaderImpl{data: data}
}

func (r *bytesReaderImpl) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
