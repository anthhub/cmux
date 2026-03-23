package channels

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	tele "gopkg.in/telebot.v4"
)

// TelegramChannel implements Channel for Telegram.
type TelegramChannel struct {
	bot     *tele.Bot
	handler func(msg InboundMessage)
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

	tc := &TelegramChannel{bot: bot}

	// Handle all text messages
	bot.Handle(tele.OnText, func(c tele.Context) error {
		if tc.handler == nil {
			return nil
		}

		tc.handler(InboundMessage{
			ChatID: strconv.FormatInt(c.Chat().ID, 10),
			UserID: strconv.FormatInt(c.Sender().ID, 10),
			Text:   c.Text(),
		})
		return nil
	})

	return tc, nil
}

func (tc *TelegramChannel) Name() string {
	return "telegram"
}

func (tc *TelegramChannel) Start(_ context.Context) error {
	log.Printf("[telegram] bot @%s starting", tc.bot.Me.Username)
	go tc.bot.Start()
	return nil
}

func (tc *TelegramChannel) Stop() error {
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
	if msg.Format == "markdown" || msg.Format == "code" {
		opts.ParseMode = tele.ModeMarkdownV2
	}

	stored := tele.StoredMessage{
		MessageID: messageID,
		ChatID:    chatIDInt,
	}
	_, err = tc.bot.Edit(stored, msg.Text, opts)
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

func (tc *TelegramChannel) MaxMessageLength() int {
	return 4096
}

func (tc *TelegramChannel) send(chatID string, msg OutboundMessage) (*tele.Message, error) {
	id, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid chat ID: %w", err)
	}

	chat := &tele.Chat{ID: id}

	// Handle attachments (images)
	if len(msg.Attachments) > 0 {
		for _, att := range msg.Attachments {
			if att.Type == "image" {
				photo := &tele.Photo{
					File:    tele.FromReader(bytesReader(att.Data)),
					Caption: msg.Text,
				}
				sent, err := tc.bot.Send(chat, photo)
				return sent, err
			}
		}
	}

	// Send text with appropriate parse mode
	opts := &tele.SendOptions{}
	if msg.Format == "markdown" || msg.Format == "code" {
		opts.ParseMode = tele.ModeMarkdownV2
	}

	// For code format, the text is already wrapped in ```
	// For plain text, send as-is
	sent, err := tc.bot.Send(chat, msg.Text, opts)
	if err != nil {
		// Retry without parse mode if markdown fails
		sent, err = tc.bot.Send(chat, msg.Text)
	}
	return sent, err
}

func (tc *TelegramChannel) OnMessage(handler func(msg InboundMessage)) {
	tc.handler = handler
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
