package channels

import (
	"context"
	"fmt"
	"log"
	"strconv"
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
	id, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid chat ID: %w", err)
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
				_, err := tc.bot.Send(chat, photo)
				return err
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
	_, err = tc.bot.Send(chat, msg.Text, opts)
	if err != nil {
		// Retry without parse mode if markdown fails
		_, err = tc.bot.Send(chat, msg.Text)
	}
	return err
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
		return 0, fmt.Errorf("EOF")
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
