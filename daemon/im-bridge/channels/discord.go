//go:build discord

// Requires: github.com/bwmarrin/discordgo

package channels

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/bwmarrin/discordgo"
	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

func init() {
	RegisterFactory("discord", func(cfg interface{}) (Channel, error) {
		dc, ok := cfg.(*config.DiscordConfig)
		if !ok {
			return nil, fmt.Errorf("discord factory: expected *config.DiscordConfig, got %T", cfg)
		}
		return NewDiscordChannel(dc.BotToken)
	})
}

// DiscordChannel implements Channel for Discord.
type DiscordChannel struct {
	*BaseChannel
	session   *discordgo.Session
	handlerMu sync.Mutex
	handler   func(InboundMessage)
}

// NewDiscordChannel creates a Discord channel adapter.
func NewDiscordChannel(token string) (*DiscordChannel, error) {
	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		return nil, fmt.Errorf("discord create session: %w", err)
	}

	ch := &DiscordChannel{
		BaseChannel: NewBaseChannel("discord", 2000, nil),
		session:     dg,
	}

	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		// Ignore messages from the bot itself.
		if m.Author.ID == s.State.User.ID {
			return
		}
		ch.handlerMu.Lock()
		h := ch.handler
		ch.handlerMu.Unlock()
		if h != nil {
			h(InboundMessage{
				ChatID: m.ChannelID,
				UserID: m.Author.ID,
				Text:   m.Content,
			})
		}
	})

	return ch, nil
}

func (dc *DiscordChannel) Start(_ context.Context) error {
	dc.SetRunning(true)
	if err := dc.session.Open(); err != nil {
		dc.SetRunning(false)
		return fmt.Errorf("discord open: %w", err)
	}
	log.Printf("[discord] bot %s connected", dc.session.State.User.Username)
	return nil
}

func (dc *DiscordChannel) Stop() error {
	dc.SetRunning(false)
	if err := dc.session.Close(); err != nil {
		return fmt.Errorf("discord close: %w", err)
	}
	return nil
}

func (dc *DiscordChannel) Send(chatID string, msg OutboundMessage) error {
	text := msg.Text
	if msg.Format == "code" {
		text = "```\n" + msg.Text + "\n```"
	}
	if _, err := dc.session.ChannelMessageSend(chatID, text); err != nil {
		return fmt.Errorf("discord send: %w", err)
	}
	return nil
}

func (dc *DiscordChannel) SendStreaming(chatID string, msg OutboundMessage) (string, error) {
	text := msg.Text
	if msg.Format == "code" {
		text = "```\n" + msg.Text + "\n```"
	}
	sent, err := dc.session.ChannelMessageSend(chatID, text)
	if err != nil {
		return "", fmt.Errorf("discord send streaming: %w", err)
	}
	return sent.ID, nil
}

func (dc *DiscordChannel) EditStreaming(chatID, messageID string, msg OutboundMessage) error {
	text := msg.Text
	if msg.Format == "code" {
		text = "```\n" + msg.Text + "\n```"
	}
	if _, err := dc.session.ChannelMessageEdit(chatID, messageID, text); err != nil {
		return fmt.Errorf("discord edit: %w", err)
	}
	return nil
}

func (dc *DiscordChannel) OnMessage(handler func(InboundMessage)) {
	dc.handlerMu.Lock()
	defer dc.handlerMu.Unlock()
	dc.handler = handler
}
