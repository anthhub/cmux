//go:build slack

// Requires: github.com/slack-go/slack

package channels

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

func init() {
	RegisterFactory("slack", func(cfg interface{}) (Channel, error) {
		sc, ok := cfg.(*slackFactoryCfg)
		if !ok {
			return nil, fmt.Errorf("slack factory: expected *slackFactoryCfg, got %T", cfg)
		}
		return NewSlackChannel(sc.BotToken, sc.AppToken)
	})
}

// slackFactoryCfg is the config type passed to the slack factory.
type slackFactoryCfg struct {
	BotToken string
	AppToken string
}

// SlackChannel implements Channel for Slack using Socket Mode.
type SlackChannel struct {
	*BaseChannel
	api       *slack.Client
	socket    *socketmode.Client
	handlerMu sync.Mutex
	handler   func(InboundMessage)
	cancel    context.CancelFunc
}

// NewSlackChannel creates a Slack channel adapter using Socket Mode.
// botToken is the xoxb-... token; appToken is the xapp-... token required for Socket Mode.
func NewSlackChannel(botToken, appToken string) (*SlackChannel, error) {
	api := slack.New(botToken, slack.OptionAppLevelToken(appToken))
	socket := socketmode.New(api)

	ch := &SlackChannel{
		BaseChannel: NewBaseChannel("slack", 40000, nil),
		api:         api,
		socket:      socket,
	}
	return ch, nil
}

func (sc *SlackChannel) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	sc.cancel = cancel
	sc.SetRunning(true)
	go sc.run(ctx)
	return nil
}

func (sc *SlackChannel) run(ctx context.Context) {
	go func() {
		if err := sc.socket.RunContext(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[slack] socket mode exited: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-sc.socket.Events:
			if !ok {
				return
			}
			sc.handleEvent(evt)
		}
	}
}

func (sc *SlackChannel) handleEvent(evt socketmode.Event) {
	switch evt.Type {
	case socketmode.EventTypeEventsAPI:
		sc.socket.Ack(*evt.Request)
		eventsAPI, ok := evt.Data.(slack.EventsAPIEvent)
		if !ok {
			return
		}
		if eventsAPI.Type == slack.EventsAPIType("message") {
			inner, ok := eventsAPI.InnerEvent.Data.(*slack.MessageEvent)
			if !ok {
				return
			}
			// Ignore bot messages to avoid feedback loops.
			if inner.BotID != "" {
				return
			}
			sc.handlerMu.Lock()
			h := sc.handler
			sc.handlerMu.Unlock()
			if h != nil {
				h(InboundMessage{
					ChatID: inner.Channel,
					UserID: inner.User,
					Text:   inner.Text,
				})
			}
		}
	case socketmode.EventTypeInteractive:
		sc.socket.Ack(*evt.Request)
	}
}

func (sc *SlackChannel) Stop() error {
	sc.SetRunning(false)
	if sc.cancel != nil {
		sc.cancel()
	}
	return nil
}

func (sc *SlackChannel) Send(chatID string, msg OutboundMessage) error {
	_, _, err := sc.api.PostMessage(chatID, slack.MsgOptionText(msg.Text, false))
	if err != nil {
		return fmt.Errorf("slack send: %w", err)
	}
	return nil
}

func (sc *SlackChannel) SendStreaming(chatID string, msg OutboundMessage) (string, error) {
	_, ts, err := sc.api.PostMessage(chatID, slack.MsgOptionText(msg.Text, false))
	if err != nil {
		return "", fmt.Errorf("slack send streaming: %w", err)
	}
	return ts, nil
}

func (sc *SlackChannel) EditStreaming(chatID, messageID string, msg OutboundMessage) error {
	_, _, _, err := sc.api.UpdateMessage(chatID, messageID, slack.MsgOptionText(msg.Text, false))
	if err != nil {
		return fmt.Errorf("slack edit streaming: %w", err)
	}
	return nil
}

func (sc *SlackChannel) SendTyping(chatID string) (func(), error) {
	// Slack's typing indicator is fire-and-forget; no sustained loop needed.
	if err := sc.api.SetUserPresence("auto"); err != nil {
		log.Printf("[slack] set user presence: %v", err)
	}
	return func() {}, nil
}

func (sc *SlackChannel) OnMessage(handler func(InboundMessage)) {
	sc.handlerMu.Lock()
	defer sc.handlerMu.Unlock()
	sc.handler = handler
}
