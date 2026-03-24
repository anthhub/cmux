//go:build feishu

// Requires: github.com/larksuite/oapi-sdk-go/v3

package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
)

func init() {
	RegisterFactory("feishu", func(cfg interface{}) (Channel, error) {
		fc, ok := cfg.(*feishuFactoryCfg)
		if !ok {
			return nil, fmt.Errorf("feishu factory: expected *feishuFactoryCfg, got %T", cfg)
		}
		return NewFeishuChannel(fc.AppID, fc.AppSecret)
	})
}

// feishuFactoryCfg is the config type passed to the feishu factory.
type feishuFactoryCfg struct {
	AppID     string
	AppSecret string
}

// FeishuChannel implements Channel for Feishu/Lark.
type FeishuChannel struct {
	*BaseChannel
	client  *lark.Client
	handler func(InboundMessage)
	cancel  context.CancelFunc
}

// NewFeishuChannel creates a Feishu/Lark channel adapter.
func NewFeishuChannel(appID, appSecret string) (*FeishuChannel, error) {
	client := lark.NewClient(appID, appSecret)
	ch := &FeishuChannel{
		BaseChannel: NewBaseChannel("feishu", 30720, nil),
		client:      client,
	}
	return ch, nil
}

func (fc *FeishuChannel) Start(ctx context.Context) error {
	fc.SetRunning(true)
	runCtx, cancel := context.WithCancel(ctx)
	fc.cancel = cancel

	// Build event dispatcher for receiving messages.
	eventDisp := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(func(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
			if fc.handler == nil {
				return nil
			}
			msg := event.Event.Message
			if msg == nil {
				return nil
			}
			text := extractFeishuText(msg.Content)
			senderID := ""
			if event.Event.Sender != nil && event.Event.Sender.SenderId != nil {
				senderID = larkcore.StringValue(event.Event.Sender.SenderId.OpenId)
			}
			chatID := larkcore.StringValue(msg.ChatId)
			fc.handler(InboundMessage{
				ChatID: chatID,
				UserID: senderID,
				Text:   text,
			})
			return nil
		})

	go func() {
		cli := larkevent.NewEventServer()
		cli.Use(eventDisp)
		if err := cli.Start(runCtx); err != nil && runCtx.Err() == nil {
			log.Printf("[feishu] event server exited: %v", err)
		}
	}()

	return nil
}

func (fc *FeishuChannel) Stop() error {
	fc.SetRunning(false)
	if fc.cancel != nil {
		fc.cancel()
	}
	return nil
}

func (fc *FeishuChannel) Send(chatID string, msg OutboundMessage) error {
	var content string
	var msgType string

	if msg.Format == "code" {
		// Send as interactive card with code block.
		card := buildFeishuCodeCard(msg.Text)
		b, err := json.Marshal(card)
		if err != nil {
			return fmt.Errorf("feishu marshal card: %w", err)
		}
		content = string(b)
		msgType = "interactive"
	} else {
		// Plain text or markdown as text message.
		b, err := json.Marshal(map[string]string{"text": msg.Text})
		if err != nil {
			return fmt.Errorf("feishu marshal text: %w", err)
		}
		content = string(b)
		msgType = "text"
	}

	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType("chat_id").
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(chatID).
			MsgType(msgType).
			Content(content).
			Build()).
		Build()

	resp, err := fc.client.Im.Message.Create(context.Background(), req)
	if err != nil {
		return fmt.Errorf("feishu send: %w", err)
	}
	if !resp.Success() {
		return fmt.Errorf("feishu send error: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

func (fc *FeishuChannel) SendStreaming(chatID string, msg OutboundMessage) (string, error) {
	b, err := json.Marshal(map[string]string{"text": msg.Text})
	if err != nil {
		return "", fmt.Errorf("feishu marshal: %w", err)
	}

	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType("chat_id").
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(chatID).
			MsgType("text").
			Content(string(b)).
			Build()).
		Build()

	resp, err := fc.client.Im.Message.Create(context.Background(), req)
	if err != nil {
		return "", fmt.Errorf("feishu send streaming: %w", err)
	}
	if !resp.Success() {
		return "", fmt.Errorf("feishu send streaming error: code=%d msg=%s", resp.Code, resp.Msg)
	}
	msgID := ""
	if resp.Data != nil && resp.Data.MessageId != nil {
		msgID = larkcore.StringValue(resp.Data.MessageId)
	}
	return msgID, nil
}

func (fc *FeishuChannel) EditStreaming(chatID, messageID string, msg OutboundMessage) error {
	b, err := json.Marshal(map[string]string{"text": msg.Text})
	if err != nil {
		return fmt.Errorf("feishu marshal: %w", err)
	}

	req := larkim.NewPatchMessageReqBuilder().
		MessageId(messageID).
		Body(larkim.NewPatchMessageReqBodyBuilder().
			MsgType("text").
			Content(string(b)).
			Build()).
		Build()

	resp, err := fc.client.Im.Message.Patch(context.Background(), req)
	if err != nil {
		return fmt.Errorf("feishu edit streaming: %w", err)
	}
	if !resp.Success() {
		return fmt.Errorf("feishu edit streaming error: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

func (fc *FeishuChannel) OnMessage(handler func(InboundMessage)) {
	fc.handler = handler
}

// extractFeishuText parses the JSON content of a Feishu text message.
func extractFeishuText(content *string) string {
	if content == nil {
		return ""
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(*content), &m); err != nil {
		return *content
	}
	if t, ok := m["text"]; ok {
		return t
	}
	return *content
}

// buildFeishuCodeCard builds a Feishu interactive card containing a code block.
func buildFeishuCodeCard(code string) map[string]interface{} {
	return map[string]interface{}{
		"config": map[string]interface{}{
			"wide_screen_mode": true,
		},
		"elements": []interface{}{
			map[string]interface{}{
				"tag":     "markdown",
				"content": "```\n" + code + "\n```",
			},
		},
	}
}
