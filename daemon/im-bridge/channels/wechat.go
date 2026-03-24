package channels

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

const (
	wechatDefaultBaseURL = "https://ilinkai.weixin.qq.com"
	wechatMaxMsgLen      = 4096
)

func init() {
	RegisterFactory("wechat", func(cfg interface{}) (Channel, error) {
		wc, ok := cfg.(*config.WeChatConfig)
		if !ok {
			return nil, fmt.Errorf("wechat factory: expected *config.WeChatConfig, got %T", cfg)
		}
		return NewWeChatChannel(wc.BotToken, wc.CredentialsDir)
	})
}

// WeChatChannel implements Channel for WeChat via the iLink bot API.
type WeChatChannel struct {
	*BaseChannel
	botToken   string
	botID      string
	baseURL    string
	uin        string
	httpClient *http.Client
	handler    func(InboundMessage)
	syncBuf    string
	credsDir   string
	cancel     context.CancelFunc
	mu         sync.Mutex
}

// NewWeChatChannel creates a WeChat channel adapter.
func NewWeChatChannel(botToken, credsDir string) (*WeChatChannel, error) {
	n, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	uin := fmt.Sprintf("%d", n.Int64())

	w := &WeChatChannel{
		BaseChannel: NewBaseChannel("wechat", wechatMaxMsgLen, nil),
		botToken:    botToken,
		baseURL:     wechatDefaultBaseURL,
		uin:         uin,
		credsDir:    credsDir,
		httpClient: &http.Client{
			Timeout: 40 * time.Second,
		},
	}

	// Try loading saved credentials if no token provided.
	if w.botToken == "" && w.credsDir != "" {
		w.loadCredentials()
	}

	return w, nil
}

func (w *WeChatChannel) Start(ctx context.Context) error {
	if w.botToken == "" {
		if err := w.qrLogin(ctx); err != nil {
			return fmt.Errorf("wechat: qr login failed: %w", err)
		}
	}

	log.Printf("[wechat] starting with base URL %s", w.baseURL)
	w.SetRunning(true)

	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	go w.pollLoop(ctx)

	return nil
}

func (w *WeChatChannel) Stop() error {
	w.SetRunning(false)
	if w.cancel != nil {
		w.cancel()
	}
	log.Println("[wechat] stopped")
	return nil
}

func (w *WeChatChannel) Send(chatID string, msg OutboundMessage) error {
	clientID, err := newUUID()
	if err != nil {
		return fmt.Errorf("wechat: generate clientID: %w", err)
	}

	w.mu.Lock()
	botID := w.botID
	w.mu.Unlock()

	req := wechatSendMsgReq{
		BaseInfo: wechatBaseInfo{ChannelVersion: ""},
		Message: wechatSendMsg{
			ToWeixinID:   chatID,
			FromWeixinID: botID,
			MsgType:      2,
			MsgState:     2,
			Items:        []wechatMsgItem{{Text: &wechatTextItem{Content: msg.Text}}},
			ClientID:     clientID,
		},
	}

	var resp wechatSendMsgResp
	if err := w.doRequest(context.Background(), http.MethodPost, "/ilink/bot/sendmessage", req, &resp); err != nil {
		return fmt.Errorf("wechat: send: %w", err)
	}
	if resp.Errcode != 0 {
		return fmt.Errorf("wechat: send error %d: %s", resp.Errcode, resp.Errmsg)
	}
	return nil
}

func (w *WeChatChannel) OnMessage(handler func(InboundMessage)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handler = handler
}

// pollLoop runs the long-polling getUpdates loop.
func (w *WeChatChannel) pollLoop(ctx context.Context) {
	backoff := 3 * time.Second
	const maxBackoff = 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		req := wechatGetUpdatesReq{
			BaseInfo:      wechatBaseInfo{ChannelVersion: ""},
			GetUpdatesBuf: w.syncBuf,
		}

		var resp wechatGetUpdatesResp
		err := w.doRequest(ctx, http.MethodPost, "/ilink/bot/getupdates", req, &resp)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[wechat] getUpdates error: %v, retrying in %v", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Handle session expired error.
		if resp.Errcode == -14 {
			log.Println("[wechat] session expired (errcode -14), resetting sync buffer")
			w.syncBuf = ""
			backoff = 3 * time.Second
			continue
		}

		if resp.Errcode != 0 {
			log.Printf("[wechat] getUpdates errcode %d: %s, retrying in %v", resp.Errcode, resp.Errmsg, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Success — reset backoff and update sync buffer.
		backoff = 3 * time.Second
		if resp.GetUpdatesBuf != "" {
			w.syncBuf = resp.GetUpdatesBuf
		}

		w.mu.Lock()
		handler := w.handler
		w.mu.Unlock()

		for _, msg := range resp.Messages {
			// Only process user messages (MsgType=1) that are new or finished (MsgState=0 or 2).
			if msg.MsgType != 1 {
				continue
			}
			if msg.MsgState != 0 && msg.MsgState != 2 {
				continue
			}

			// Infer bot ID from the first message received.
			if w.botID == "" && msg.ToWeixinID != "" {
				w.mu.Lock()
				w.botID = msg.ToWeixinID
				w.mu.Unlock()
			}

			text := extractText(msg.Items)
			if text == "" {
				continue
			}

			if handler != nil {
				handler(InboundMessage{
					ChannelName: "wechat",
					ChatID:      msg.FromWeixinID,
					UserID:      msg.FromWeixinID,
					Text:        text,
				})
			}
		}
	}
}

// doRequest sends an HTTP request with authentication headers and JSON body.
func (w *WeChatChannel) doRequest(ctx context.Context, method, path string, body, result interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, w.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	w.setAuthHeaders(req)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

// qrLogin performs QR code login to obtain a bot token.
func (w *WeChatChannel) qrLogin(ctx context.Context) error {
	log.Println("[wechat] no bot token, starting QR code login...")

	var qrResp wechatQRCodeResp
	if err := w.doRequest(ctx, http.MethodGet, "/ilink/bot/get_bot_qrcode?bot_type=3", nil, &qrResp); err != nil {
		return fmt.Errorf("get qr code: %w", err)
	}

	log.Printf("[wechat] QR code obtained (ID: %s). Scan to login.", qrResp.QRCodeID)
	if qrResp.ImageContent != "" {
		log.Printf("[wechat] QR image data length: %d bytes (base64)", len(qrResp.ImageContent))
	}

	// Poll for QR code status.
	pollURL := fmt.Sprintf("/ilink/bot/get_qrcode_status?qrcode=%s", qrResp.QRCodeID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var statusResp wechatQRStatusResp
		if err := w.doRequest(ctx, http.MethodGet, pollURL, nil, &statusResp); err != nil {
			log.Printf("[wechat] qr status poll error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		switch statusResp.Status {
		case "confirmed":
			w.botToken = statusResp.BotToken
			w.botID = statusResp.ILinkBotID
			if statusResp.BaseURL != "" {
				w.baseURL = statusResp.BaseURL
			}
			log.Printf("[wechat] QR login successful, bot ID: %s", w.botID)
			w.saveCredentials()
			return nil
		case "expired":
			return fmt.Errorf("QR code expired")
		case "scanned":
			log.Println("[wechat] QR code scanned, waiting for confirmation...")
		default:
			// "wait" or other — keep polling.
		}

		time.Sleep(2 * time.Second)
	}
}

func (w *WeChatChannel) setAuthHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if w.botToken != "" {
		req.Header.Set("Authorization", "Bearer "+w.botToken)
	}
	req.Header.Set("AuthorizationType", "ilink_bot_token")
	req.Header.Set("X-WECHAT-UIN", w.uin)
}

// saveCredentials persists the bot token and related info to credsDir.
func (w *WeChatChannel) saveCredentials() {
	if w.credsDir == "" {
		return
	}
	if err := os.MkdirAll(w.credsDir, 0700); err != nil {
		log.Printf("[wechat] failed to create creds dir: %v", err)
		return
	}
	creds := map[string]string{
		"bot_token": w.botToken,
		"bot_id":    w.botID,
		"base_url":  w.baseURL,
	}
	data, _ := json.Marshal(creds)
	path := filepath.Join(w.credsDir, "wechat_credentials.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		log.Printf("[wechat] failed to save credentials: %v", err)
	} else {
		log.Printf("[wechat] credentials saved to %s", path)
	}
}

// loadCredentials reads saved credentials from credsDir.
func (w *WeChatChannel) loadCredentials() {
	path := filepath.Join(w.credsDir, "wechat_credentials.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var creds map[string]string
	if err := json.Unmarshal(data, &creds); err != nil {
		return
	}
	if token, ok := creds["bot_token"]; ok && token != "" {
		w.botToken = token
	}
	if id, ok := creds["bot_id"]; ok && id != "" {
		w.botID = id
	}
	if url, ok := creds["base_url"]; ok && url != "" {
		w.baseURL = url
	}
	if w.botToken != "" {
		log.Printf("[wechat] loaded saved credentials from %s", path)
	}
}

// extractText concatenates text content from message items.
func extractText(items []wechatMsgItem) string {
	var text string
	for _, item := range items {
		if item.Text != nil {
			text += item.Text.Content
		}
	}
	return text
}

// newUUID generates a UUID v4 string without external dependencies.
func newUUID() (string, error) {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

// --- Wire types ---

type wechatBaseInfo struct {
	ChannelVersion string `json:"channelVersion"`
}

type wechatGetUpdatesReq struct {
	BaseInfo      wechatBaseInfo `json:"baseInfo"`
	GetUpdatesBuf string         `json:"getUpdatesBuf"`
}

type wechatGetUpdatesResp struct {
	Errcode       int              `json:"errcode"`
	Errmsg        string           `json:"errmsg"`
	Messages      []wechatMessage  `json:"messages"`
	GetUpdatesBuf string           `json:"getUpdatesBuf"`
}

type wechatMessage struct {
	FromWeixinID string          `json:"fromWeixinID"`
	ToWeixinID   string          `json:"toWeixinID"`
	MsgType      int             `json:"msgType"`
	MsgState     int             `json:"msgState"`
	Items        []wechatMsgItem `json:"items"`
}

type wechatMsgItem struct {
	Text  *wechatTextItem  `json:"text,omitempty"`
	Image *wechatImageItem `json:"image,omitempty"`
}

type wechatTextItem struct {
	Content string `json:"content"`
}

type wechatImageItem struct {
	URL string `json:"url,omitempty"`
}

type wechatSendMsgReq struct {
	BaseInfo wechatBaseInfo `json:"baseInfo"`
	Message  wechatSendMsg  `json:"message"`
}

type wechatSendMsg struct {
	ToWeixinID   string          `json:"toWeixinID"`
	FromWeixinID string          `json:"fromWeixinID"`
	MsgType      int             `json:"msgType"`
	MsgState     int             `json:"msgState"`
	Items        []wechatMsgItem `json:"items"`
	ClientID     string          `json:"clientID"`
}

type wechatSendMsgResp struct {
	Errcode int    `json:"errcode"`
	Errmsg  string `json:"errmsg"`
}

type wechatQRCodeResp struct {
	QRCodeID     string `json:"qrcodeID"`
	ImageContent string `json:"imageContent"`
}

type wechatQRStatusResp struct {
	Status      string `json:"status"`
	BotToken    string `json:"botToken"`
	ILinkBotID  string `json:"ilinkBotID"`
	BaseURL     string `json:"baseURL"`
	ILinkUserID string `json:"ilinkUserID"`
}
