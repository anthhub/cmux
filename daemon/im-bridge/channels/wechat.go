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
	wechatDefaultBaseURL     = "https://ilinkai.weixin.qq.com"
	wechatMaxMsgLen          = 4096
	wechatDefaultCredsSubdir = ".weclaw/accounts"
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
	resolvedCredsDir := resolveWeChatCredentialsDir(credsDir)

	w := &WeChatChannel{
		BaseChannel: NewBaseChannel("wechat", wechatMaxMsgLen, nil),
		baseURL:     wechatDefaultBaseURL,
		uin:         uin,
		credsDir:    resolvedCredsDir,
		httpClient: &http.Client{
			Timeout: 40 * time.Second,
		},
	}

	if w.credsDir != "" {
		w.loadCredentials()
	}
	if botToken != "" {
		w.botToken = botToken
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
		BaseInfo: wechatBaseInfo{ChannelVersion: "1.0.0"},
		Message: wechatSendMsg{
			ToUserID:     chatID,
			FromUserID:   botID,
			MsgType:      2,
			MsgState:     2,
			Items:        []wechatMsgItem{{Type: 1, TextItem: &wechatTextItem{Text: msg.Text}}},
			ClientID:     clientID,
			ContextToken: msg.ContextToken,
		},
	}

	// Debug: log the request
	reqJSON, _ := json.Marshal(req)
	log.Printf("[wechat-debug] sendMessage req: %s", string(reqJSON))

	var resp wechatSendMsgResp
	if err := w.doRequest(context.Background(), http.MethodPost, "/ilink/bot/sendmessage", req, &resp); err != nil {
		log.Printf("[wechat] send error: %v", err)
		return fmt.Errorf("wechat: send: %w", err)
	}
	log.Printf("[wechat-debug] sendMessage resp: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.Errcode, resp.Errmsg)
	if resp.Ret != 0 {
		return fmt.Errorf("wechat: send error ret=%d errcode=%d: %s", resp.Ret, resp.Errcode, resp.Errmsg)
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
			BaseInfo:      wechatBaseInfo{ChannelVersion: "1.0.0"},
			GetUpdatesBuf: w.syncBuf,
		}

		var resp wechatGetUpdatesResp
		err := w.doRequestDebug(ctx, http.MethodPost, "/ilink/bot/getupdates", req, &resp)
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
			w.saveCredentials()
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
			w.saveCredentials()
		}
		if len(resp.Messages) > 0 {
			log.Printf("[wechat] received %d messages", len(resp.Messages))
		}

		w.mu.Lock()
		handler := w.handler
		w.mu.Unlock()

		for _, msg := range resp.Messages {
			// Only process user messages (message_type=1) that are finished (message_state=0 or 2).
			if msg.MsgType.String() != "1" {
				continue
			}
			if msg.MsgState.String() != "0" && msg.MsgState.String() != "2" {
				continue
			}

			// Infer bot ID from the first message received.
			if w.botID == "" && msg.ToUserID != "" {
				w.mu.Lock()
				w.botID = msg.ToUserID
				w.mu.Unlock()
			}

			text := extractText(msg.Items)
			if text == "" {
				continue
			}

			if handler != nil {
				handler(InboundMessage{
					ChannelName:  "wechat",
					ChatID:       msg.FromUserID,
					UserID:       msg.FromUserID,
					Text:         text,
					ContextToken: msg.ContextToken,
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

// doRequestDebug is like doRequest but logs the raw response (for debugging).
func (w *WeChatChannel) doRequestDebug(ctx context.Context, method, path string, body, result interface{}) error {
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

	// Debug: log raw response (truncated)
	raw := string(respBody)
	if len(raw) > 2000 {
		raw = raw[:2000] + "..."
	}
	if len(raw) > 20 { // skip empty polls
		log.Printf("[wechat-debug] getUpdates raw: %s", raw)
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

// qrLogin performs QR code login to obtain a bot token. Auto-retries on expiry.
func (w *WeChatChannel) qrLogin(ctx context.Context) error {
	log.Println("[wechat] no bot token, starting QR code login...")

	const maxRetries = 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		var qrResp wechatQRCodeResp
		if err := w.doRequest(ctx, http.MethodGet, "/ilink/bot/get_bot_qrcode?bot_type=3", nil, &qrResp); err != nil {
			return fmt.Errorf("get qr code: %w", err)
		}

		log.Printf("[wechat] ========================================")
		log.Printf("[wechat] Scan this QR code to login WeChat:")
		log.Printf("[wechat] %s", qrResp.ImageContent)
		log.Printf("[wechat] ========================================")

		// Poll for QR code status.
		pollURL := fmt.Sprintf("/ilink/bot/get_qrcode_status?qrcode=%s", qrResp.QRCodeID)
		expired := false
		for !expired {
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
				log.Println("[wechat] QR code expired, generating new one...")
				expired = true
			case "scanned":
				log.Println("[wechat] QR code scanned, waiting for confirmation...")
			default:
				// "wait" or other — keep polling.
			}

			if !expired {
				time.Sleep(2 * time.Second)
			}
		}
	}
	return fmt.Errorf("QR login failed after %d attempts", maxRetries)
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
	creds := wechatCredentials{
		BotToken: w.botToken,
		BotID:    w.botID,
		BaseURL:  w.baseURL,
		SyncBuf:  w.syncBuf,
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
	var creds wechatCredentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return
	}
	if creds.BotToken != "" {
		w.botToken = creds.BotToken
	}
	if creds.BotID != "" {
		w.botID = creds.BotID
	}
	if creds.BaseURL != "" {
		w.baseURL = creds.BaseURL
	}
	if creds.SyncBuf != "" {
		w.syncBuf = creds.SyncBuf
	}
	if w.botToken != "" {
		log.Printf("[wechat] loaded saved credentials from %s", path)
	}
}

func resolveWeChatCredentialsDir(credsDir string) string {
	if credsDir != "" {
		return credsDir
	}
	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		return credsDir
	}
	return filepath.Join(homeDir, wechatDefaultCredsSubdir)
}

// extractText concatenates text content from message items.
func extractText(items []wechatMsgItem) string {
	var text string
	for _, item := range items {
		if item.TextItem != nil && item.TextItem.Text != "" {
			text += item.TextItem.Text
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
	ChannelVersion string `json:"channel_version"`
}

type wechatGetUpdatesReq struct {
	BaseInfo      wechatBaseInfo `json:"base_info"`
	GetUpdatesBuf string         `json:"get_updates_buf"`
}

type wechatGetUpdatesResp struct {
	Errcode       int             `json:"errcode"`
	Errmsg        string          `json:"errmsg"`
	Messages      []wechatMessage `json:"msgs"`
	GetUpdatesBuf string          `json:"get_updates_buf"`
}

type wechatMessage struct {
	Seq          int             `json:"seq"`
	MessageID    int64           `json:"message_id"`
	FromUserID   string          `json:"from_user_id"`
	ToUserID     string          `json:"to_user_id"`
	ClientID     string          `json:"client_id"`
	MsgType      json.Number     `json:"message_type"`
	MsgState     json.Number     `json:"message_state"`
	Items        []wechatMsgItem `json:"item_list"`
	ContextToken string          `json:"context_token"`
	CreateTimeMs int64           `json:"create_time_ms"`
}

type wechatMsgItem struct {
	Type     int              `json:"type"` // 1=text, 2=image
	TextItem *wechatTextItem  `json:"text_item,omitempty"`
	Image    *wechatImageItem `json:"image_item,omitempty"`
}

type wechatTextItem struct {
	Text string `json:"text"`
}

type wechatImageItem struct {
	URL string `json:"url,omitempty"`
}

type wechatSendMsgReq struct {
	BaseInfo wechatBaseInfo `json:"base_info"`
	Message  wechatSendMsg  `json:"message"`
}

type wechatSendMsg struct {
	ToUserID     string          `json:"to_user_id"`
	FromUserID   string          `json:"from_user_id"`
	MsgType      int             `json:"message_type"`
	MsgState     int             `json:"message_state"`
	Items        []wechatMsgItem `json:"item_list"`
	ClientID     string          `json:"client_id"`
	ContextToken string          `json:"context_token,omitempty"`
}

type wechatSendMsgResp struct {
	Ret     int    `json:"ret"`
	Errcode int    `json:"errcode"`
	Errmsg  string `json:"errmsg"`
}

type wechatQRCodeResp struct {
	QRCodeID     string `json:"qrcode"`
	ImageContent string `json:"qrcode_img_content"`
	Ret          int    `json:"ret"`
}

type wechatQRStatusResp struct {
	Ret         int    `json:"ret"`
	Status      string `json:"status"`        // "wait", "scanned", "confirmed", "expired"
	BotToken    string `json:"bot_token"`     // available when confirmed
	ILinkBotID  string `json:"ilink_bot_id"`  // available when confirmed
	BaseURL     string `json:"base_url"`      // available when confirmed
	ILinkUserID string `json:"ilink_user_id"` // available when confirmed
}

type wechatCredentials struct {
	BotToken string `json:"bot_token"`
	BotID    string `json:"bot_id"`
	BaseURL  string `json:"base_url"`
	SyncBuf  string `json:"sync_buf"`
}
