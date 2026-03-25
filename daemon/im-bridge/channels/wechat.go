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
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

const (
	wechatDefaultBaseURL     = "https://ilinkai.weixin.qq.com"
	wechatMaxMsgLen          = 4096
	wechatDefaultCredsSubdir = ".weclaw/accounts"
)

var wechatMarkdownImageRe = regexp.MustCompile(`!\[[^\]]*\]\(([^)]+)\)`)

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
	ctx        context.Context
	cancel     context.CancelFunc
	relogging  bool
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
	w.mu.Lock()
	w.ctx = ctx
	w.cancel = cancel
	w.mu.Unlock()
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
	relogging := w.relogging
	w.mu.Unlock()

	if relogging {
		return fmt.Errorf("wechat: re-login in progress, try again later")
	}

	text, imageURLs, fallbackText := normalizeWeChatOutbound(msg.Text, msg.Attachments)
	items := make([]wechatMsgItem, 0, 1+len(imageURLs)+1)
	if text != "" {
		items = append(items, wechatMsgItem{
			Type:     1,
			TextItem: &wechatTextItem{Text: text},
		})
	}
	for _, imageURL := range imageURLs {
		items = append(items, wechatMsgItem{
			Type: 2,
			Image: &wechatImageItem{
				URL: imageURL,
			},
		})
	}
	if fallbackText != "" {
		if text == "" {
			items = append([]wechatMsgItem{{
				Type:     1,
				TextItem: &wechatTextItem{Text: fallbackText},
			}}, items...)
		} else {
			items = append(items, wechatMsgItem{
				Type:     1,
				TextItem: &wechatTextItem{Text: fallbackText},
			})
		}
	}
	if len(items) == 0 {
		items = append(items, wechatMsgItem{
			Type:     1,
			TextItem: &wechatTextItem{Text: ""},
		})
	}

	req := wechatSendMsgReq{
		BaseInfo: wechatBaseInfo{ChannelVersion: "1.0.0"},
		Message: wechatSendMsg{
			ToUserID:     chatID,
			FromUserID:   botID,
			MsgType:      2,
			MsgState:     2,
			Items:        items,
			ClientID:     clientID,
			ContextToken: msg.ContextToken,
		},
	}

	var resp wechatSendMsgResp
	w.mu.Lock()
	ctx := w.ctx
	w.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := w.doRequest(ctx, http.MethodPost, "/ilink/bot/sendmessage", req, &resp); err != nil {
		return fmt.Errorf("wechat: send: %w", err)
	}
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

		w.mu.Lock()
		syncBuf := w.syncBuf
		w.mu.Unlock()

		req := wechatGetUpdatesReq{
			BaseInfo:      wechatBaseInfo{ChannelVersion: "1.0.0"},
			GetUpdatesBuf: syncBuf,
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

		// Handle session expired error — need to re-login.
		if resp.Errcode == -14 {
			log.Println("[wechat] session expired (errcode -14), starting re-login...")
			w.mu.Lock()
			w.relogging = true
			w.syncBuf = ""
			w.botToken = ""
			// Keep botID intact so in-flight Send() calls don't use an empty ToUserID.
			w.mu.Unlock()
			w.saveCredentials()

			if err := w.qrLogin(ctx); err != nil {
				log.Printf("[wechat] re-login failed: %v, retrying in %v", err, backoff)
				// relogging stays true; Send() will keep rejecting.
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			w.mu.Lock()
			w.relogging = false
			w.mu.Unlock()
			log.Println("[wechat] re-login successful, resuming poll")
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
			w.mu.Lock()
			w.syncBuf = resp.GetUpdatesBuf
			w.mu.Unlock()
			w.saveCredentials()
		}

		for _, msg := range resp.Messages {
			// Only process user messages (message_type=1) that are finished (message_state=0 or 2).
			if msg.MsgType.String() != "1" {
				continue
			}
			if msg.MsgState.String() != "0" && msg.MsgState.String() != "2" {
				continue
			}

			// Infer bot ID from the first message received.
			if msg.ToUserID != "" {
				w.mu.Lock()
				if w.botID == "" {
					w.botID = msg.ToUserID
				}
				w.mu.Unlock()
			}

			text := extractText(msg.Items)
			attachments := extractAttachments(msg.Items)
			if text == "" && len(attachments) == 0 {
				continue
			}

			// Read handler inside the loop to avoid losing messages if
			// OnMessage is called between poll start and message processing.
			w.mu.Lock()
			handler := w.handler
			w.mu.Unlock()
			if handler == nil {
				continue
			}

			accountID := msg.ToUserID
			if accountID == "" {
				w.mu.Lock()
				accountID = w.botID
				w.mu.Unlock()
			}
			peerID := msg.FromUserID
			handler(InboundMessage{
				ChannelName:  "wechat",
				AccountID:    accountID,
				ChatID:       peerID,
				PeerKind:     "dm",
				PeerID:       peerID,
				MessageID:    fmt.Sprintf("%d", msg.MessageID),
				UserID:       peerID,
				Text:         text,
				ContextToken: msg.ContextToken,
				Attachments:  attachments,
			})
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

// qrLogin performs QR code login to obtain a bot token. Auto-retries on expiry.
func (w *WeChatChannel) qrLogin(ctx context.Context) error {
	log.Println("[wechat] no bot token, starting QR code login...")

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	const maxRetries = 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		var qrResp wechatQRCodeResp
		if err := w.doRequest(ctx, http.MethodGet, "/ilink/bot/get_bot_qrcode?bot_type=3", nil, &qrResp); err != nil {
			return fmt.Errorf("get qr code: %w", err)
		}

		log.Printf("[wechat] scan QR code to login: %s", qrResp.ImageContent)

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
				time.Sleep(2 * time.Second)
				continue
			}

			switch statusResp.Status {
			case "confirmed":
				w.mu.Lock()
				w.botToken = statusResp.BotToken
				w.botID = statusResp.ILinkBotID
				if statusResp.BaseURL != "" {
					w.baseURL = statusResp.BaseURL
				}
				w.mu.Unlock()
				log.Printf("[wechat] QR login successful, bot ID: %s", statusResp.ILinkBotID)
				w.saveCredentials()
				return nil
			case "expired":
				log.Println("[wechat] QR code expired, generating a new one")
				expired = true
			case "scanned":
				log.Println("[wechat] QR code scanned, waiting for confirmation")
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
	w.mu.Lock()
	token := w.botToken
	w.mu.Unlock()
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
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
	w.mu.Lock()
	creds := wechatCredentials{
		BotToken: w.botToken,
		BotID:    w.botID,
		BaseURL:  w.baseURL,
		SyncBuf:  w.syncBuf,
	}
	w.mu.Unlock()
	data, _ := json.Marshal(creds)
	path := filepath.Join(w.credsDir, "wechat_credentials.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		log.Printf("[wechat] failed to save credentials: %v", err)
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

func extractAttachments(items []wechatMsgItem) []Attachment {
	var attachments []Attachment
	for _, item := range items {
		if item.Image == nil || strings.TrimSpace(item.Image.URL) == "" {
			continue
		}
		url := strings.TrimSpace(item.Image.URL)
		filename := attachmentFilenameFromURL(url)
		attachments = append(attachments, Attachment{
			Type:     "image",
			Filename: filename,
			MimeType: mimeTypeForFilename(filename),
			URL:      url,
		})
	}
	return attachments
}

func normalizeWeChatOutbound(text string, attachments []Attachment) (string, []string, string) {
	cleanedText, markdownImageURLs := extractMarkdownImageURLs(text)

	imageURLs := make([]string, 0, len(markdownImageURLs)+len(attachments))
	seen := make(map[string]struct{}, len(markdownImageURLs)+len(attachments))
	addImageURL := func(raw string) {
		url := strings.TrimSpace(raw)
		if url == "" {
			return
		}
		if _, ok := seen[url]; ok {
			return
		}
		seen[url] = struct{}{}
		imageURLs = append(imageURLs, url)
	}
	for _, url := range markdownImageURLs {
		addImageURL(url)
	}

	var unsupported []string
	for _, attachment := range attachments {
		switch {
		case strings.EqualFold(attachment.Type, "image") && strings.TrimSpace(attachment.URL) != "":
			addImageURL(attachment.URL)
		case strings.TrimSpace(attachment.URL) != "":
			unsupported = append(unsupported, describeUnsupportedAttachment(attachment))
		case len(attachment.Data) > 0 || strings.TrimSpace(attachment.Path) != "":
			unsupported = append(unsupported, describeUnsupportedAttachment(attachment))
		default:
			unsupported = append(unsupported, describeUnsupportedAttachment(attachment))
		}
	}

	fallbackText := ""
	if len(unsupported) > 0 {
		fallbackText = "Attachments not sent: " + strings.Join(unsupported, ", ")
	}

	return strings.TrimSpace(cleanedText), imageURLs, fallbackText
}

func extractMarkdownImageURLs(text string) (string, []string) {
	if strings.TrimSpace(text) == "" {
		return "", nil
	}
	matches := wechatMarkdownImageRe.FindAllStringSubmatch(text, -1)
	if len(matches) == 0 {
		return text, nil
	}

	imageURLs := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			imageURLs = append(imageURLs, match[1])
		}
	}

	cleaned := wechatMarkdownImageRe.ReplaceAllString(text, "")
	cleaned = strings.ReplaceAll(cleaned, "\n\n\n", "\n\n")
	cleaned = strings.ReplaceAll(cleaned, "  ", " ")
	cleaned = strings.TrimSpace(cleaned)
	return cleaned, imageURLs
}

func describeUnsupportedAttachment(attachment Attachment) string {
	if name := strings.TrimSpace(attachment.Filename); name != "" {
		return filepath.Base(name)
	}
	if path := strings.TrimSpace(attachment.Path); path != "" {
		return filepath.Base(path)
	}
	if url := strings.TrimSpace(attachment.URL); url != "" {
		return filepath.Base(url)
	}
	if attachment.Type != "" {
		return attachment.Type
	}
	return "attachment"
}

func attachmentFilenameFromURL(url string) string {
	base := filepath.Base(strings.SplitN(url, "?", 2)[0])
	base = strings.TrimSpace(base)
	if base == "" || base == "." || base == "/" {
		return "wechat-image.png"
	}
	return base
}

func mimeTypeForFilename(name string) string {
	if value := mime.TypeByExtension(strings.ToLower(filepath.Ext(name))); value != "" {
		return value
	}
	return "image/png"
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
	Message  wechatSendMsg  `json:"msg"`
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
