package channels

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestNewWeChatChannel_UsesDefaultCredentialsDirAndLoadsSavedState(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	credsDir := filepath.Join(homeDir, ".weclaw", "accounts")
	if err := os.MkdirAll(credsDir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	credsPath := filepath.Join(credsDir, "wechat_credentials.json")
	data := []byte(`{"bot_token":"saved-token","bot_id":"saved-bot","base_url":"https://saved.example","sync_buf":"saved-buf"}`)
	if err := os.WriteFile(credsPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	channel, err := NewWeChatChannel("explicit-token", "")
	if err != nil {
		t.Fatalf("NewWeChatChannel failed: %v", err)
	}

	if channel.credsDir != credsDir {
		t.Fatalf("credsDir = %q, want %q", channel.credsDir, credsDir)
	}
	if channel.botToken != "explicit-token" {
		t.Fatalf("botToken = %q, want %q", channel.botToken, "explicit-token")
	}
	if channel.botID != "saved-bot" {
		t.Fatalf("botID = %q, want %q", channel.botID, "saved-bot")
	}
	if channel.baseURL != "https://saved.example" {
		t.Fatalf("baseURL = %q, want %q", channel.baseURL, "https://saved.example")
	}
	if channel.syncBuf != "saved-buf" {
		t.Fatalf("syncBuf = %q, want %q", channel.syncBuf, "saved-buf")
	}
}

func TestWeChatChannel_SaveCredentialsPersistsSyncBuf(t *testing.T) {
	channel, err := NewWeChatChannel("", t.TempDir())
	if err != nil {
		t.Fatalf("NewWeChatChannel failed: %v", err)
	}
	channel.botToken = "bot-token"
	channel.botID = "bot-id"
	channel.baseURL = "https://example.com"
	channel.syncBuf = "sync-buf-123"

	channel.saveCredentials()

	credsPath := filepath.Join(channel.credsDir, "wechat_credentials.json")
	raw, err := os.ReadFile(credsPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var creds map[string]string
	if err := json.Unmarshal(raw, &creds); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if creds["sync_buf"] != "sync-buf-123" {
		t.Fatalf("sync_buf = %q, want %q", creds["sync_buf"], "sync-buf-123")
	}
}

func TestWeChatChannel_SendIncludesContextToken(t *testing.T) {
	var gotReq wechatSendMsgReq
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ilink/bot/sendmessage" {
			t.Fatalf("path = %q, want /ilink/bot/sendmessage", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wechatSendMsgResp{Ret: 0})
	}))
	defer server.Close()

	channel, err := NewWeChatChannel("bot-token", t.TempDir())
	if err != nil {
		t.Fatalf("NewWeChatChannel failed: %v", err)
	}
	channel.baseURL = server.URL
	channel.botID = "bot-id"

	err = channel.Send("user-1", OutboundMessage{
		Text:         "hello from test",
		Format:       "text",
		ContextToken: "ctx-123",
	})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if gotReq.Message.ContextToken != "ctx-123" {
		t.Fatalf("ContextToken = %q, want %q", gotReq.Message.ContextToken, "ctx-123")
	}
}
