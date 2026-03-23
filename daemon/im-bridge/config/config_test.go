package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_ValidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `
cmux:
  socket_path: /tmp/test.sock
ai:
  default_agent: claude
  workdir: /tmp/work
telegram:
  enabled: true
  bot_token: "test-token-123"
security:
  allowed_users:
    - user1
    - user2
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Cmux.SocketPath != "/tmp/test.sock" {
		t.Errorf("SocketPath = %q, want %q", cfg.Cmux.SocketPath, "/tmp/test.sock")
	}
	if cfg.AI.DefaultAgent != "claude" {
		t.Errorf("DefaultAgent = %q, want %q", cfg.AI.DefaultAgent, "claude")
	}
	if cfg.Telegram.BotToken != "test-token-123" {
		t.Errorf("BotToken = %q, want %q", cfg.Telegram.BotToken, "test-token-123")
	}
	if !cfg.Telegram.Enabled {
		t.Error("Telegram.Enabled = false, want true")
	}
	if len(cfg.Security.AllowedUsers) != 2 {
		t.Errorf("AllowedUsers len = %d, want 2", len(cfg.Security.AllowedUsers))
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	content := `
telegram:
  enabled: false
  bot_token: "original-token"
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Setenv("TELEGRAM_BOT_TOKEN", "env-override-token")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Telegram.BotToken != "env-override-token" {
		t.Errorf("BotToken = %q, want %q", cfg.Telegram.BotToken, "env-override-token")
	}
	if !cfg.Telegram.Enabled {
		t.Error("Telegram.Enabled = false, want true (env override should enable)")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

func TestIsUserAllowed_EmptyList(t *testing.T) {
	cfg := &Config{}
	if !cfg.IsUserAllowed("anyone") {
		t.Error("empty list should allow all users")
	}
}

func TestIsUserAllowed_InList(t *testing.T) {
	cfg := &Config{
		Security: SecurityConfig{AllowedUsers: []string{"user1", "user2"}},
	}
	if !cfg.IsUserAllowed("user1") {
		t.Error("user1 should be allowed")
	}
}

func TestIsUserAllowed_NotInList(t *testing.T) {
	cfg := &Config{
		Security: SecurityConfig{AllowedUsers: []string{"user1", "user2"}},
	}
	if cfg.IsUserAllowed("user3") {
		t.Error("user3 should not be allowed")
	}
}
