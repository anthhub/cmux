package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for the IM bridge.
type Config struct {
	Cmux     CmuxConfig     `yaml:"cmux"`
	AI       AIConfig       `yaml:"ai"`
	Telegram TelegramConfig `yaml:"telegram"`
	Slack    SlackConfig    `yaml:"slack"`
	Feishu   FeishuConfig   `yaml:"feishu"`
	Security SecurityConfig `yaml:"security"`
}

// CmuxConfig configures the cmux socket connection.
type CmuxConfig struct {
	SocketPath string `yaml:"socket_path"` // empty = auto-discover
}

// AIConfig configures subprocess-backed AI agents.
type AIConfig struct {
	DefaultAgent         string `yaml:"default_agent"`
	Workdir              string `yaml:"workdir"`
	ClaudePath           string `yaml:"claude_path"`
	ClaudeModel          string `yaml:"claude_model"`
	ClaudePermissionMode string `yaml:"claude_permission_mode"`
	CodexPath            string `yaml:"codex_path"`
	CodexModel           string `yaml:"codex_model"`
	CodexSandbox         string `yaml:"codex_sandbox"`
}

// TelegramConfig configures the Telegram bot.
type TelegramConfig struct {
	Enabled  bool   `yaml:"enabled"`
	BotToken string `yaml:"bot_token"`
}

// SlackConfig configures the Slack bot.
type SlackConfig struct {
	Enabled  bool   `yaml:"enabled"`
	BotToken string `yaml:"bot_token"`
	AppToken string `yaml:"app_token"` // for Socket Mode
}

// FeishuConfig configures the Feishu/Lark bot.
type FeishuConfig struct {
	Enabled   bool   `yaml:"enabled"`
	AppID     string `yaml:"app_id"`
	AppSecret string `yaml:"app_secret"`
}

// SecurityConfig configures access control.
type SecurityConfig struct {
	AllowedUsers []string `yaml:"allowed_users"` // whitelist of IM user IDs
}

// Load reads config from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Override with environment variables
	if token := os.Getenv("TELEGRAM_BOT_TOKEN"); token != "" {
		cfg.Telegram.BotToken = token
		cfg.Telegram.Enabled = true
	}
	if value := os.Getenv("CMUX_IM_BRIDGE_DEFAULT_AGENT"); value != "" {
		cfg.AI.DefaultAgent = value
	}
	if value := os.Getenv("CMUX_IM_BRIDGE_WORKDIR"); value != "" {
		cfg.AI.Workdir = value
	}
	if value := os.Getenv("CMUX_IM_BRIDGE_CLAUDE_PATH"); value != "" {
		cfg.AI.ClaudePath = value
	}
	if value := os.Getenv("CMUX_IM_BRIDGE_CODEX_PATH"); value != "" {
		cfg.AI.CodexPath = value
	}

	return &cfg, nil
}

// IsUserAllowed checks if a user ID is in the whitelist.
// Empty whitelist = allow all.
func (c *Config) IsUserAllowed(userID string) bool {
	if len(c.Security.AllowedUsers) == 0 {
		return true
	}
	for _, id := range c.Security.AllowedUsers {
		if id == userID {
			return true
		}
	}
	return false
}
