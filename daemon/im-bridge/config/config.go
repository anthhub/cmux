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
	Discord  DiscordConfig  `yaml:"discord"`
	WeChat   WeChatConfig   `yaml:"wechat"`
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

// DiscordConfig configures the Discord bot.
type DiscordConfig struct {
	Enabled  bool   `yaml:"enabled"`
	BotToken string `yaml:"bot_token"`
}

// WeChatConfig configures the WeChat bot.
type WeChatConfig struct {
	Enabled        bool   `yaml:"enabled"`
	BotToken       string `yaml:"bot_token"`       // iLink bot token（从 QR 登录获取）
	CredentialsDir string `yaml:"credentials_dir"` // 凭证保存目录，默认 ~/.weclaw/accounts
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
	if value := os.Getenv("SLACK_BOT_TOKEN"); value != "" {
		cfg.Slack.BotToken = value
		cfg.Slack.Enabled = true
	}
	if value := os.Getenv("SLACK_APP_TOKEN"); value != "" {
		cfg.Slack.AppToken = value
	}
	if value := os.Getenv("FEISHU_APP_ID"); value != "" {
		cfg.Feishu.AppID = value
		cfg.Feishu.Enabled = true
	}
	if value := os.Getenv("FEISHU_APP_SECRET"); value != "" {
		cfg.Feishu.AppSecret = value
	}
	if value := os.Getenv("DISCORD_BOT_TOKEN"); value != "" {
		cfg.Discord.BotToken = value
		cfg.Discord.Enabled = true
	}
	if token := os.Getenv("WECHAT_BOT_TOKEN"); token != "" {
		cfg.WeChat.BotToken = token
		cfg.WeChat.Enabled = true
	}

	return &cfg, nil
}

// EnabledChannels returns the names of all channels that are enabled in the config.
func (c *Config) EnabledChannels() []string {
	var names []string
	if c.Telegram.Enabled {
		names = append(names, "telegram")
	}
	if c.Slack.Enabled {
		names = append(names, "slack")
	}
	if c.Feishu.Enabled {
		names = append(names, "feishu")
	}
	if c.Discord.Enabled {
		names = append(names, "discord")
	}
	if c.WeChat.Enabled {
		names = append(names, "wechat")
	}
	return names
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
