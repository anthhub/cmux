package bridge

import (
	"fmt"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

const (
	AgentTypeClaude = "claude"
	AgentTypeCodex  = "codex"
	AgentTypeShell  = "shell"
)

// AgentRunner provides agent lifecycle operations for terminal passthrough mode.
type AgentRunner struct {
	cfg config.AIConfig
}

// NewAgentRunner creates a new runner.
func NewAgentRunner(cfg config.AIConfig) *AgentRunner {
	return &AgentRunner{cfg: cfg}
}

// LaunchAgentInTerminal sends the agent launch command into a cmux terminal surface.
func LaunchAgentInTerminal(cmux *CmuxClient, workspaceID, surfaceID, agentType string) error {
	var cmd string
	switch agentType {
	case AgentTypeClaude:
		cmd = "claude"
	case AgentTypeCodex:
		cmd = "codex"
	default:
		return fmt.Errorf("unsupported agent type for terminal launch: %q", agentType)
	}
	return cmux.SendText(workspaceID, surfaceID, cmd+"\n")
}

// StopAgent sends Ctrl+C to the terminal surface.
func StopAgent(cmux *CmuxClient, workspaceID, surfaceID string) error {
	return cmux.SendText(workspaceID, surfaceID, "\x03")
}
