package bridge

import (
	"fmt"
	"strings"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

const (
	AgentTypeClaude = "claude"
	AgentTypeCodex  = "codex"
	AgentTypeShell  = "shell"
)

// BuildAgentCommand constructs the CLI command string for launching the given session's agent.
// Returns an empty string for shell sessions (no agent to launch).
func BuildAgentCommand(session *Session, aiCfg config.AIConfig) string {
	agentType := session.agentType()
	switch agentType {
	case AgentTypeClaude:
		parts := []string{"claude", "-p", "--output-format", "stream-json"}
		model := coalesce(session.model(), aiCfg.ClaudeModel)
		if model != "" {
			parts = append(parts, "--model", model)
		}
		if effort := session.effort(); effort != "" {
			parts = append(parts, "--effort", effort)
		}
		perm := coalesce(session.permissionMode(), aiCfg.ClaudePermissionMode)
		if perm != "" {
			parts = append(parts, "--permission-mode", perm)
		}
		if id := session.providerSessionID(); id != "" {
			parts = append(parts, "--resume", id)
		}
		return strings.Join(parts, " ")
	case AgentTypeCodex:
		parts := []string{"codex"}
		model := coalesce(session.model(), aiCfg.CodexModel)
		if model != "" {
			parts = append(parts, "--model", model)
		}
		return strings.Join(parts, " ")
	default:
		return ""
	}
}

// coalesce returns the first non-empty string.
func coalesce(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// LaunchAgentInTerminal sends the agent launch command into a cmux terminal surface.
func LaunchAgentInTerminal(cmux *CmuxClient, wsID, surfaceID string, session *Session, aiCfg config.AIConfig) error {
	cmd := BuildAgentCommand(session, aiCfg)
	if cmd == "" {
		return fmt.Errorf("unsupported agent type for terminal launch: %q", session.agentType())
	}
	if aiCfg.Workdir != "" {
		cmd = "cd " + aiCfg.Workdir + " && " + cmd
	}
	return cmux.SendText(wsID, surfaceID, cmd+"\n")
}

// StopAgent sends Ctrl+C to the terminal surface.
func StopAgent(cmux *CmuxClient, workspaceID, surfaceID string) error {
	return cmux.SendText(workspaceID, surfaceID, "\x03")
}
