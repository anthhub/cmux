package bridge

import (
	"strings"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

// AgentDefinition describes one named IM-facing agent.
type AgentDefinition struct {
	ID             string
	Provider       string
	Model          string
	Workdir        string
	Args           []string
	PermissionMode string
}

func builtinAgentDefinitions(ai config.AIConfig) map[string]AgentDefinition {
	defs := map[string]AgentDefinition{
		AgentTypeClaude: {
			ID:             AgentTypeClaude,
			Provider:       AgentTypeClaude,
			Model:          ai.ClaudeModel,
			Workdir:        ai.Workdir,
			PermissionMode: ai.ClaudePermissionMode,
		},
		AgentTypeCodex: {
			ID:       AgentTypeCodex,
			Provider: AgentTypeCodex,
			Model:    ai.CodexModel,
			Workdir:  ai.Workdir,
		},
		AgentTypeShell: {
			ID:       AgentTypeShell,
			Provider: AgentTypeShell,
			Workdir:  ai.Workdir,
		},
	}
	return defs
}

func resolveAgentDefinitions(cfg *config.Config) map[string]AgentDefinition {
	defs := builtinAgentDefinitions(cfg.AI)
	for _, entry := range cfg.Agents.List {
		id := normalizeKey(entry.ID)
		if id == "" {
			continue
		}
		provider := effectiveAgentType(entry.Provider, defs[id].Provider)
		if provider == "" {
			provider = AgentTypeClaude
		}
		workdir := strings.TrimSpace(entry.CWD)
		if workdir == "" {
			workdir = cfg.AI.Workdir
		}
		permissionMode := strings.TrimSpace(entry.PermissionMode)
		if permissionMode == "" && provider == AgentTypeClaude {
			permissionMode = cfg.AI.ClaudePermissionMode
		}

		defs[id] = AgentDefinition{
			ID:             id,
			Provider:       provider,
			Model:          strings.TrimSpace(entry.Model),
			Workdir:        workdir,
			Args:           append([]string(nil), entry.Args...),
			PermissionMode: permissionMode,
		}
	}
	return defs
}
