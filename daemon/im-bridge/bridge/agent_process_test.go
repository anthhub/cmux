package bridge

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

func makeTestSession(agentType, model, effort, perm, providerSessionID string) *Session {
	return &Session{
		AgentType:         agentType,
		Model:             model,
		Effort:            effort,
		PermissionMode:    perm,
		ProviderSessionID: providerSessionID,
	}
}

func TestBuildAgentCommand_ClaudeMinimal(t *testing.T) {
	session := makeTestSession(AgentTypeClaude, "", "", "", "")
	cmd := BuildAgentCommand(session, config.AIConfig{})
	if cmd != "claude -p --output-format stream-json" {
		t.Fatalf("got %q", cmd)
	}
}

func TestBuildAgentCommand_ClaudeWithAllFlags(t *testing.T) {
	session := makeTestSession(AgentTypeClaude, "opus", "high", "plan", "sess-123")
	cmd := BuildAgentCommand(session, config.AIConfig{ClaudePermissionMode: "default"})
	// session values take precedence over aiCfg defaults
	if !strings.Contains(cmd, "--model opus") {
		t.Errorf("missing --model opus in %q", cmd)
	}
	if !strings.Contains(cmd, "--effort high") {
		t.Errorf("missing --effort high in %q", cmd)
	}
	if !strings.Contains(cmd, "--permission-mode plan") {
		t.Errorf("missing --permission-mode plan in %q", cmd)
	}
	if !strings.Contains(cmd, "--resume sess-123") {
		t.Errorf("missing --resume sess-123 in %q", cmd)
	}
}

func TestBuildAgentCommand_ClaudeModelFallback(t *testing.T) {
	session := makeTestSession(AgentTypeClaude, "", "", "", "")
	cmd := BuildAgentCommand(session, config.AIConfig{ClaudeModel: "sonnet"})
	if !strings.Contains(cmd, "--model sonnet") {
		t.Errorf("missing --model sonnet in %q", cmd)
	}
}

func TestBuildAgentCommand_CodexMinimal(t *testing.T) {
	session := makeTestSession(AgentTypeCodex, "", "", "", "")
	cmd := BuildAgentCommand(session, config.AIConfig{})
	if cmd != "codex" {
		t.Fatalf("got %q", cmd)
	}
}

func TestBuildAgentCommand_CodexWithModel(t *testing.T) {
	session := makeTestSession(AgentTypeCodex, "gpt-4o", "", "", "")
	cmd := BuildAgentCommand(session, config.AIConfig{})
	if !strings.Contains(cmd, "--model gpt-4o") {
		t.Errorf("missing --model gpt-4o in %q", cmd)
	}
}

func TestBuildAgentCommand_Shell(t *testing.T) {
	session := makeTestSession(AgentTypeShell, "", "", "", "")
	cmd := BuildAgentCommand(session, config.AIConfig{})
	if cmd != "" {
		t.Fatalf("shell should return empty, got %q", cmd)
	}
}

func TestCoalesce(t *testing.T) {
	if coalesce("a", "b") != "a" {
		t.Error("coalesce should return first non-empty")
	}
	if coalesce("", "b") != "b" {
		t.Error("coalesce should fall back to second")
	}
	if coalesce("", "") != "" {
		t.Error("coalesce of two empty should return empty")
	}
}

func TestLaunchAgentInTerminal_SendsClaudeCommand(t *testing.T) {
	var sentText string
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method == "surface.send_text" {
			var p map[string]string
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, err
			}
			sentText = p["text"]
			return json.RawMessage(`{}`), nil
		}
		return nil, nil
	})
	cmux := NewCmuxClient(sockPath)
	session := makeTestSession(AgentTypeClaude, "", "", "", "")

	if err := LaunchAgentInTerminal(cmux, "ws-1", "surf-1", session, config.AIConfig{}); err != nil {
		t.Fatalf("LaunchAgentInTerminal failed: %v", err)
	}
	if !strings.HasPrefix(sentText, "claude -p --output-format stream-json") {
		t.Fatalf("sentText = %q, want claude stream-json command", sentText)
	}
}

func TestLaunchAgentInTerminal_SendsCodexCommand(t *testing.T) {
	var sentText string
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method == "surface.send_text" {
			var p map[string]string
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, err
			}
			sentText = p["text"]
			return json.RawMessage(`{}`), nil
		}
		return nil, nil
	})
	cmux := NewCmuxClient(sockPath)
	session := makeTestSession(AgentTypeCodex, "", "", "", "")

	if err := LaunchAgentInTerminal(cmux, "ws-1", "surf-1", session, config.AIConfig{}); err != nil {
		t.Fatalf("LaunchAgentInTerminal failed: %v", err)
	}
	if sentText != "codex\n" {
		t.Fatalf("sentText = %q, want %q", sentText, "codex\n")
	}
}

func TestLaunchAgentInTerminal_RejectsShell(t *testing.T) {
	cmux := NewCmuxClient("/nonexistent")
	session := makeTestSession(AgentTypeShell, "", "", "", "")
	if err := LaunchAgentInTerminal(cmux, "ws-1", "surf-1", session, config.AIConfig{}); err == nil {
		t.Fatal("expected error for shell agent type")
	}
}

func TestLaunchAgentInTerminal_Workdir(t *testing.T) {
	var sentText string
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method == "surface.send_text" {
			var p map[string]string
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, err
			}
			sentText = p["text"]
			return json.RawMessage(`{}`), nil
		}
		return nil, nil
	})
	cmux := NewCmuxClient(sockPath)
	session := makeTestSession(AgentTypeClaude, "", "", "", "")

	if err := LaunchAgentInTerminal(cmux, "ws-1", "surf-1", session, config.AIConfig{Workdir: "/my/project"}); err != nil {
		t.Fatalf("LaunchAgentInTerminal failed: %v", err)
	}
	if !strings.HasPrefix(sentText, "cd /my/project && claude") {
		t.Fatalf("sentText = %q, want cd prefix", sentText)
	}
}

func TestStopAgent_SendsCtrlC(t *testing.T) {
	var sentText string
	sockPath := startMockCmuxSocket(t, func(method string, params json.RawMessage) (json.RawMessage, error) {
		if method == "surface.send_text" {
			var p map[string]string
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, err
			}
			sentText = p["text"]
			return json.RawMessage(`{}`), nil
		}
		return nil, nil
	})
	cmux := NewCmuxClient(sockPath)

	if err := StopAgent(cmux, "ws-1", "surf-1"); err != nil {
		t.Fatalf("StopAgent failed: %v", err)
	}
	if sentText != "\x03" {
		t.Fatalf("sentText = %q, want ctrl-c", sentText)
	}
}
