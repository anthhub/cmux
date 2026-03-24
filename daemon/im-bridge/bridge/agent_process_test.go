package bridge

import (
	"encoding/json"
	"testing"
)

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

	if err := LaunchAgentInTerminal(cmux, "ws-1", "surf-1", AgentTypeClaude); err != nil {
		t.Fatalf("LaunchAgentInTerminal failed: %v", err)
	}
	if sentText != "claude\n" {
		t.Fatalf("sentText = %q, want %q", sentText, "claude\n")
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

	if err := LaunchAgentInTerminal(cmux, "ws-1", "surf-1", AgentTypeCodex); err != nil {
		t.Fatalf("LaunchAgentInTerminal failed: %v", err)
	}
	if sentText != "codex\n" {
		t.Fatalf("sentText = %q, want %q", sentText, "codex\n")
	}
}

func TestLaunchAgentInTerminal_RejectsShell(t *testing.T) {
	cmux := NewCmuxClient("/nonexistent")
	if err := LaunchAgentInTerminal(cmux, "ws-1", "surf-1", AgentTypeShell); err == nil {
		t.Fatal("expected error for shell agent type")
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
