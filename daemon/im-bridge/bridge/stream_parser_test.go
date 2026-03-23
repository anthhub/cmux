package bridge

import (
	"testing"
)

func TestParseClaudeStream_Init(t *testing.T) {
	line := []byte(`{"type":"system","subtype":"init","session_id":"sess-123"}`)
	events, err := ParseClaudeStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventInit {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventInit)
	}
	if ev.Provider != "claude" {
		t.Errorf("Provider = %q, want %q", ev.Provider, "claude")
	}
	if ev.Meta["session_id"] != "sess-123" {
		t.Errorf("session_id = %v, want %q", ev.Meta["session_id"], "sess-123")
	}
}

func TestParseClaudeStream_TextDelta(t *testing.T) {
	line := []byte(`{"type":"stream_event","event":{"delta":{"type":"text_delta","text":"hello "}}}`)
	events, err := ParseClaudeStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventText {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventText)
	}
	if !ev.Delta {
		t.Error("Delta = false, want true")
	}
	if ev.Content != "hello " {
		t.Errorf("Content = %q, want %q", ev.Content, "hello ")
	}
}

func TestParseClaudeStream_ThinkingDelta(t *testing.T) {
	line := []byte(`{"type":"stream_event","event":{"delta":{"type":"thinking_delta","thinking":"let me think"}}}`)
	events, err := ParseClaudeStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventThinking {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventThinking)
	}
	if ev.Content != "let me think" {
		t.Errorf("Content = %q, want %q", ev.Content, "let me think")
	}
}

func TestParseClaudeStream_ToolUse(t *testing.T) {
	line := []byte(`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{"path":"/tmp/test.go"}}]}}`)
	events, err := ParseClaudeStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventToolUse {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventToolUse)
	}
	if ev.Provider != "claude" {
		t.Errorf("Provider = %q, want %q", ev.Provider, "claude")
	}
	// Content should contain the tool name
	if ev.Content == "" {
		t.Error("Content is empty, expected tool description")
	}
}

func TestParseClaudeStream_ToolResult(t *testing.T) {
	line := []byte(`{"type":"user","message":{"content":[{"type":"tool_result","content":"file contents here"}]}}`)
	events, err := ParseClaudeStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventToolResult {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventToolResult)
	}
	if ev.Content != "file contents here" {
		t.Errorf("Content = %q, want %q", ev.Content, "file contents here")
	}
}

func TestParseClaudeStream_ControlRequest(t *testing.T) {
	line := []byte(`{"type":"control_request","message":"Please approve this action"}`)
	events, err := ParseClaudeStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventControlRequest {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventControlRequest)
	}
	if ev.Content != "Please approve this action" {
		t.Errorf("Content = %q, want %q", ev.Content, "Please approve this action")
	}
}

func TestParseClaudeStream_Result(t *testing.T) {
	line := []byte(`{"type":"result","result":"Task completed","usage":{"input_tokens":100,"output_tokens":50}}`)
	events, err := ParseClaudeStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventResult {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventResult)
	}
	if ev.Content != "Task completed" {
		t.Errorf("Content = %q, want %q", ev.Content, "Task completed")
	}
	if ev.Meta == nil {
		t.Fatal("Meta is nil")
	}
}

func TestParseClaudeStream_Error(t *testing.T) {
	line := []byte(`{"type":"error","message":"rate limited"}`)
	events, err := ParseClaudeStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventError {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventError)
	}
	if ev.Content != "rate limited" {
		t.Errorf("Content = %q, want %q", ev.Content, "rate limited")
	}
}

func TestParseClaudeStream_MalformedJSON(t *testing.T) {
	line := []byte(`{not valid json`)
	_, err := ParseClaudeStream(line)
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestParseCodexStream_ThreadStarted(t *testing.T) {
	line := []byte(`{"type":"thread.started","thread_id":"th-abc"}`)
	events, err := ParseCodexStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventInit {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventInit)
	}
	if ev.Provider != "codex" {
		t.Errorf("Provider = %q, want %q", ev.Provider, "codex")
	}
	if ev.Meta["thread_id"] != "th-abc" {
		t.Errorf("thread_id = %v, want %q", ev.Meta["thread_id"], "th-abc")
	}
}

func TestParseCodexStream_AgentMessage(t *testing.T) {
	line := []byte(`{"type":"item.agent_message","item":{"type":"agent_message","text":"I will help you"}}`)
	events, err := ParseCodexStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventText {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventText)
	}
	if ev.Content != "I will help you" {
		t.Errorf("Content = %q, want %q", ev.Content, "I will help you")
	}
}

func TestParseCodexStream_CommandExecution(t *testing.T) {
	line := []byte(`{"type":"item.completed","item":{"type":"command_execution","command":"ls -la"}}`)
	events, err := ParseCodexStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventToolUse {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventToolUse)
	}
	if ev.Content != "ls -la" {
		t.Errorf("Content = %q, want %q", ev.Content, "ls -la")
	}
}

func TestParseCodexStream_TurnCompleted(t *testing.T) {
	line := []byte(`{"type":"turn.completed","status":"done"}`)
	events, err := ParseCodexStream(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != StreamEventResult {
		t.Errorf("Type = %q, want %q", ev.Type, StreamEventResult)
	}
	if ev.Provider != "codex" {
		t.Errorf("Provider = %q, want %q", ev.Provider, "codex")
	}
}
