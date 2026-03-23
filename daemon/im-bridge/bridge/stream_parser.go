package bridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	StreamEventInit           = "init"
	StreamEventText           = "text"
	StreamEventThinking       = "thinking"
	StreamEventToolUse        = "tool_use"
	StreamEventToolResult     = "tool_result"
	StreamEventControlRequest = "control_request"
	StreamEventResult         = "result"
	StreamEventError          = "error"
)

// StreamEvent is a normalized event from Claude Code or Codex JSON streams.
type StreamEvent struct {
	Type     string
	Content  string
	Provider string
	Delta    bool
	Meta     map[string]interface{}
}

// ParseClaudeStream normalizes one Claude stream-json line.
func ParseClaudeStream(line []byte) ([]StreamEvent, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(line), &payload); err != nil {
		return nil, fmt.Errorf("parse claude stream: %w", err)
	}

	var events []StreamEvent
	switch payload["type"] {
	case "system":
		if payload["subtype"] == "init" {
			events = append(events, StreamEvent{
				Type:     StreamEventInit,
				Provider: "claude",
				Meta: map[string]interface{}{
					"session_id": payload["session_id"],
				},
			})
		}
	case "stream_event":
		event, _ := payload["event"].(map[string]interface{})
		delta, _ := event["delta"].(map[string]interface{})
		deltaType, _ := delta["type"].(string)
		switch deltaType {
		case "text_delta":
			events = append(events, StreamEvent{
				Type:     StreamEventText,
				Provider: "claude",
				Content:  stringValue(delta["text"]),
				Delta:    true,
			})
		case "thinking_delta":
			events = append(events, StreamEvent{
				Type:     StreamEventThinking,
				Provider: "claude",
				Content:  stringValue(delta["thinking"]),
				Delta:    true,
			})
		}
	case "assistant":
		message, _ := payload["message"].(map[string]interface{})
		contentItems, _ := message["content"].([]interface{})
		for _, item := range contentItems {
			block, _ := item.(map[string]interface{})
			blockType, _ := block["type"].(string)
			switch blockType {
			case "text":
				events = append(events, StreamEvent{
					Type:     StreamEventText,
					Provider: "claude",
					Content:  stringValue(block["text"]),
				})
			case "thinking":
				events = append(events, StreamEvent{
					Type:     StreamEventThinking,
					Provider: "claude",
					Content:  stringValue(block["thinking"]),
				})
			case "tool_use":
				events = append(events, StreamEvent{
					Type:     StreamEventToolUse,
					Provider: "claude",
					Content:  describeClaudeToolUse(block),
					Meta:     block,
				})
			case "tool_result":
				events = append(events, StreamEvent{
					Type:     StreamEventToolResult,
					Provider: "claude",
					Content:  stringValue(block["content"]),
					Meta:     block,
				})
			}
		}
	case "user":
		message, _ := payload["message"].(map[string]interface{})
		contentItems, _ := message["content"].([]interface{})
		for _, item := range contentItems {
			block, _ := item.(map[string]interface{})
			if stringValue(block["type"]) == "tool_result" {
				events = append(events, StreamEvent{
					Type:     StreamEventToolResult,
					Provider: "claude",
					Content:  stringValue(block["content"]),
					Meta:     block,
				})
			}
		}
	case "control_request":
		events = append(events, StreamEvent{
			Type:     StreamEventControlRequest,
			Provider: "claude",
			Content:  stringValue(payload["message"]),
			Meta:     payload,
		})
	case "result":
		events = append(events, StreamEvent{
			Type:     StreamEventResult,
			Provider: "claude",
			Content:  stringValue(payload["result"]),
			Meta:     payload,
		})
	}

	if payload["type"] == "error" {
		events = append(events, StreamEvent{
			Type:     StreamEventError,
			Provider: "claude",
			Content:  stringValue(payload["message"]),
			Meta:     payload,
		})
	}
	if assistantError := stringValue(payload["error"]); assistantError != "" && len(events) == 0 {
		events = append(events, StreamEvent{
			Type:     StreamEventError,
			Provider: "claude",
			Content:  assistantError,
			Meta:     payload,
		})
	}
	return events, nil
}

// ParseCodexStream normalizes one Codex exec --json line.
func ParseCodexStream(line []byte) ([]StreamEvent, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(line), &payload); err != nil {
		return nil, fmt.Errorf("parse codex stream: %w", err)
	}

	eventType := stringValue(payload["type"])
	var events []StreamEvent
	switch eventType {
	case "thread.started":
		events = append(events, StreamEvent{
			Type:     StreamEventInit,
			Provider: "codex",
			Meta: map[string]interface{}{
				"thread_id": payload["thread_id"],
			},
		})
	case "item.completed", "item.started":
		item, _ := payload["item"].(map[string]interface{})
		itemType := stringValue(item["type"])
		events = append(events, codexItemToEvents(itemType, item)...)
	case "item.agent_message":
		item, _ := payload["item"].(map[string]interface{})
		events = append(events, StreamEvent{
			Type:     StreamEventText,
			Provider: "codex",
			Content:  codexText(item),
		})
	case "turn.completed":
		events = append(events, StreamEvent{
			Type:     StreamEventResult,
			Provider: "codex",
			Meta:     payload,
		})
	}

	return events, nil
}

func codexItemToEvents(itemType string, item map[string]interface{}) []StreamEvent {
	switch itemType {
	case "agent_message":
		return []StreamEvent{{
			Type:     StreamEventText,
			Provider: "codex",
			Content:  codexText(item),
		}}
	case "reasoning":
		return []StreamEvent{{
			Type:     StreamEventThinking,
			Provider: "codex",
			Content:  codexText(item),
			Meta:     item,
		}}
	case "command_execution":
		return []StreamEvent{{
			Type:     StreamEventToolUse,
			Provider: "codex",
			Content:  codexCommand(item),
			Meta:     item,
		}}
	case "command_result", "file_change":
		return []StreamEvent{{
			Type:     StreamEventToolResult,
			Provider: "codex",
			Content:  codexText(item),
			Meta:     item,
		}}
	default:
		return nil
	}
}

func describeClaudeToolUse(block map[string]interface{}) string {
	name := stringValue(block["name"])
	if name == "" {
		name = "tool"
	}
	var parts []string
	parts = append(parts, name)
	if input := block["input"]; input != nil {
		if encoded, err := json.Marshal(input); err == nil && string(encoded) != "{}" && string(encoded) != "null" {
			parts = append(parts, string(encoded))
		}
	}
	return strings.Join(parts, " ")
}

func codexText(item map[string]interface{}) string {
	if text := stringValue(item["text"]); text != "" {
		return text
	}
	if content := stringValue(item["content"]); content != "" {
		return content
	}
	if parts, ok := item["content"].([]interface{}); ok {
		var chunks []string
		for _, raw := range parts {
			if block, ok := raw.(map[string]interface{}); ok {
				if text := stringValue(block["text"]); text != "" {
					chunks = append(chunks, text)
				}
			}
		}
		return strings.Join(chunks, "")
	}
	return ""
}

func codexCommand(item map[string]interface{}) string {
	if command := stringValue(item["command"]); command != "" {
		return command
	}
	if args, ok := item["args"].([]interface{}); ok {
		parts := make([]string, 0, len(args))
		for _, arg := range args {
			if text := stringValue(arg); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, " ")
	}
	return codexText(item)
}

func stringValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return ""
	}
}
