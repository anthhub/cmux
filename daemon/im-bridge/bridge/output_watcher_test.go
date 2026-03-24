package bridge

import (
	"strings"
	"testing"
)

func TestExtractNewLines_AppendedContent(t *testing.T) {
	old := "line1\nline2"
	cur := "line1\nline2\nline3"
	result := extractNewLines(old, cur)
	if result != "line3" {
		t.Errorf("got %q, want %q", result, "line3")
	}
}

func TestExtractNewLines_ScreenScrolled(t *testing.T) {
	// Screen scrolled: last line of old appears in current, with new content after
	old := "line1\nline2\nline3"
	cur := "line2\nline3\nline4"
	result := extractNewLines(old, cur)
	if result != "line4" {
		t.Errorf("got %q, want %q", result, "line4")
	}
}

func TestExtractNewLines_CompleteRewrite(t *testing.T) {
	old := "alpha\nbeta"
	cur := "gamma\ndelta"
	result := extractNewLines(old, cur)
	// Complete change — should return all current content
	if result != "gamma\ndelta" {
		t.Errorf("got %q, want %q", result, "gamma\ndelta")
	}
}

func TestExtractNewLines_EmptyLast(t *testing.T) {
	cur := "hello\nworld"
	result := extractNewLines("", cur)
	if result != "hello\nworld" {
		t.Errorf("got %q, want %q", result, "hello\nworld")
	}
}

func TestStripANSI_Colors(t *testing.T) {
	input := "\x1b[31mred\x1b[0m"
	result := stripANSI(input)
	if result != "red" {
		t.Errorf("got %q, want %q", result, "red")
	}
}

func TestStripANSI_OSC(t *testing.T) {
	input := "\x1b]0;title\x07text"
	result := stripANSI(input)
	if result != "text" {
		t.Errorf("got %q, want %q", result, "text")
	}
}

func TestStripANSI_CursorMoves(t *testing.T) {
	input := "\x1b[2Jtext"
	result := stripANSI(input)
	if result != "text" {
		t.Errorf("got %q, want %q", result, "text")
	}
}

func TestStripANSI_PreservesNewlines(t *testing.T) {
	input := "a\nb\n"
	result := stripANSI(input)
	if result != "a\nb\n" {
		t.Errorf("got %q, want %q", result, "a\nb\n")
	}
}

func TestCleanTerminalOutput_ExcessiveBlanks(t *testing.T) {
	input := "a\n\n\n\nb"
	result := cleanTerminalOutput(input)
	if result != "a\n\nb" {
		t.Errorf("got %q, want %q", result, "a\n\nb")
	}
}

func TestSplitMessage_WithinLimit(t *testing.T) {
	text := "short message"
	result := splitMessage(text, 4000)
	if len(result) != 1 {
		t.Fatalf("len = %d, want 1", len(result))
	}
	if result[0] != text {
		t.Errorf("got %q, want %q", result[0], text)
	}
}

func TestSplitMessage_ExceedsLimit(t *testing.T) {
	// Build a long message with many lines
	var lines []string
	for i := 0; i < 100; i++ {
		lines = append(lines, strings.Repeat("x", 50))
	}
	text := strings.Join(lines, "\n")
	// Total is 100*50 + 99 newlines = 5099 chars

	result := splitMessage(text, 200)
	if len(result) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(result))
	}
	// Each chunk should be within the limit
	for i, chunk := range result {
		if len(chunk) > 200 {
			t.Errorf("chunk[%d] len = %d, exceeds limit 200", i, len(chunk))
		}
	}
}

func TestSplitMessage_HardSplitsLongSingleLine(t *testing.T) {
	text := strings.Repeat("x", 450)

	result := splitMessage(text, 200)
	if len(result) != 3 {
		t.Fatalf("len = %d, want 3", len(result))
	}
	for i, chunk := range result {
		if len(chunk) > 200 {
			t.Fatalf("chunk[%d] len = %d, exceeds limit 200", i, len(chunk))
		}
	}
	if strings.Join(result, "") != text {
		t.Fatal("split chunks did not preserve content")
	}
}
