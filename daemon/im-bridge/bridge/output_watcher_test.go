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

// --- stripBoxChars tests ---

func TestStripBoxChars_ClaudeCodeBox(t *testing.T) {
	input := "╭─ Read file ─────╮\n│ src/login.tsx   │\n╰─────────────────╯"
	result := stripBoxChars(input)
	if strings.Contains(result, "╭") || strings.Contains(result, "│") || strings.Contains(result, "─") {
		t.Errorf("box chars not stripped: %q", result)
	}
	if !strings.Contains(result, "Read file") {
		t.Errorf("content lost: %q", result)
	}
	if !strings.Contains(result, "src/login.tsx") {
		t.Errorf("content lost: %q", result)
	}
}

func TestStripBoxChars_ThickVariants(t *testing.T) {
	input := "┏━━━┓\n┃ hi ┃\n┗━━━┛"
	result := stripBoxChars(input)
	if strings.ContainsAny(result, "┏━┓┃┗┛") {
		t.Errorf("thick box chars not stripped: %q", result)
	}
	if !strings.Contains(result, "hi") {
		t.Errorf("content lost: %q", result)
	}
}

func TestStripBoxChars_PreservesNonBox(t *testing.T) {
	input := "hello world\n✅ done"
	result := stripBoxChars(input)
	if result != input {
		t.Errorf("got %q, want %q", result, input)
	}
}

// --- stripSpinner tests ---

func TestStripSpinner_BrailleSpinner(t *testing.T) {
	input := "⠋ Working...\n✅ Done\n⠙ Loading..."
	result := stripSpinner(input)
	if strings.Contains(result, "⠋") || strings.Contains(result, "⠙") {
		t.Errorf("spinner not stripped: %q", result)
	}
	if !strings.Contains(result, "✅ Done") {
		t.Errorf("content lost: %q", result)
	}
}

func TestStripSpinner_RecordIndicator(t *testing.T) {
	input := "⏺ Reading file...\nsome content"
	result := stripSpinner(input)
	if strings.Contains(result, "⏺") {
		t.Errorf("record indicator not stripped: %q", result)
	}
	if !strings.Contains(result, "some content") {
		t.Errorf("content lost: %q", result)
	}
}

func TestStripSpinner_BakedLine(t *testing.T) {
	input := "result here\nBaked for 3.2s\nmore text"
	result := stripSpinner(input)
	if strings.Contains(result, "Baked for") {
		t.Errorf("baked line not stripped: %q", result)
	}
	if !strings.Contains(result, "result here") || !strings.Contains(result, "more text") {
		t.Errorf("content lost: %q", result)
	}
}

func TestStripSpinner_WorkingThinking(t *testing.T) {
	input := "Working...\nThinking...\nactual output"
	result := stripSpinner(input)
	if strings.Contains(result, "Working...") || strings.Contains(result, "Thinking...") {
		t.Errorf("status lines not stripped: %q", result)
	}
	if !strings.Contains(result, "actual output") {
		t.Errorf("content lost: %q", result)
	}
}

func TestStripSpinner_CookedCrunched(t *testing.T) {
	input := "Cooked for 1.5s\nCrunched for 2s"
	result := stripSpinner(input)
	trimmed := strings.TrimSpace(result)
	if trimmed != "" {
		t.Errorf("expected empty, got %q", trimmed)
	}
}

// --- stripPrompt tests ---

func TestStripPrompt_PurePrompt(t *testing.T) {
	input := "qiyuan@MacBook-Pro ~/cmux % "
	result := stripPrompt(input)
	trimmed := strings.TrimSpace(result)
	if trimmed != "" {
		t.Errorf("expected empty, got %q", trimmed)
	}
}

func TestStripPrompt_PromptWithCommand(t *testing.T) {
	input := "qiyuan@MacBook-Pro ~/cmux % echo hello"
	result := stripPrompt(input)
	trimmed := strings.TrimSpace(result)
	if trimmed != "echo hello" {
		t.Errorf("got %q, want %q", trimmed, "echo hello")
	}
}

func TestStripPrompt_DollarPrompt(t *testing.T) {
	input := "user@hostname /tmp $ "
	result := stripPrompt(input)
	trimmed := strings.TrimSpace(result)
	if trimmed != "" {
		t.Errorf("expected empty, got %q", trimmed)
	}
}

func TestStripPrompt_CondaPrefix(t *testing.T) {
	input := "(myenv) user@host ~ % "
	result := stripPrompt(input)
	trimmed := strings.TrimSpace(result)
	if trimmed != "" {
		t.Errorf("expected empty, got %q", trimmed)
	}
}

func TestStripPrompt_PreservesNonPrompt(t *testing.T) {
	input := "this is regular text\nsome output"
	result := stripPrompt(input)
	if result != input {
		t.Errorf("got %q, want %q", result, input)
	}
}

// --- compressBlankLines tests ---

func TestCompressBlankLines_LeadingTrailing(t *testing.T) {
	input := "\n\nhello\n\n"
	result := compressBlankLines(input)
	if result != "hello" {
		t.Errorf("got %q, want %q", result, "hello")
	}
}

func TestCompressBlankLines_ThreePlusToOne(t *testing.T) {
	input := "a\n\n\n\n\nb"
	result := compressBlankLines(input)
	expected := "a\n\nb"
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}

func TestCompressBlankLines_TwoBlankPreserved(t *testing.T) {
	input := "a\n\nb"
	result := compressBlankLines(input)
	if result != input {
		t.Errorf("got %q, want %q", result, input)
	}
}

// --- stripANSI enhanced tests ---

func TestStripANSI_8bitCSI(t *testing.T) {
	// 0xC2 0x9B is UTF-8 encoding of U+009B
	input := string([]byte{0xC2, 0x9B}) + "31mred"
	result := stripANSI(input)
	if result != "red" {
		t.Errorf("got %q, want %q", result, "red")
	}
}

func TestStripANSI_PrivateSequence(t *testing.T) {
	input := "\x1b[?25htext"
	result := stripANSI(input)
	if result != "text" {
		t.Errorf("got %q, want %q", result, "text")
	}
}

func TestStripANSI_ControlChars(t *testing.T) {
	input := "hello\x01\x02world"
	result := stripANSI(input)
	if result != "helloworld" {
		t.Errorf("got %q, want %q", result, "helloworld")
	}
}

// --- cleanTerminalOutput integration test ---

func TestCleanTerminalOutput_ClaudeCodeTUI(t *testing.T) {
	input := "╭──────────────────────────╮\n" +
		"│ ✅ Fixed src/login.tsx   │\n" +
		"│                          │\n" +
		"│ Added null check at L42  │\n" +
		"╰──────────────────────────╯\n" +
		"qiyuan@MacBook-Pro ~/cmux % "

	result := cleanTerminalOutput(input)

	if strings.ContainsAny(result, "╭╮╰╯│─") {
		t.Errorf("box chars still present: %q", result)
	}
	if strings.Contains(result, "qiyuan@MacBook-Pro") {
		t.Errorf("prompt still present: %q", result)
	}
	if !strings.Contains(result, "Fixed src/login.tsx") {
		t.Errorf("content lost: %q", result)
	}
	if !strings.Contains(result, "Added null check at L42") {
		t.Errorf("content lost: %q", result)
	}
}

func TestCleanTerminalOutput_SpinnerAndContent(t *testing.T) {
	input := "⠋ Working...\n\x1b[32m✅ Changes applied\x1b[0m\nBaked for 2.1s"
	result := cleanTerminalOutput(input)
	if strings.Contains(result, "Working...") {
		t.Errorf("spinner not removed: %q", result)
	}
	if strings.Contains(result, "Baked for") {
		t.Errorf("baked line not removed: %q", result)
	}
	if !strings.Contains(result, "Changes applied") {
		t.Errorf("content lost: %q", result)
	}
}

func TestCleanTerminalOutput_Empty(t *testing.T) {
	result := cleanTerminalOutput("")
	if result != "" {
		t.Errorf("expected empty, got %q", result)
	}
}

