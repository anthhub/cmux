package bridge

import (
	"context"
	"log"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	fastInterval   = 200 * time.Millisecond
	slowInterval   = 2 * time.Second
	idleInterval   = 5 * time.Second
	maxPendingSize = 64 * 1024 // 64KB cap to prevent unbounded growth
)

// watchOutput polls a terminal surface for output changes and streams diffs to IM.
// Implements OpenClaw-style conversational output:
// - Captures baseline BEFORE command, only sends genuinely NEW content
// - Debounces rapid changes (waits for output to stabilize)
// - Strips ANSI codes, cleans up terminal noise
// - In AI mode, attempts to parse stream-json lines and route them through IMPresenter
func (sm *SessionManager) watchOutput(ctx context.Context, workspaceID string, session *Session, channelName, chatID, contextToken string) {
	var presenter *IMPresenter
	defer func() {
		session.setRunningTurn(false)
		if presenter != nil {
			presenter.Close()
		}
	}()

	log.Printf("[watcher] starting for session %s (surface %s)", session.Name, session.SurfaceID)

	isAI := session.agentType() != AgentTypeShell
	bufferedIM := isAI && strings.EqualFold(channelName, "wechat")

	// For AI mode, create a per-turn presenter with a control_request callback.
	if isAI {
		presenter = NewIMPresenter(sm.channel, channelName, chatID, contextToken, session.verbose())
		if bufferedIM {
			presenter.SetBufferedMode("处理中...", 2*time.Second)
		}
		presenter.onControlRequest = func(event StreamEvent) {
			sm.handleStreamEvent(session, event)
		}
	}

	ticker := time.NewTicker(fastInterval)
	defer ticker.Stop()

	idleCount := 0
	lastSentContent := ""
	// Pending output buffer — accumulate changes before sending (shell mode and non-buffered AI only)
	var pendingOutput string
	pendingTimer := time.NewTimer(0)
	<-pendingTimer.C // drain initial fire

	// Track processed lines to prevent duplicates across polling cycles.
	// extractNewLines can return the same lines when the terminal scrolls.
	processedLines := make(map[string]bool)
	defer func() {
		if !pendingTimer.Stop() {
			select {
			case <-pendingTimer.C:
			default:
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[watcher] context cancelled for session %s", session.Name)
			return
		case <-ticker.C:
			if !session.isWatching() {
				log.Printf("[watcher] stopped for session %s", session.Name)
				return
			}

			currentOutput, err := sm.cmux.ReadText(workspaceID, session.SurfaceID)
			if err != nil {
				idleCount++
				continue
			}

			// Normalize for comparison — strip ANSI first to avoid false diffs from cursor/color changes
			currentCleaned := cleanTerminalOutput(currentOutput)

			// Atomically swap lastOutput to avoid TOCTOU race between read and write
			previousOutput := session.swapLastOutput(currentOutput)
			lastCleaned := cleanTerminalOutput(previousOutput)

			if currentCleaned == lastCleaned || currentCleaned == "" {
				idleCount++
				if idleCount > 60 {
					ticker.Reset(idleInterval)
				} else if idleCount > 10 {
					ticker.Reset(slowInterval)
				}
				continue
			}

			// Genuine new content detected
			diff := extractNewLines(lastCleaned, currentCleaned)
			idleCount = 0
			ticker.Reset(fastInterval)

			if isAI && presenter != nil {
				// AI mode: only use stream-json events via presenter.
				// Never fall back to plain-text — terminal screen content is unreliable
				// (wrapped lines, scrolling, duplicates) and causes repeated/garbled output.
				for _, line := range strings.Split(diff, "\n") {
					events, err := ParseLineForProvider(session.agentType(), line)
					if err != nil || len(events) == 0 {
						continue
					}
					for _, event := range events {
						presenter.HandleEvent(event)
						sm.handleStreamEvent(session, event)
					}
				}
			} else {
				// Shell mode: plain-text accumulation path
				cleaned := strings.TrimSpace(diff)
				if cleaned == "" {
					continue
				}
				if pendingOutput == "" {
					pendingOutput = cleaned
				} else {
					pendingOutput += "\n" + cleaned
				}
				if len(pendingOutput) > maxPendingSize {
					// Force flush to prevent unbounded growth
					if pendingOutput != lastSentContent {
						log.Printf("[watcher] force flush %d chars (cap) for session %s", len(pendingOutput), session.Name)
						lastSentContent = pendingOutput
						sm.sendTextWithContext(channelName, chatID, contextToken, pendingOutput)
					}
					pendingOutput = ""
					pendingTimer.Stop()
				} else {
					resetTimer(pendingTimer, 1*time.Second)
				}
			}

		case <-pendingTimer.C:
			if pendingOutput == "" || pendingOutput == lastSentContent {
				continue
			}

			log.Printf("[watcher] sending %d chars for session %s", len(pendingOutput), session.Name)
			lastSentContent = pendingOutput

			sm.sendTextWithContext(channelName, chatID, contextToken, pendingOutput)
			pendingOutput = ""
		}
	}
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

// extractNewLines returns only the lines that are new in currentOutput.
// Compares from the end of lastOutput to find genuinely new content.
func extractNewLines(lastOutput, currentOutput string) string {
	if lastOutput == "" {
		return currentOutput
	}

	lastLines := strings.Split(strings.TrimRight(lastOutput, "\n"), "\n")
	currentLines := strings.Split(strings.TrimRight(currentOutput, "\n"), "\n")

	if len(currentLines) <= len(lastLines) {
		// Screen might have scrolled or been redrawn
		// Find the last matching line from lastOutput in currentOutput
		lastLine := ""
		if len(lastLines) > 0 {
			lastLine = lastLines[len(lastLines)-1]
		}

		// Search for lastLine in currentOutput
		matchIdx := -1
		for i := len(currentLines) - 1; i >= 0; i-- {
			if currentLines[i] == lastLine {
				matchIdx = i
				break
			}
		}

		if matchIdx >= 0 && matchIdx < len(currentLines)-1 {
			return strings.Join(currentLines[matchIdx+1:], "\n")
		}
		// Complete screen change - return all
		return strings.Join(currentLines, "\n")
	}

	// More lines now - find where new content starts
	// Match from the end of lastLines
	matchLen := 0
	for i := 0; i < len(lastLines) && i < len(currentLines); i++ {
		if lastLines[i] == currentLines[i] {
			matchLen = i + 1
		} else {
			break
		}
	}

	if matchLen > 0 && matchLen < len(currentLines) {
		return strings.Join(currentLines[matchLen:], "\n")
	}

	return strings.Join(currentLines, "\n")
}

// cleanTerminalOutput strips ANSI codes, box drawing chars, spinners, prompts, and compresses blanks.
func cleanTerminalOutput(text string) string {
	cleaned := stripANSI(text)
	cleaned = stripBoxChars(cleaned)
	cleaned = stripSpinner(cleaned)
	cleaned = stripClaudeTUI(cleaned)
	cleaned = stripPrompt(cleaned)
	cleaned = stripShellStartupNoise(cleaned)
	cleaned = stripBridgeCommandEcho(cleaned)
	cleaned = compressBlankLines(cleaned)
	return strings.TrimSpace(cleaned)
}

func stripShellStartupNoise(s string) string {
	lines := strings.Split(s, "\n")
	result := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "Last login:"):
			continue
		default:
			result = append(result, line)
		}
	}
	return strings.Join(result, "\n")
}

func stripBridgeCommandEcho(s string) string {
	lines := strings.Split(s, "\n")
	result := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.Contains(trimmed, "cmux-im-bridge-prompt-"):
			continue
		case strings.Contains(trimmed, "claude -p --output-format stream-json"):
			continue
		case strings.HasPrefix(trimmed, "codex exec --json"):
			continue
		case strings.HasPrefix(trimmed, "codex exec resume --json"):
			continue
		default:
			result = append(result, line)
		}
	}
	return strings.Join(result, "\n")
}

// looksLikeJSONFragment detects partial JSON lines that leaked from stream-json output
// (e.g. a long JSON line split across terminal screen rows).
func looksLikeJSONFragment(s string) bool {
	if len(s) == 0 {
		return false
	}
	// Complete JSON objects are handled by the parser, not filtered here
	if s[0] == '{' {
		return false
	}
	// Fragments with JSON key-value pairs
	if strings.Contains(s, `":"`) {
		return true
	}
	// Fragments ending with JSON closing (e.g. `0bde677531"}`)
	if strings.HasSuffix(s, `"}`) || strings.HasSuffix(s, `"}`) {
		return true
	}
	// Hex-like content ending with } (UUID fragments from stream-json)
	if strings.HasSuffix(s, "}") && !strings.Contains(s, " ") {
		return true
	}
	return false
}

// compressBlankLines collapses 3+ consecutive blank lines into 1, and trims leading/trailing blank lines.
func compressBlankLines(s string) string {
	lines := strings.Split(s, "\n")

	// Trim leading blank lines
	start := 0
	for start < len(lines) && strings.TrimSpace(lines[start]) == "" {
		start++
	}
	// Trim trailing blank lines
	end := len(lines)
	for end > start && strings.TrimSpace(lines[end-1]) == "" {
		end--
	}
	if start >= end {
		return ""
	}
	lines = lines[start:end]

	// Compress 3+ consecutive blank lines to 1
	var result []string
	blankCount := 0
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			blankCount++
			if blankCount <= 1 {
				result = append(result, line)
			}
		} else {
			blankCount = 0
			result = append(result, line)
		}
	}
	return strings.Join(result, "\n")
}

// stripBoxChars removes Unicode box drawing characters (U+2500-U+257F) and their thick/dashed variants.
func stripBoxChars(s string) string {
	var result strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if isBoxDrawing(r) {
			i += size
			continue
		}
		result.WriteRune(r)
		i += size
	}
	return result.String()
}

func isBoxDrawing(r rune) bool {
	// U+2500-U+257F: Box Drawing block
	return r >= 0x2500 && r <= 0x257F
}

// stripSpinner removes spinner/progress indicator lines from Claude Code TUI output.
func stripSpinner(s string) string {
	lines := strings.Split(s, "\n")
	var result []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if isSpinnerLine(trimmed) {
			continue
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

var spinnerRunes = map[rune]bool{
	'⏺': true, '⏳': true,
	'⠋': true, '⠙': true, '⠹': true, '⠸': true,
	'⠼': true, '⠴': true, '⠦': true, '⠧': true,
	'⠇': true, '⠏': true,
	'✻': true,
}

func isSpinnerLine(line string) bool {
	if line == "" {
		return false
	}
	// Lines that start with a spinner rune
	r, _ := utf8.DecodeRuneInString(line)
	if spinnerRunes[r] {
		return true
	}
	// Status-only lines
	lower := strings.ToLower(line)
	if lower == "working..." || lower == "thinking..." {
		return true
	}
	// "Baked for" / "Cooked for" / "Crunched for" lines
	if strings.HasPrefix(lower, "baked for") || strings.HasPrefix(lower, "cooked for") || strings.HasPrefix(lower, "crunched for") {
		return true
	}
	return false
}

// isClaudeTUILine returns true if the line is part of Claude Code's TUI chrome.
// Uses simple string matching (case-insensitive) to be robust against ANSI artifacts.
func isClaudeTUILine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)

	// Claude Code header
	if strings.Contains(lower, "claude code v") {
		return true
	}
	// Model info lines: "Opus 4.6 (1M context)"
	if (strings.Contains(lower, "opus") || strings.Contains(lower, "sonnet") || strings.Contains(lower, "haiku")) &&
		strings.Contains(lower, "context") {
		return true
	}
	// Status bar with MCPs/hooks/CLAUDE.md
	if strings.Contains(lower, "mcps") || strings.Contains(lower, "hooks") {
		if strings.Contains(trimmed, "|") {
			return true
		}
	}
	if strings.Contains(lower, "claude.md") && strings.Contains(trimmed, "|") {
		return true
	}
	// Navigation hints
	if strings.Contains(lower, "press ctrl-c") || strings.Contains(lower, "press ctrl+c") {
		return true
	}
	if strings.Contains(lower, "ctrl+g to edit") || strings.Contains(lower, "ctrl-g to edit") {
		return true
	}
	// Progress bar: 3+ block elements
	blockCount := 0
	for _, r := range trimmed {
		if r >= 0x2580 && r <= 0x259F { // Block Elements Unicode range
			blockCount++
			if blockCount >= 3 {
				return true
			}
		} else {
			blockCount = 0
		}
	}
	// Bare prompt markers (❯ followed by just "claude" or nothing)
	stripped := strings.TrimLeft(trimmed, "❯❮>$ ")
	stripped = strings.TrimSpace(stripped)
	if stripped == "" || stripped == "claude" || stripped == "codex" {
		return true
	}
	// Path-only lines: just "/Users/xxx"
	if strings.HasPrefix(trimmed, "/Users/") && !strings.Contains(trimmed, " ") {
		return true
	}
	return false
}

// stripClaudeTUI removes Claude Code TUI chrome lines from output.
func stripClaudeTUI(s string) string {
	lines := strings.Split(s, "\n")
	var result []string
	for _, line := range lines {
		if !isClaudeTUILine(line) {
			result = append(result, line)
		}
	}
	return strings.Join(result, "\n")
}

// promptRe matches common shell prompts: optional (env) prefix, user@host path % or $
var promptRe = regexp.MustCompile(`^(?:\([^)]+\)\s+)?[\w.-]+@[\w.-]+\s+[^\s]+\s+[%$]\s*$`)

// promptCmdRe matches a prompt followed by a command — we keep the command part
var promptCmdRe = regexp.MustCompile(`^(?:\([^)]+\)\s+)?[\w.-]+@[\w.-]+\s+[^\s]+\s+[%$]\s+(.+)$`)

// stripPrompt removes shell prompt lines. If a prompt is followed by a command, the command is kept.
func stripPrompt(s string) string {
	lines := strings.Split(s, "\n")
	var result []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if promptRe.MatchString(trimmed) {
			// Pure prompt with no command — skip
			continue
		}
		if m := promptCmdRe.FindStringSubmatch(trimmed); m != nil {
			// Prompt followed by command — keep the command
			result = append(result, m[1])
			continue
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

// stripANSI removes ANSI escape sequences and non-printable control characters from text.
func stripANSI(s string) string {
	var result strings.Builder
	inEscape := false
	inOSC := false

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])

		// Handle ESC character
		if r == '\x1b' {
			inEscape = true
			// Check if next char is ']' (OSC sequence)
			if i+size < len(s) && s[i+size] == ']' {
				inOSC = true
			}
			i += size
			continue
		}

		// Handle 8-bit CSI (U+009B)
		if r == 0x9B {
			inEscape = true
			i += size
			continue
		}

		// In OSC sequence, skip until BEL or ST
		if inOSC {
			if r == '\a' || (r == '\\' && i > 0 && s[i-1] == '\x1b') {
				inOSC = false
				inEscape = false
			}
			i += size
			continue
		}

		// In CSI escape sequence, skip until final byte
		if inEscape {
			if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '~' || r == '@' {
				inEscape = false
			}
			i += size
			continue
		}

		// Skip C0 control characters except newline/tab
		if r < 32 && r != '\n' && r != '\t' {
			i += size
			continue
		}

		// Skip C1 control characters (U+0080-U+009F)
		if r >= 0x80 && r <= 0x9F {
			i += size
			continue
		}

		// Skip other Unicode control characters (except common whitespace)
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			i += size
			continue
		}

		result.WriteRune(r)
		i += size
	}
	return result.String()
}

// splitMessage splits a long message into chunks respecting line boundaries.
func splitMessage(text string, maxLen int) []string {
	if maxLen <= 0 || len(text) <= maxLen {
		return []string{text}
	}

	var chunks []string
	lines := strings.Split(text, "\n")
	var current strings.Builder
	flush := func() {
		if current.Len() == 0 {
			return
		}
		chunks = append(chunks, current.String())
		current.Reset()
	}

	for _, line := range lines {
		remaining := line
		for {
			prefixLen := 0
			if current.Len() > 0 {
				prefixLen = 1
			}

			available := maxLen - current.Len() - prefixLen
			if available <= 0 {
				flush()
				continue
			}

			if len(remaining) <= available {
				if current.Len() > 0 {
					current.WriteByte('\n')
				}
				current.WriteString(remaining)
				break
			}

			if current.Len() > 0 {
				current.WriteByte('\n')
			}
			current.WriteString(remaining[:available])
			remaining = remaining[available:]
			flush()
		}
	}

	flush()

	return chunks
}
