package bridge

import (
	"context"
	"log"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/channels"
)

const (
	fastInterval = 500 * time.Millisecond
	slowInterval = 2 * time.Second
	idleInterval = 5 * time.Second
)

// watchOutput polls a terminal surface for output changes and streams diffs to IM.
// Implements OpenClaw-style conversational output:
// - Captures baseline BEFORE command, only sends genuinely NEW content
// - Debounces rapid changes (waits for output to stabilize)
// - Strips ANSI codes, cleans up terminal noise
func (sm *SessionManager) watchOutput(ctx context.Context, workspaceID string, session *Session, channelName, chatID string) {
	log.Printf("[watcher] starting for session %s (surface %s)", session.Name, session.SurfaceID)

	ticker := time.NewTicker(fastInterval)
	defer ticker.Stop()

	idleCount := 0
	lastSentContent := ""
	// Pending output buffer — accumulate changes before sending
	var pendingOutput string
	pendingTimer := time.NewTimer(0)
	<-pendingTimer.C // drain initial fire

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
			lastCleaned := cleanTerminalOutput(session.lastOutput())

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
			session.setLastOutput(currentOutput)
			idleCount = 0
			ticker.Reset(fastInterval)

			cleaned := strings.TrimSpace(diff)
			if cleaned == "" {
				continue
			}

			// Accumulate into pending buffer and debounce (wait 1s for output to stabilize)
			pendingOutput = cleaned
			resetTimer(pendingTimer, 1*time.Second)

		case <-pendingTimer.C:
			if pendingOutput == "" || pendingOutput == lastSentContent {
				continue
			}

			log.Printf("[watcher] sending %d chars for session %s", len(pendingOutput), session.Name)
			lastSentContent = pendingOutput

			for _, chunk := range splitMessage(pendingOutput, sm.channel.MaxMessageLength(channelName)) {
				if err := sm.channel.Send(channelName, chatID, channels.OutboundMessage{
					Text:   chunk,
					Format: "text",
				}); err != nil {
					log.Printf("[watcher] failed to send output: %v", err)
				}
			}
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

// cleanTerminalOutput strips ANSI codes, removes empty lines, and trims.
func cleanTerminalOutput(text string) string {
	cleaned := stripANSI(text)
	cleaned = strings.TrimSpace(cleaned)

	// Remove excessive blank lines
	for strings.Contains(cleaned, "\n\n\n") {
		cleaned = strings.ReplaceAll(cleaned, "\n\n\n", "\n\n")
	}

	return cleaned
}

// stripANSI removes ANSI escape sequences from text.
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

		// In OSC sequence, skip until BEL or ST
		if inOSC {
			if r == '\a' || (r == '\\' && i > 0 && s[i-1] == '\x1b') {
				inOSC = false
				inEscape = false
			}
			i += size
			continue
		}

		// In CSI escape sequence, skip until letter
		if inEscape {
			if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '~' {
				inEscape = false
			}
			i += size
			continue
		}

		// Skip other control characters except newline/tab
		if r < 32 && r != '\n' && r != '\t' {
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
	if len(text) <= maxLen {
		return []string{text}
	}

	var chunks []string
	lines := strings.Split(text, "\n")
	var current strings.Builder

	for _, line := range lines {
		if current.Len()+len(line)+1 > maxLen && current.Len() > 0 {
			chunks = append(chunks, current.String())
			current.Reset()
		}
		if current.Len() > 0 {
			current.WriteByte('\n')
		}
		current.WriteString(line)
	}

	if current.Len() > 0 {
		chunks = append(chunks, current.String())
	}

	return chunks
}
