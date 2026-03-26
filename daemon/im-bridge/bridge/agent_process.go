package bridge

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

// AgentProcess represents a running agent subprocess with stdout pipe for reading stream-json.
type AgentProcess struct {
	Cmd    *exec.Cmd
	Stdout io.ReadCloser
	Stdin  io.WriteCloser
}

// LaunchAgentProcess starts the agent as a direct subprocess and returns pipes for I/O.
// This bypasses the cmux terminal, reading stream-json directly from stdout.
func LaunchAgentProcess(session *Session, prompt string, aiCfg config.AIConfig, mediaDir string) (*AgentProcess, error) {
	if session == nil {
		return nil, fmt.Errorf("session is nil")
	}

	promptPath, err := writeTurnPromptFile(mediaDir, prompt)
	if err != nil {
		return nil, err
	}

	var bin string
	var args []string

	switch session.agentType() {
	case AgentTypeClaude:
		bin = coalesce(strings.TrimSpace(aiCfg.ClaudePath), "claude")
		args = []string{"-p", "--output-format", "stream-json", "--verbose", "--include-partial-messages"}
		if model := coalesce(session.model(), aiCfg.ClaudeModel); model != "" {
			args = append(args, "--model", model)
		}
		if effort := session.effort(); effort != "" {
			args = append(args, "--effort", effort)
		}
		if perm := coalesce(session.permissionMode(), aiCfg.ClaudePermissionMode); perm != "" {
			args = append(args, "--permission-mode", perm)
		}
		if id := session.providerSessionID(); id != "" {
			args = append(args, "--resume", id)
		}
		args = append(args, session.extraArgs()...)
	case AgentTypeCodex:
		bin = coalesce(strings.TrimSpace(aiCfg.CodexPath), "codex")
		args = []string{"exec", "--json"}
		if session.providerSessionID() != "" {
			args = []string{"exec", "resume", "--json"}
		}
		if model := coalesce(session.model(), aiCfg.CodexModel); model != "" {
			args = append(args, "--model", model)
		}
		if sandbox := strings.TrimSpace(aiCfg.CodexSandbox); sandbox != "" {
			args = append(args, "--sandbox", sandbox)
		}
		args = append(args, session.extraArgs()...)
		if session.providerSessionID() != "" {
			args = append(args, session.providerSessionID())
		}
		args = append(args, "-")
	default:
		os.Remove(promptPath)
		return nil, fmt.Errorf("unsupported agent type for subprocess: %s", session.agentType())
	}

	cmd := exec.Command(bin, args...)
	cmd.Dir = coalesce(session.workdir(), aiCfg.Workdir)
	if cmd.Dir == "" {
		cmd.Dir, _ = os.Getwd()
	}

	// Feed prompt via stdin from file
	promptFile, err := os.Open(promptPath)
	if err != nil {
		return nil, fmt.Errorf("open prompt file: %w", err)
	}
	cmd.Stdin = promptFile

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		promptFile.Close()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	// Capture stderr for debugging (don't pipe to user)
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		promptFile.Close()
		return nil, fmt.Errorf("start agent: %w", err)
	}

	// Clean up prompt file in background after process starts
	go func() {
		cmd.Wait()
		promptFile.Close()
		os.Remove(promptPath)
	}()

	return &AgentProcess{
		Cmd:    cmd,
		Stdout: stdout,
	}, nil
}

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
		bin := coalesce(strings.TrimSpace(aiCfg.ClaudePath), "claude")
		parts := []string{bin}
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
		parts = append(parts, session.extraArgs()...)
		return strings.Join(parts, " ")
	case AgentTypeCodex:
		bin := coalesce(strings.TrimSpace(aiCfg.CodexPath), "codex")
		parts := []string{bin}
		model := coalesce(session.model(), aiCfg.CodexModel)
		if model != "" {
			parts = append(parts, "--model", model)
		}
		parts = append(parts, session.extraArgs()...)
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
	workdir := coalesce(session.workdir(), aiCfg.Workdir)
	if workdir != "" {
		cmd = "cd " + shellQuote(workdir) + " && " + cmd
	}
	return cmux.SendText(wsID, surfaceID, cmd+"\n")
}

func ShouldLaunchPersistentAgent(session *Session) bool {
	return false
}

func PrepareTurnCommand(session *Session, prompt string, aiCfg config.AIConfig, mediaDir string) (string, error) {
	if session == nil {
		return "", fmt.Errorf("session is nil")
	}
	switch session.agentType() {
	case AgentTypeClaude:
		return prepareClaudeTurnCommand(session, prompt, aiCfg, mediaDir)
	case AgentTypeCodex:
		return prepareCodexTurnCommand(session, prompt, aiCfg, mediaDir)
	default:
		return prompt + "\n", nil
	}
}

// StopAgent sends Ctrl+C to the terminal surface.
func StopAgent(cmux *CmuxClient, workspaceID, surfaceID string) error {
	return cmux.SendText(workspaceID, surfaceID, "\x03")
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	if !strings.ContainsAny(value, " \t'\"\\$`()[]{}*?!&;<>|") {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func prepareCodexTurnCommand(session *Session, prompt string, aiCfg config.AIConfig, mediaDir string) (string, error) {
	promptPath, err := writeTurnPromptFile(mediaDir, prompt)
	if err != nil {
		return "", err
	}

	bin := coalesce(strings.TrimSpace(aiCfg.CodexPath), "codex")
	args := []string{bin, "exec"}
	if session.providerSessionID() != "" {
		args = append(args, "resume", "--json")
	} else {
		args = append(args, "--json")
	}
	if model := coalesce(session.model(), aiCfg.CodexModel); model != "" {
		args = append(args, "--model", model)
	}
	if sandbox := strings.TrimSpace(aiCfg.CodexSandbox); sandbox != "" {
		args = append(args, "--sandbox", sandbox)
	}
	args = append(args, session.extraArgs()...)
	if session.providerSessionID() != "" {
		args = append(args, session.providerSessionID())
	}
	args = append(args, "-")

	command := strings.Join(args, " ")
	command = fmt.Sprintf("%s < %s; rm -f %s", command, shellQuote(promptPath), shellQuote(promptPath))

	workdir := coalesce(session.workdir(), aiCfg.Workdir)
	if workdir != "" {
		command = "cd " + shellQuote(workdir) + " && " + command
	}
	return command + "\n", nil
}

func prepareClaudeTurnCommand(session *Session, prompt string, aiCfg config.AIConfig, mediaDir string) (string, error) {
	promptPath, err := writeTurnPromptFile(mediaDir, prompt)
	if err != nil {
		return "", err
	}

	bin := coalesce(strings.TrimSpace(aiCfg.ClaudePath), "claude")
	args := []string{bin, "-p", "--output-format", "stream-json", "--verbose", "--include-partial-messages"}
	if model := coalesce(session.model(), aiCfg.ClaudeModel); model != "" {
		args = append(args, "--model", model)
	}
	if effort := session.effort(); effort != "" {
		args = append(args, "--effort", effort)
	}
	if perm := coalesce(session.permissionMode(), aiCfg.ClaudePermissionMode); perm != "" {
		args = append(args, "--permission-mode", perm)
	}
	if id := session.providerSessionID(); id != "" {
		args = append(args, "--resume", id)
	}
	args = append(args, session.extraArgs()...)
	command := strings.Join(args, " ")
	command = fmt.Sprintf("%s < %s; rm -f %s", command, shellQuote(promptPath), shellQuote(promptPath))

	workdir := coalesce(session.workdir(), aiCfg.Workdir)
	if workdir != "" {
		command = "cd " + shellQuote(workdir) + " && " + command
	}
	return command + "\n", nil
}

func writeTurnPromptFile(mediaDir, prompt string) (string, error) {
	dir := strings.TrimSpace(mediaDir)
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "cmux-im-bridge-prompts")
	} else {
		dir = filepath.Join(dir, "prompts")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", fmt.Errorf("create prompt dir: %w", err)
	}

	name := fmt.Sprintf("cmux-im-bridge-prompt-%d.txt", time.Now().UnixNano())
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(prompt), 0600); err != nil {
		return "", fmt.Errorf("write prompt file: %w", err)
	}
	return path, nil
}
