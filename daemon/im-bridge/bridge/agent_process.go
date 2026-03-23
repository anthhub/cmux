package bridge

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

const (
	AgentTypeClaude = "claude"
	AgentTypeCodex  = "codex"
	AgentTypeShell  = "shell"
)

// TurnResult tracks an in-flight one-shot AI turn.
type TurnResult struct {
	Events <-chan StreamEvent
	Done   <-chan error
}

type runningTurn struct {
	cancel context.CancelFunc
	cmd    *exec.Cmd
}

// AgentRunner launches one-shot Claude/Codex turns and tracks the current turn per session.
type AgentRunner struct {
	cfg     config.AIConfig
	mu      sync.Mutex
	running map[string]*runningTurn
}

// NewAgentRunner creates a new runner for subprocess-backed AI agents.
func NewAgentRunner(cfg config.AIConfig) *AgentRunner {
	return &AgentRunner{
		cfg:     cfg,
		running: make(map[string]*runningTurn),
	}
}

// StartTurn launches one AI turn for the session and streams normalized events.
func (r *AgentRunner) StartTurn(ctx context.Context, session *Session, prompt string) (*TurnResult, error) {
	provider := session.agentType()
	if provider == "" || provider == AgentTypeShell {
		return nil, fmt.Errorf("session %s is not an AI-backed session", session.Name)
	}

	ctx, cancel := context.WithCancel(ctx)
	cmd, err := r.commandForSession(ctx, session, prompt)
	if err != nil {
		cancel()
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start %s turn: %w", provider, err)
	}

	r.mu.Lock()
	r.running[session.ID] = &runningTurn{cancel: cancel, cmd: cmd}
	r.mu.Unlock()

	events := make(chan StreamEvent, 128)
	done := make(chan error, 1)

	go r.collectTurn(session, provider, cmd, stdout, stderr, events, done)

	return &TurnResult{
		Events: events,
		Done:   done,
	}, nil
}

// StopTurn cancels a currently running turn.
func (r *AgentRunner) StopTurn(sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	turn := r.running[sessionID]
	if turn == nil {
		return fmt.Errorf("no running turn for session %s", sessionID)
	}
	turn.cancel()
	return nil
}

func (r *AgentRunner) collectTurn(session *Session, provider string, cmd *exec.Cmd, stdout, stderr io.ReadCloser, events chan<- StreamEvent, done chan<- error) {
	defer close(events)

	parse := ParseCodexStream
	if provider == AgentTypeClaude {
		parse = ParseClaudeStream
	}

	var wg sync.WaitGroup
	var stderrBuf bytes.Buffer
	wg.Add(2)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			parsed, err := parse(scanner.Bytes())
			if err != nil {
				events <- StreamEvent{
					Type:     StreamEventError,
					Provider: provider,
					Content:  err.Error(),
				}
				continue
			}
			for _, event := range parsed {
				r.applyInitEvent(session, event)
				events <- event
			}
		}
		if err := scanner.Err(); err != nil {
			events <- StreamEvent{
				Type:     StreamEventError,
				Provider: provider,
				Content:  fmt.Sprintf("read stdout: %v", err),
			}
		}
	}()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(&stderrBuf, stderr)
	}()

	waitErr := cmd.Wait()
	wg.Wait()

	r.mu.Lock()
	delete(r.running, session.ID)
	r.mu.Unlock()

	if stderrText := strings.TrimSpace(stderrBuf.String()); stderrText != "" {
		events <- StreamEvent{
			Type:     StreamEventError,
			Provider: provider,
			Content:  stderrText,
		}
	}

	done <- waitErr
	close(done)
}

func (r *AgentRunner) applyInitEvent(session *Session, event StreamEvent) {
	if event.Type != StreamEventInit {
		return
	}
	switch event.Provider {
	case AgentTypeClaude:
		if sessionID, _ := event.Meta["session_id"].(string); sessionID != "" {
			session.setProviderSessionID(sessionID)
		}
	case AgentTypeCodex:
		if threadID, _ := event.Meta["thread_id"].(string); threadID != "" {
			session.setProviderSessionID(threadID)
		}
	}
}

func (r *AgentRunner) commandForSession(ctx context.Context, session *Session, prompt string) (*exec.Cmd, error) {
	workdir, err := resolveWorkdir(r.cfg.Workdir)
	if err != nil {
		return nil, err
	}

	switch session.agentType() {
	case AgentTypeClaude:
		path := r.cfg.ClaudePath
		if path == "" {
			path = "claude"
		}
		args := []string{"-p", "--verbose", "--output-format", "stream-json", "--include-partial-messages"}
		if permission := strings.TrimSpace(session.permissionMode()); permission != "" {
			args = append(args, "--permission-mode", permission)
		} else if permission := strings.TrimSpace(r.cfg.ClaudePermissionMode); permission != "" {
			args = append(args, "--permission-mode", permission)
		}
		if model := strings.TrimSpace(session.model()); model != "" {
			args = append(args, "--model", model)
		} else if model := strings.TrimSpace(r.cfg.ClaudeModel); model != "" {
			args = append(args, "--model", model)
		}
		if effort := strings.TrimSpace(session.effort()); effort != "" {
			args = append(args, "--effort", effort)
		}
		if providerID := strings.TrimSpace(session.providerSessionID()); providerID != "" {
			args = append(args, "--resume", providerID)
		}
		args = append(args, prompt)
		cmd := exec.CommandContext(ctx, path, args...)
		cmd.Dir = workdir
		return cmd, nil

	case AgentTypeCodex:
		path := r.cfg.CodexPath
		if path == "" {
			path = "codex"
		}
		args := []string{"exec"}
		if providerID := strings.TrimSpace(session.providerSessionID()); providerID != "" {
			args = append(args, "resume", providerID)
		}
		args = append(args, "--json")
		if sandbox := strings.TrimSpace(r.cfg.CodexSandbox); sandbox != "" && !strings.Contains(strings.Join(args, " "), " resume ") {
			args = append(args, "--sandbox", sandbox)
		}
		if model := strings.TrimSpace(session.model()); model != "" {
			args = append(args, "--model", model)
		} else if model := strings.TrimSpace(r.cfg.CodexModel); model != "" {
			args = append(args, "--model", model)
		}
		args = append(args, prompt)
		cmd := exec.CommandContext(ctx, path, args...)
		cmd.Dir = workdir
		return cmd, nil
	default:
		return nil, fmt.Errorf("unsupported provider %q", session.agentType())
	}
}

func resolveWorkdir(configured string) (string, error) {
	if strings.TrimSpace(configured) != "" {
		if filepath.IsAbs(configured) {
			return configured, nil
		}
		return filepath.Abs(configured)
	}

	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve cwd: %w", err)
	}

	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return cwd, nil
		}
		dir = parent
	}
}
