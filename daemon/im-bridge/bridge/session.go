package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/channels"
	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

const (
	defaultAgentName   = "default"
	defaultSessionName = "default"
	maxFallbackPreview = 3500
)

// UsageSnapshot tracks the latest token usage reported for a session.
type UsageSnapshot struct {
	Provider          string
	InputTokens       int
	CachedInputTokens int
	OutputTokens      int
}

// UserState tracks all IM-managed agents for one user.
type UserState struct {
	ID             string
	Agents         map[string]*Agent
	AgentOrder     []string
	ActiveAgentKey string
	mu             sync.RWMutex
}

// Agent represents one IM assistant mapped to a cmux workspace.
type Agent struct {
	Name         string
	DefaultType  string
	WorkspaceID  string
	Sessions     map[string]*Session
	SessionOrder []string
	ActiveSID    string

	workspaceOnce sync.Once
	workspaceErr  error
	mu            sync.RWMutex
}

// Session represents one IM conversation mapped to a cmux surface.
type Session struct {
	ID          string
	SurfaceID   string
	Name        string
	SurfaceType string

	AgentType         string
	Model             string
	Effort            string
	Fast              bool
	Verbose           bool
	ProviderSessionID string
	PermissionMode    string
	PreviousModel     string
	PendingApproval   string
	LastUsage         UsageSnapshot
	LastOutput        string

	watchCancel context.CancelFunc
	runningTurn bool
	mu          sync.RWMutex
}

// SessionManager manages IM user -> agent -> session mappings.
type SessionManager struct {
	ctx              context.Context
	users            map[string]*UserState
	cmux             *CmuxClient
	channel          *channels.Manager
	runner           *AgentRunner
	defaultAgentType string
	mu               sync.RWMutex
}

// NewSessionManager creates a new session manager.
func NewSessionManager(ctx context.Context, cmux *CmuxClient, channel *channels.Manager, ai config.AIConfig) *SessionManager {
	defaultType := normalizeAgentType(ai.DefaultAgent)
	if defaultType == "" {
		defaultType = AgentTypeClaude
	}

	return &SessionManager{
		ctx:              ctx,
		users:            make(map[string]*UserState),
		cmux:             cmux,
		channel:          channel,
		runner:           NewAgentRunner(ai),
		defaultAgentType: defaultType,
	}
}

// HandleMessage processes one inbound IM message.
func (sm *SessionManager) HandleMessage(msg channels.InboundMessage) {
	text := strings.TrimSpace(msg.Text)
	if text == "" {
		return
	}

	user := sm.getOrCreateUser(msg.UserID)
	agent, err := sm.ensureActiveAgent(user)
	if err != nil {
		log.Printf("[session] failed to ensure active agent for %s: %v", msg.UserID, err)
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	if strings.HasPrefix(text, "!") {
		sm.handleBash(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "!")))
		return
	}

	if strings.HasPrefix(text, "/") {
		sm.handleCommand(user, agent, msg, text)
		return
	}

	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session. Use /new to create one.")
		return
	}

	switch session.agentType() {
	case AgentTypeShell:
		sm.handleBash(agent, msg, text)
	default:
		sm.handleAIInput(agent, session, msg, text)
	}
}

func (sm *SessionManager) handleCommand(user *UserState, agent *Agent, msg channels.InboundMessage, text string) {
	fields := strings.Fields(text)
	command := strings.TrimPrefix(fields[0], "/")
	args := fields[1:]

	switch command {
	case "help", "commands":
		sm.handleHelp(msg)
	case "status":
		sm.handleStatus(agent, msg)
	case "whoami", "id":
		sm.reply(msg, fmt.Sprintf("user=%s chat=%s channel=%s", msg.UserID, msg.ChatID, msg.ChannelName))
	case "agent":
		sm.handleAgentCommand(user, msg, strings.TrimSpace(strings.TrimPrefix(text, "/agent")))
	case "new":
		sm.handleNewSession(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "/new")))
	case "list":
		sm.handleListSessions(agent, msg)
	case "switch":
		sm.handleSwitchSession(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "/switch")))
	case "close":
		sm.handleCloseSession(agent, msg)
	case "session":
		sm.handleSessionCommand(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "/session")))
	case "reset":
		sm.handleResetSession(agent, msg)
	case "model":
		sm.handleModelCommand(agent, msg, args)
	case "think":
		sm.handleThinkCommand(agent, msg, args)
	case "fast":
		sm.handleFastCommand(agent, msg, args)
	case "verbose":
		sm.handleVerboseCommand(agent, msg, args)
	case "usage":
		sm.handleUsageCommand(agent, msg)
	case "context":
		sm.handleContextCommand(agent, msg)
	case "bash":
		sm.handleBash(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "/bash")))
	case "stop", "ctrlc":
		sm.handleStop(agent, msg)
	case "approve":
		sm.handleApprove(agent, msg)
	case "permission", "perm":
		sm.handlePermissionCommand(agent, msg, args)
	case "screenshot":
		sm.handleScreenshot(agent, msg)
	case "browse":
		sm.handleBrowse(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "/browse")))
	case "split":
		sm.handleSplit(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "/split")))
	case "panes":
		sm.handlePanes(agent, msg)
	case "tree":
		sm.handleTree(agent, msg)
	case "read":
		sm.handleRead(agent, msg, args)
	case "raw":
		sm.handleRaw(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "/raw")))
	default:
		sm.reply(msg, "Unknown command: /"+command+"\nUse /help to see supported commands.")
	}
}

func (sm *SessionManager) getOrCreateUser(userID string) *UserState {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if user, ok := sm.users[userID]; ok {
		return user
	}

	user := &UserState{
		ID:     userID,
		Agents: make(map[string]*Agent),
	}
	sm.users[userID] = user
	return user
}

func (sm *SessionManager) ensureActiveAgent(user *UserState) (*Agent, error) {
	agent := user.activeAgent()
	if agent == nil {
		agent = user.ensureAgent(defaultAgentName, sm.defaultAgentType)
	}

	created, err := sm.ensureAgentWorkspace(user.ID, agent)
	if err != nil {
		return nil, err
	}
	if err := sm.syncAgentSessions(agent); err != nil {
		return nil, err
	}

	if created {
		session := agent.ActiveSession()
		if session != nil {
			if err := sm.cmux.RenameSurface(agent.workspaceID(), session.SurfaceID, defaultSessionName); err != nil {
				log.Printf("[session] failed to rename default surface for agent %s: %v", agent.Name, err)
			} else if err := sm.syncAgentSessions(agent); err != nil {
				return nil, err
			}
		}
	}

	return agent, nil
}

func (sm *SessionManager) ensureAgentWorkspace(userID string, agent *Agent) (bool, error) {
	if workspaceID := agent.workspaceID(); workspaceID != "" {
		return false, nil
	}

	created := false
	agent.workspaceOnce.Do(func() {
		title := workspaceTitle(userID, agent.Name)
		if workspaceID, err := sm.findWorkspaceIDByTitle(title); err == nil && workspaceID != "" {
			agent.setWorkspaceID(workspaceID)
			return
		}

		ws, err := sm.cmux.CreateWorkspace(title)
		if err != nil {
			agent.workspaceErr = fmt.Errorf("create workspace for agent %q: %w", agent.Name, err)
			return
		}
		created = true
		agent.setWorkspaceID(ws.ID)
	})
	if agent.workspaceErr != nil {
		return false, agent.workspaceErr
	}
	return created, nil
}

func (sm *SessionManager) findWorkspaceIDByTitle(title string) (string, error) {
	workspaces, err := sm.cmux.ListWorkspaces()
	if err != nil {
		return "", err
	}
	for _, workspace := range workspaces {
		if strings.TrimSpace(workspace.Title) == title {
			return workspace.ID, nil
		}
	}
	return "", nil
}

func (sm *SessionManager) syncAgentSessions(agent *Agent) error {
	workspaceID := agent.workspaceID()
	if workspaceID == "" {
		return fmt.Errorf("agent %q has no workspace", agent.Name)
	}

	surfaces, err := sm.cmux.ListSurfaces(workspaceID)
	if err != nil {
		return fmt.Errorf("list surfaces for agent %q: %w", agent.Name, err)
	}

	agent.mu.Lock()
	defer agent.mu.Unlock()

	existing := agent.Sessions
	if existing == nil {
		existing = make(map[string]*Session)
	}

	sessions := make(map[string]*Session, len(surfaces))
	order := make([]string, 0, len(surfaces))
	activeSID := agent.ActiveSID

	for index, surface := range surfaces {
		session, ok := existing[surface.ID]
		if !ok {
			session = &Session{
				ID:        surface.ID,
				SurfaceID: surface.ID,
				AgentType: agent.defaultType(),
			}
		}

		session.Name = sessionNameForSurface(surface, index)
		session.SurfaceType = surface.Type
		if session.AgentType == "" {
			session.AgentType = agent.defaultType()
		}
		sessions[session.ID] = session
		order = append(order, session.ID)

		if surface.Focused {
			activeSID = session.ID
		}
	}

	for id, session := range existing {
		if _, ok := sessions[id]; !ok {
			session.stopWatching()
		}
	}

	if activeSID == "" || sessions[activeSID] == nil {
		if len(order) > 0 {
			activeSID = order[0]
		}
	}

	agent.Sessions = sessions
	agent.SessionOrder = order
	agent.ActiveSID = activeSID
	return nil
}

func (sm *SessionManager) handleNewSession(agent *Agent, msg channels.InboundMessage, raw string) {
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	name, provider := extractProviderOption(raw)
	if name == "" {
		name = sm.nextSessionName(agent, "session")
	}
	if _, ok := agent.SessionByName(name); ok {
		sm.reply(msg, "Session already exists: "+name)
		return
	}

	surface, err := sm.cmux.CreateSurface(agent.workspaceID())
	if err != nil {
		sm.reply(msg, "Error creating session: "+err.Error())
		return
	}
	if err := sm.cmux.RenameSurface(agent.workspaceID(), surface.ID, name); err != nil {
		log.Printf("[session] failed to rename surface %s to %q: %v", surface.ID, name, err)
	}
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Error syncing sessions: "+err.Error())
		return
	}
	agent.setActiveSession(surface.ID)
	if session := agent.ActiveSession(); session != nil {
		session.setAgentType(effectiveAgentType(provider, agent.defaultType()))
	}
	sm.reply(msg, fmt.Sprintf("New session [%s] created (%s)", name, agent.ActiveSession().agentType()))
}

func (sm *SessionManager) handleListSessions(agent *Agent, msg channels.InboundMessage) {
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	sessions := agent.ListSessions()
	if len(sessions) == 0 {
		sm.reply(msg, "No sessions in the current agent")
		return
	}

	active := agent.activeSessionID()
	var lines []string
	for _, session := range sessions {
		prefix := "  "
		if session.ID == active {
			prefix = "▶ "
		}
		suffix := fmt.Sprintf(" [%s]", session.agentType())
		if session.SurfaceType != "" {
			suffix += " / " + session.SurfaceType
		}
		lines = append(lines, prefix+session.Name+suffix)
	}
	sm.reply(msg, "Sessions:\n"+strings.Join(lines, "\n"))
}

func (sm *SessionManager) handleSwitchSession(agent *Agent, msg channels.InboundMessage, requestedName string) {
	name := sanitizeName(requestedName)
	if name == "" {
		sm.reply(msg, "Usage: /switch <name>")
		return
	}
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	session, ok := agent.SessionByName(name)
	if !ok {
		sm.reply(msg, "Session not found: "+name)
		return
	}
	if err := sm.cmux.FocusSurface(agent.workspaceID(), session.SurfaceID); err != nil {
		sm.reply(msg, "Error switching session: "+err.Error())
		return
	}
	agent.setActiveSession(session.ID)
	sm.reply(msg, "Switched to session: "+session.Name)
}

func (sm *SessionManager) handleCloseSession(agent *Agent, msg channels.InboundMessage) {
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session to close")
		return
	}

	if session.isRunningTurn() {
		_ = sm.runner.StopTurn(session.ID)
	}
	session.stopWatching()
	if err := sm.cmux.CloseSurface(agent.workspaceID(), session.SurfaceID); err != nil {
		sm.reply(msg, "Error closing session: "+err.Error())
		return
	}
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Session closed, but refresh failed: "+err.Error())
		return
	}

	reply := "Closed session: " + session.Name
	if next := agent.ActiveSession(); next != nil {
		reply += "\nActive session: " + next.Name
	}
	sm.reply(msg, reply)
}

func (sm *SessionManager) handleSessionCommand(agent *Agent, msg channels.InboundMessage, raw string) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		sm.reply(msg, "Usage: /session <new|list|switch|close> ...")
		return
	}

	switch fields[0] {
	case "new":
		sm.handleNewSession(agent, msg, strings.TrimSpace(strings.TrimPrefix(raw, fields[0])))
	case "list":
		sm.handleListSessions(agent, msg)
	case "switch":
		sm.handleSwitchSession(agent, msg, strings.TrimSpace(strings.TrimPrefix(raw, fields[0])))
	case "close":
		sm.handleCloseSession(agent, msg)
	default:
		sm.reply(msg, "Unknown session command: "+fields[0])
	}
}

func (sm *SessionManager) handleResetSession(agent *Agent, msg channels.InboundMessage) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session to reset")
		return
	}
	if session.isRunningTurn() {
		_ = sm.runner.StopTurn(session.ID)
	}
	session.setProviderSessionID("")
	session.setPendingApproval("")
	session.setLastUsage(UsageSnapshot{})
	sm.reply(msg, "Reset session context: "+session.Name)
}

func (sm *SessionManager) handleModelCommand(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}

	if len(args) == 0 || args[0] == "status" {
		model := session.model()
		if model == "" {
			model = "(provider default)"
		}
		sm.reply(msg, fmt.Sprintf("Provider: %s\nModel: %s", session.agentType(), model))
		return
	}
	if args[0] == "list" {
		sm.reply(msg, "Model selection is provider-defined. Use /model <name> to set the raw model string for the current session.")
		return
	}

	session.setModel(strings.Join(args, " "))
	sm.reply(msg, "Model set to: "+session.model())
}

func (sm *SessionManager) handleThinkCommand(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}
	if len(args) == 0 {
		value := session.effort()
		if value == "" {
			value = "off"
		}
		sm.reply(msg, "Thinking effort: "+value)
		return
	}

	value := strings.ToLower(args[0])
	switch value {
	case "off":
		session.setEffort("")
	case "low", "medium", "high", "max":
		session.setEffort(value)
	default:
		sm.reply(msg, "Usage: /think off|low|medium|high|max")
		return
	}
	sm.reply(msg, "Thinking effort set to: "+value)
}

func (sm *SessionManager) handleFastCommand(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}
	if len(args) == 0 {
		model := session.model()
		if model == "" {
			model = "(provider default)"
		}
		sm.reply(msg, fmt.Sprintf("Fast mode: %t\nModel: %s", session.fast(), model))
		return
	}

	switch strings.ToLower(args[0]) {
	case "on":
		func() {
			session.mu.Lock()
			defer session.mu.Unlock()
			if !session.Fast {
				session.PreviousModel = session.Model
				session.Model = "sonnet"
				session.Fast = true
			}
		}()
		sm.reply(msg, "Fast mode on — model set to: sonnet")
	case "off":
		func() {
			session.mu.Lock()
			defer session.mu.Unlock()
			if session.Fast {
				session.Model = session.PreviousModel
				session.PreviousModel = ""
				session.Fast = false
			}
		}()
		model := session.model()
		if model == "" {
			model = "(provider default)"
		}
		sm.reply(msg, "Fast mode off — model restored to: "+model)
	default:
		sm.reply(msg, "Usage: /fast on|off")
		return
	}
}

func (sm *SessionManager) handleVerboseCommand(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}
	if len(args) == 0 {
		sm.reply(msg, fmt.Sprintf("Verbose mode: %t", session.verbose()))
		return
	}

	switch strings.ToLower(args[0]) {
	case "on":
		session.setVerbose(true)
	case "off":
		session.setVerbose(false)
	default:
		sm.reply(msg, "Usage: /verbose on|off")
		return
	}
	sm.reply(msg, fmt.Sprintf("Verbose mode set to: %t", session.verbose()))
}

func (sm *SessionManager) handleUsageCommand(agent *Agent, msg channels.InboundMessage) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}

	usage := session.lastUsage()
	sm.reply(msg, fmt.Sprintf(
		"Provider: %s\nInput tokens: %d\nCached input tokens: %d\nOutput tokens: %d",
		usage.Provider,
		usage.InputTokens,
		usage.CachedInputTokens,
		usage.OutputTokens,
	))
}

func (sm *SessionManager) handleContextCommand(agent *Agent, msg channels.InboundMessage) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}

	model := session.model()
	if model == "" {
		model = "(provider default)"
	}
	contextID := session.providerSessionID()
	if contextID == "" {
		contextID = "(new session)"
	}
	sm.reply(msg, fmt.Sprintf(
		"Session: %s\nProvider: %s\nModel: %s\nContext ID: %s\nVerbose: %t\nFast: %t\nThinking: %s",
		session.Name,
		session.agentType(),
		model,
		contextID,
		session.verbose(),
		session.fast(),
		defaultString(session.effort(), "off"),
	))
}

func (sm *SessionManager) handleAIInput(agent *Agent, session *Session, msg channels.InboundMessage, prompt string) {
	if session.SurfaceType != "" && session.SurfaceType != "terminal" {
		sm.reply(msg, "The active session is not a terminal. Switch to a terminal session before sending AI prompts.")
		return
	}
	if !session.startTurn() {
		sm.reply(msg, "A turn is already running in this session. Use /stop to interrupt it.")
		return
	}
	defer session.finishTurn()

	session.setPendingApproval("")
	sm.appendTranscript(agent, session, "User", prompt)

	turn, err := sm.runner.StartTurn(sm.ctx, session, prompt)
	if err != nil {
		sm.reply(msg, "Error starting AI turn: "+err.Error())
		return
	}

	presenter := NewIMPresenter(sm.channel, msg.ChannelName, msg.ChatID, session.verbose())
	var assistant strings.Builder
	sawDelta := false

	for event := range turn.Events {
		switch event.Type {
		case StreamEventText:
			if event.Delta {
				sawDelta = true
				assistant.WriteString(event.Content)
			} else if !sawDelta && strings.TrimSpace(event.Content) != "" {
				assistant.Reset()
				assistant.WriteString(event.Content)
			}
		case StreamEventResult:
			session.setLastUsage(usageFromEvent(event))
			if assistant.Len() == 0 && strings.TrimSpace(event.Content) != "" {
				assistant.WriteString(event.Content)
			}
		case StreamEventControlRequest:
			session.setPendingApproval(event.Content)
			perm := session.permissionMode()
			if perm == "" {
				perm = sm.runner.cfg.ClaudePermissionMode
			}
			if perm == "" {
				perm = "(not set)"
			}
			sm.reply(msg, fmt.Sprintf(
				"Tool approval requested: %s\n"+
					"Current permission mode: %s\n"+
					"Use /permission <mode> to change how approvals are handled on the next turn.",
				event.Content, perm,
			))
		}
		presenter.HandleEvent(event)
	}
	presenter.Close()

	waitErr := <-turn.Done
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		log.Printf("[session] AI turn failed for %s/%s: %v", agent.Name, session.Name, waitErr)
	}

	if text := strings.TrimSpace(assistant.String()); text != "" {
		sm.appendTranscript(agent, session, "Assistant", text)
	}
}

func (sm *SessionManager) handleBash(agent *Agent, msg channels.InboundMessage, command string) {
	command = strings.TrimSpace(command)
	if command == "" {
		sm.reply(msg, "Usage: /bash <cmd> or ! <cmd>")
		return
	}

	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}
	if session.SurfaceType != "" && session.SurfaceType != "terminal" {
		sm.reply(msg, "The active session is not a terminal. Switch to a terminal session before sending shell commands.")
		return
	}

	session.stopWatching()
	baseline, _ := sm.cmux.ReadText(agent.workspaceID(), session.SurfaceID)
	session.setLastOutput(baseline)

	ctx, cancel := context.WithCancel(sm.ctx)
	session.setWatchCancel(cancel)
	go sm.watchOutput(ctx, agent.workspaceID(), session, msg.ChannelName, msg.ChatID)

	if err := sm.cmux.SendText(agent.workspaceID(), session.SurfaceID, command+"\n"); err != nil {
		session.stopWatching()
		sm.reply(msg, "Error sending to terminal: "+err.Error())
		return
	}
}

func (sm *SessionManager) handleStop(agent *Agent, msg channels.InboundMessage) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}

	stopped := false
	if session.isRunningTurn() {
		if err := sm.runner.StopTurn(session.ID); err == nil {
			stopped = true
		}
	}
	if session.isWatching() || session.SurfaceType == "terminal" {
		session.stopWatching()
		_ = sm.cmux.SendKey(agent.workspaceID(), session.SurfaceID, "ctrl+c")
		stopped = true
	}

	if stopped {
		sm.reply(msg, "Stop signal sent.")
		return
	}
	sm.reply(msg, "Nothing is currently running.")
}

func (sm *SessionManager) handleApprove(agent *Agent, msg channels.InboundMessage) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}
	pending := strings.TrimSpace(session.pendingApproval())
	if pending == "" {
		sm.reply(msg, "No pending approval request.")
		return
	}

	perm := session.permissionMode()
	if perm == "" {
		perm = sm.runner.cfg.ClaudePermissionMode
	}
	if perm == "" {
		perm = "(not set)"
	}

	sm.reply(msg, fmt.Sprintf(
		"Claude Code's -p mode does not support interactive approval via stdin.\n"+
			"When a tool requires approval, it is handled by the --permission-mode setting.\n\n"+
			"Current permission mode: %s\n"+
			"Pending request was: %s\n\n"+
			"Use /permission <mode> to change the permission mode for the next turn.\n"+
			"Valid modes: default, plan, bypassPermissions, auto",
		perm, pending,
	))
}

func (sm *SessionManager) handlePermissionCommand(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}

	if len(args) == 0 {
		perm := session.permissionMode()
		if perm == "" {
			perm = sm.runner.cfg.ClaudePermissionMode
		}
		if perm == "" {
			perm = "(not set — using provider default)"
		}
		sm.reply(msg, "Permission mode: "+perm+"\nUsage: /permission <default|plan|bypassPermissions|auto>")
		return
	}

	mode := strings.TrimSpace(args[0])
	switch mode {
	case "default", "plan", "bypassPermissions", "auto":
		session.setPermissionMode(mode)
		sm.reply(msg, "Permission mode set to: "+mode)
	default:
		sm.reply(msg, "Invalid permission mode: "+mode+"\nValid modes: default, plan, bypassPermissions, auto")
	}
}

func (sm *SessionManager) handleScreenshot(agent *Agent, msg channels.InboundMessage) {
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session to capture")
		return
	}

	if session.SurfaceType == "browser" {
		shot, err := sm.cmux.BrowserScreenshot(agent.workspaceID(), session.SurfaceID)
		if err != nil {
			sm.reply(msg, "Error taking browser screenshot: "+err.Error())
			return
		}

		data, err := decodeOrReadScreenshot(shot.PNGBase64, shot.Path)
		if err != nil {
			sm.reply(msg, "Error reading browser screenshot: "+err.Error())
			return
		}

		sm.send(msg.ChannelName, msg.ChatID, channels.OutboundMessage{
			Text: fmt.Sprintf("Browser screenshot: %s", session.Name),
			Attachments: []channels.Attachment{{
				Type:     "image",
				Data:     data,
				Filename: screenshotFilename(session.Name),
				MimeType: "image/png",
			}},
		})
		return
	}

	shot, err := sm.cmux.DebugPanelSnapshot(session.SurfaceID, screenshotLabel(session.Name))
	if err == nil {
		data, readErr := os.ReadFile(shot.Path)
		if readErr != nil {
			sm.reply(msg, "Error reading terminal screenshot: "+readErr.Error())
			return
		}

		sm.send(msg.ChannelName, msg.ChatID, channels.OutboundMessage{
			Text: fmt.Sprintf("Terminal screenshot: %s", session.Name),
			Attachments: []channels.Attachment{{
				Type:     "image",
				Data:     data,
				Filename: screenshotFilename(session.Name),
				MimeType: "image/png",
			}},
		})
		return
	}

	text, readErr := sm.cmux.ReadText(agent.workspaceID(), session.SurfaceID)
	if readErr != nil {
		sm.reply(msg, "Screenshot unavailable on this build: "+err.Error())
		return
	}

	fallback := cleanTerminalOutput(text)
	if len(fallback) > maxFallbackPreview {
		fallback = fallback[:maxFallbackPreview] + "\n..."
	}
	sm.send(msg.ChannelName, msg.ChatID, channels.OutboundMessage{
		Text: "Terminal screenshot unavailable on this build. Current text snapshot:\n" + fallback,
	})
}

func (sm *SessionManager) handleBrowse(agent *Agent, msg channels.InboundMessage, rawURL string) {
	url := strings.TrimSpace(rawURL)
	if url == "" {
		sm.reply(msg, "Usage: /browse <url>")
		return
	}

	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}

	if session.SurfaceType == "browser" {
		if err := sm.cmux.NavigateBrowser(session.SurfaceID, url); err != nil {
			sm.reply(msg, "Error navigating browser: "+err.Error())
			return
		}
		sm.reply(msg, "Navigated browser: "+url)
		return
	}

	surface, err := sm.cmux.OpenBrowserSplit(agent.workspaceID(), session.SurfaceID, url)
	if err != nil {
		sm.reply(msg, "Error opening browser split: "+err.Error())
		return
	}
	name := sm.nextSessionName(agent, "browser")
	if err := sm.cmux.RenameSurface(agent.workspaceID(), surface.ID, name); err != nil {
		log.Printf("[session] failed to rename browser surface %s to %q: %v", surface.ID, name, err)
	}
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Browser opened, but refresh failed: "+err.Error())
		return
	}
	agent.setActiveSession(surface.ID)
	sm.reply(msg, fmt.Sprintf("Browser opened [%s]: %s", name, url))
}

func (sm *SessionManager) handleSplit(agent *Agent, msg channels.InboundMessage, rawArgs string) {
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session to split")
		return
	}

	direction, name := parseSplitArgs(rawArgs)
	if name == "" {
		name = sm.nextSessionName(agent, "split")
	}

	surface, err := sm.cmux.SplitSurface(agent.workspaceID(), session.SurfaceID, direction)
	if err != nil {
		sm.reply(msg, "Error splitting session: "+err.Error())
		return
	}
	if err := sm.cmux.RenameSurface(agent.workspaceID(), surface.ID, name); err != nil {
		log.Printf("[session] failed to rename split surface %s to %q: %v", surface.ID, name, err)
	}
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Split created, but refresh failed: "+err.Error())
		return
	}
	agent.setActiveSession(surface.ID)
	sm.reply(msg, fmt.Sprintf("Split created [%s] (%s)", name, direction))
}

func (sm *SessionManager) handlePanes(agent *Agent, msg channels.InboundMessage) {
	panes, err := sm.cmux.ListPanes(agent.workspaceID())
	if err != nil {
		sm.reply(msg, "Error listing panes: "+err.Error())
		return
	}
	if len(panes) == 0 {
		sm.reply(msg, "No panes")
		return
	}

	lines := make([]string, 0, len(panes))
	for _, pane := range panes {
		prefix := "  "
		if pane.Focused {
			prefix = "▶ "
		}
		lines = append(lines, fmt.Sprintf("%s%s (%d surfaces)", prefix, defaultString(pane.Ref, pane.ID), pane.SurfaceCount))
	}
	sm.reply(msg, "Panes:\n"+strings.Join(lines, "\n"))
}

func (sm *SessionManager) handleTree(agent *Agent, msg channels.InboundMessage) {
	payload, err := sm.cmux.SystemTree(agent.workspaceID())
	if err != nil {
		sm.reply(msg, "Error fetching tree: "+err.Error())
		return
	}
	sm.reply(msg, prettyJSON(payload))
}

func (sm *SessionManager) handleRead(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}

	lines := 0
	if len(args) > 0 {
		value, err := strconv.Atoi(args[0])
		if err != nil || value <= 0 {
			sm.reply(msg, "Usage: /read [lines]")
			return
		}
		lines = value
	}
	text, err := sm.cmux.ReadTextLines(agent.workspaceID(), session.SurfaceID, lines)
	if err != nil {
		sm.reply(msg, "Error reading terminal text: "+err.Error())
		return
	}
	sm.reply(msg, cleanTerminalOutput(text))
}

func (sm *SessionManager) handleRaw(agent *Agent, msg channels.InboundMessage, raw string) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		sm.reply(msg, "Usage: /raw <method> [json]")
		return
	}
	method := fields[0]
	paramText := strings.TrimSpace(strings.TrimPrefix(raw, method))

	var params interface{} = map[string]interface{}{}
	if paramText != "" {
		if err := json.Unmarshal([]byte(paramText), &params); err != nil {
			sm.reply(msg, "Invalid JSON params: "+err.Error())
			return
		}
	}
	result, err := sm.cmux.Call(method, params)
	if err != nil {
		sm.reply(msg, "cmux call failed: "+err.Error())
		return
	}
	sm.reply(msg, prettyJSON(result))
}

func (sm *SessionManager) handleAgentCommand(user *UserState, msg channels.InboundMessage, raw string) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		sm.reply(msg, "Usage: /agent <new|list|switch|close|status> ...")
		return
	}

	switch fields[0] {
	case "new":
		name, provider := extractProviderOption(strings.TrimSpace(strings.TrimPrefix(raw, fields[0])))
		if name == "" {
			sm.reply(msg, "Usage: /agent new <name> [--model claude|codex|shell]")
			return
		}
		if _, ok := user.getAgent(name); ok {
			sm.reply(msg, "Agent already exists: "+name)
			return
		}

		agent := user.ensureAgent(name, effectiveAgentType(provider, sm.defaultAgentType))
		created, err := sm.ensureAgentWorkspace(user.ID, agent)
		if err != nil {
			sm.reply(msg, "Error creating agent: "+err.Error())
			return
		}
		if err := sm.syncAgentSessions(agent); err != nil {
			sm.reply(msg, "Error preparing agent: "+err.Error())
			return
		}
		if created {
			if session := agent.ActiveSession(); session != nil {
				if err := sm.cmux.RenameSurface(agent.workspaceID(), session.SurfaceID, defaultSessionName); err != nil {
					log.Printf("[session] failed to rename default session for new agent %s: %v", name, err)
				} else {
					_ = sm.syncAgentSessions(agent)
				}
			}
		}
		if err := sm.cmux.SelectWorkspace(agent.workspaceID()); err != nil {
			log.Printf("[session] failed to select workspace for agent %s: %v", name, err)
		}
		sm.reply(msg, fmt.Sprintf("New agent created: %s (%s)", name, agent.defaultType()))

	case "list", "status":
		agents := user.listAgents()
		if len(agents) == 0 {
			sm.reply(msg, "No agents yet. Use /agent new <name> or send a message to create the default agent.")
			return
		}

		active := user.activeAgentKey()
		lines := make([]string, 0, len(agents))
		for _, agent := range agents {
			if agent.workspaceID() != "" {
				if err := sm.syncAgentSessions(agent); err != nil {
					log.Printf("[session] failed to sync agent %s for listing: %v", agent.Name, err)
				}
			}
			prefix := "  "
			if normalizeKey(agent.Name) == active {
				prefix = "▶ "
			}
			lines = append(lines, fmt.Sprintf("%s%s (%d sessions, default=%s)", prefix, agent.Name, len(agent.ListSessions()), agent.defaultType()))
		}
		sm.reply(msg, "Agents:\n"+strings.Join(lines, "\n"))

	case "switch":
		name := sanitizeName(strings.TrimSpace(strings.TrimPrefix(raw, fields[0])))
		if name == "" {
			sm.reply(msg, "Usage: /agent switch <name>")
			return
		}

		agent, ok := user.switchAgent(name)
		if !ok {
			sm.reply(msg, "Agent not found: "+name)
			return
		}
		if _, err := sm.ensureAgentWorkspace(user.ID, agent); err != nil {
			sm.reply(msg, "Error loading agent: "+err.Error())
			return
		}
		if err := sm.syncAgentSessions(agent); err != nil {
			sm.reply(msg, "Error syncing agent: "+err.Error())
			return
		}
		if err := sm.cmux.SelectWorkspace(agent.workspaceID()); err != nil {
			sm.reply(msg, "Agent switched, but workspace selection failed: "+err.Error())
			return
		}
		if session := agent.ActiveSession(); session != nil {
			if err := sm.cmux.FocusSurface(agent.workspaceID(), session.SurfaceID); err != nil {
				log.Printf("[session] failed to focus active session for agent %s: %v", name, err)
			}
		}
		sm.reply(msg, "Switched to agent: "+agent.Name)

	case "close":
		name := sanitizeName(strings.TrimSpace(strings.TrimPrefix(raw, fields[0])))
		if name == "" {
			name = defaultAgentName
		}
		if strings.EqualFold(name, "all") {
			for _, agent := range user.listAgents() {
				sm.teardownAgent(agent)
				user.deleteAgent(agent.Name)
			}
			sm.reply(msg, "Closed all agent mappings.")
			return
		}

		agent, ok := user.getAgent(name)
		if !ok {
			sm.reply(msg, "Agent not found: "+name)
			return
		}
		sm.teardownAgent(agent)
		next := user.deleteAgent(name)
		if next != nil {
			_, _ = sm.ensureActiveAgent(user)
		}
		sm.reply(msg, "Closed agent: "+name)

	default:
		sm.reply(msg, "Unknown agent command: "+fields[0])
	}
}

func (sm *SessionManager) handleStatus(agent *Agent, msg channels.InboundMessage) {
	if err := sm.syncAgentSessions(agent); err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, fmt.Sprintf("Agent: %s\nWorkspace: %s\nActive session: none", agent.Name, agent.workspaceID()))
		return
	}

	if err := sm.cmux.Ping(); err != nil {
		sm.reply(msg, "cmux connection error: "+err.Error())
		return
	}

	sm.reply(msg, fmt.Sprintf(
		"Agent: %s\nWorkspace: %s\nActive session: %s\nProvider: %s\nContext ID: %s\nVerbose: %t\nRunning: %t\nWatching shell: %t",
		agent.Name,
		agent.workspaceID(),
		session.Name,
		session.agentType(),
		defaultString(session.providerSessionID(), "(new)"),
		session.verbose(),
		session.isRunningTurn(),
		session.isWatching(),
	))
}

func (sm *SessionManager) handleHelp(msg channels.InboundMessage) {
	help := `cmux IM Bridge Commands:

/new [name] [--model claude|codex|shell]
/list
/switch <name>
/close
/session <new|list|switch|close> ...
/reset
/model [status|list|<name>]
/think [off|low|medium|high|max]
/fast [on|off]
/verbose [on|off]
/bash <cmd>
! <cmd>
/stop
/approve
/permission [default|plan|bypassPermissions|auto]
/status
/usage
/context
/whoami
/agent <new|list|switch|close|status> ...
/browse <url>
/split [left|right|up|down] [name]
/panes
/tree
/screenshot
/read [lines]
/raw <method> [json]

OpenClaw migration:
/bash <cmd>  -> /bash <cmd> or ! <cmd>
/stop        -> /stop
/acp spawn   -> /agent new <name>
/focus       -> /agent switch <name>
/reset       -> /reset
/model       -> /model
/think high  -> /think high

Plain text goes to the active AI session. In shell sessions, plain text runs in the terminal.`

	sm.reply(msg, help)
}

func (sm *SessionManager) teardownAgent(agent *Agent) {
	for _, session := range agent.ListSessions() {
		session.stopWatching()
		if session.isRunningTurn() {
			_ = sm.runner.StopTurn(session.ID)
		}
	}
}

func (sm *SessionManager) appendTranscript(agent *Agent, session *Session, role, text string) {
	if session == nil || session.SurfaceType != "terminal" || strings.TrimSpace(text) == "" {
		return
	}

	const delimiter = "__CMUX_IM_BRIDGE__"
	body := fmt.Sprintf("[%s]\n%s\n", role, text)
	command := fmt.Sprintf("cat <<'%s'\n%s%s\n", delimiter, body, delimiter)
	if err := sm.cmux.SendText(agent.workspaceID(), session.SurfaceID, command); err != nil {
		log.Printf("[session] failed to append transcript to surface %s: %v", session.SurfaceID, err)
	}
}

func (sm *SessionManager) send(channelName, chatID string, msg channels.OutboundMessage) {
	if err := sm.channel.Send(channelName, chatID, msg); err != nil {
		log.Printf("[session] failed to send outbound message: %v", err)
	}
}

func (sm *SessionManager) reply(msg channels.InboundMessage, text string) {
	sm.send(msg.ChannelName, msg.ChatID, channels.OutboundMessage{
		Text:   text,
		Format: "text",
	})
}

func (u *UserState) ensureAgent(name, defaultType string) *Agent {
	key := normalizeKey(name)

	u.mu.Lock()
	defer u.mu.Unlock()

	if agent, ok := u.Agents[key]; ok {
		u.ActiveAgentKey = key
		if defaultType != "" && agent.DefaultType == "" {
			agent.DefaultType = defaultType
		}
		return agent
	}

	agent := &Agent{
		Name:        name,
		DefaultType: effectiveAgentType(defaultType, AgentTypeClaude),
		Sessions:    make(map[string]*Session),
	}
	u.Agents[key] = agent
	u.AgentOrder = append(u.AgentOrder, key)
	u.ActiveAgentKey = key
	return agent
}

func (u *UserState) getAgent(name string) (*Agent, bool) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	agent, ok := u.Agents[normalizeKey(name)]
	return agent, ok
}

func (u *UserState) activeAgent() *Agent {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if u.ActiveAgentKey == "" {
		return nil
	}
	return u.Agents[u.ActiveAgentKey]
}

func (u *UserState) activeAgentKey() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.ActiveAgentKey
}

func (u *UserState) switchAgent(name string) (*Agent, bool) {
	key := normalizeKey(name)

	u.mu.Lock()
	defer u.mu.Unlock()

	agent, ok := u.Agents[key]
	if !ok {
		return nil, false
	}
	u.ActiveAgentKey = key
	return agent, true
}

func (u *UserState) deleteAgent(name string) *Agent {
	key := normalizeKey(name)

	u.mu.Lock()
	defer u.mu.Unlock()

	delete(u.Agents, key)
	filtered := u.AgentOrder[:0]
	for _, existing := range u.AgentOrder {
		if existing != key {
			filtered = append(filtered, existing)
		}
	}
	u.AgentOrder = filtered
	if u.ActiveAgentKey == key {
		u.ActiveAgentKey = ""
		if len(u.AgentOrder) > 0 {
			u.ActiveAgentKey = u.AgentOrder[0]
		}
	}
	if u.ActiveAgentKey == "" {
		return nil
	}
	return u.Agents[u.ActiveAgentKey]
}

func (u *UserState) listAgents() []*Agent {
	u.mu.RLock()
	defer u.mu.RUnlock()

	agents := make([]*Agent, 0, len(u.AgentOrder))
	for _, key := range u.AgentOrder {
		if agent, ok := u.Agents[key]; ok {
			agents = append(agents, agent)
		}
	}
	return agents
}

// ActiveSession returns the currently active session.
func (a *Agent) ActiveSession() *Session {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Sessions[a.ActiveSID]
}

func (a *Agent) activeSessionID() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.ActiveSID
}

func (a *Agent) setActiveSession(sessionID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.Sessions[sessionID] != nil {
		a.ActiveSID = sessionID
	}
}

func (a *Agent) workspaceID() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.WorkspaceID
}

func (a *Agent) setWorkspaceID(workspaceID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.WorkspaceID = workspaceID
}

func (a *Agent) defaultType() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return effectiveAgentType(a.DefaultType, AgentTypeClaude)
}

// SessionByName finds a session by its display name.
func (a *Agent) SessionByName(name string) (*Session, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := normalizeKey(name)
	for _, id := range a.SessionOrder {
		session := a.Sessions[id]
		if session != nil && normalizeKey(session.Name) == key {
			return session, true
		}
	}
	return nil, false
}

// ListSessions returns sessions in display order.
func (a *Agent) ListSessions() []*Session {
	a.mu.RLock()
	defer a.mu.RUnlock()

	sessions := make([]*Session, 0, len(a.SessionOrder))
	for _, id := range a.SessionOrder {
		if session, ok := a.Sessions[id]; ok {
			sessions = append(sessions, session)
		}
	}
	return sessions
}

func (s *Session) setWatchCancel(cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.watchCancel != nil {
		s.watchCancel()
	}
	s.watchCancel = cancel
}

func (s *Session) stopWatching() {
	s.mu.Lock()
	cancel := s.watchCancel
	s.watchCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Session) isWatching() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watchCancel != nil
}

func (s *Session) lastOutput() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastOutput
}

func (s *Session) setLastOutput(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastOutput = text
}

func (s *Session) startTurn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runningTurn {
		return false
	}
	s.runningTurn = true
	return true
}

func (s *Session) finishTurn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runningTurn = false
}

func (s *Session) isRunningTurn() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.runningTurn
}

func (s *Session) agentType() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return effectiveAgentType(s.AgentType, AgentTypeClaude)
}

func (s *Session) setAgentType(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AgentType = effectiveAgentType(value, AgentTypeClaude)
}

func (s *Session) model() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Model
}

func (s *Session) setModel(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Model = strings.TrimSpace(value)
}

func (s *Session) effort() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Effort
}

func (s *Session) setEffort(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Effort = strings.TrimSpace(value)
}

func (s *Session) fast() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Fast
}

func (s *Session) setFast(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Fast = value
}

func (s *Session) verbose() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Verbose
}

func (s *Session) setVerbose(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Verbose = value
}

func (s *Session) providerSessionID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ProviderSessionID
}

func (s *Session) setProviderSessionID(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ProviderSessionID = strings.TrimSpace(value)
}

func (s *Session) permissionMode() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.PermissionMode
}

func (s *Session) setPermissionMode(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PermissionMode = strings.TrimSpace(value)
}

func (s *Session) pendingApproval() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.PendingApproval
}

func (s *Session) setPendingApproval(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PendingApproval = strings.TrimSpace(value)
}

func (s *Session) lastUsage() UsageSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastUsage
}

func (s *Session) setLastUsage(value UsageSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastUsage = value
}

func normalizeKey(name string) string {
	return strings.ToLower(sanitizeName(name))
}

func sanitizeName(name string) string {
	name = strings.TrimSpace(strings.ReplaceAll(name, "\n", " "))
	for strings.Contains(name, "  ") {
		name = strings.ReplaceAll(name, "  ", " ")
	}
	return name
}

func workspaceTitle(userID, agentName string) string {
	return fmt.Sprintf("IM %s / %s", userID, agentName)
}

func sessionNameForSurface(surface SurfaceInfo, index int) string {
	title := sanitizeName(surface.Title)
	if title != "" {
		return title
	}
	if index == 0 {
		return defaultSessionName
	}
	return fmt.Sprintf("session-%d", index+1)
}

func (sm *SessionManager) nextSessionName(agent *Agent, prefix string) string {
	base := sanitizeName(prefix)
	if base == "" {
		base = "session"
	}

	for i := 1; ; i++ {
		name := fmt.Sprintf("%s-%d", base, i)
		if _, ok := agent.SessionByName(name); !ok {
			return name
		}
	}
}

func parseSplitArgs(raw string) (direction string, name string) {
	fields := strings.Fields(raw)
	direction = "right"
	if len(fields) == 0 {
		return direction, ""
	}
	if isSplitDirection(fields[0]) {
		direction = fields[0]
		return direction, sanitizeName(strings.Join(fields[1:], " "))
	}
	return direction, sanitizeName(strings.Join(fields, " "))
}

func isSplitDirection(value string) bool {
	switch value {
	case "left", "right", "up", "down":
		return true
	default:
		return false
	}
}

func screenshotLabel(name string) string {
	label := normalizeKey(name)
	if label == "" {
		return "im-bridge"
	}
	return strings.ReplaceAll(label, " ", "-")
}

func screenshotFilename(name string) string {
	label := screenshotLabel(name)
	return label + ".png"
}

func decodeOrReadScreenshot(base64Data, path string) ([]byte, error) {
	if strings.TrimSpace(base64Data) != "" {
		data, err := base64.StdEncoding.DecodeString(base64Data)
		if err != nil {
			return nil, fmt.Errorf("decode png_base64: %w", err)
		}
		return data, nil
	}
	if path == "" {
		return nil, fmt.Errorf("screenshot payload was empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read screenshot file: %w", err)
	}
	return data, nil
}

func extractProviderOption(raw string) (name string, provider string) {
	fields := strings.Fields(raw)
	rest := make([]string, 0, len(fields))
	for i := 0; i < len(fields); i++ {
		if (fields[i] == "--model" || fields[i] == "--type" || fields[i] == "--agent") && i+1 < len(fields) {
			if normalized := normalizeAgentType(fields[i+1]); normalized != "" {
				provider = normalized
				i++
				continue
			}
		}
		rest = append(rest, fields[i])
	}
	return sanitizeName(strings.Join(rest, " ")), provider
}

func normalizeAgentType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case AgentTypeClaude:
		return AgentTypeClaude
	case AgentTypeCodex:
		return AgentTypeCodex
	case AgentTypeShell:
		return AgentTypeShell
	default:
		return ""
	}
}

func effectiveAgentType(value, fallback string) string {
	if normalized := normalizeAgentType(value); normalized != "" {
		return normalized
	}
	if normalized := normalizeAgentType(fallback); normalized != "" {
		return normalized
	}
	return AgentTypeClaude
}

func usageFromEvent(event StreamEvent) UsageSnapshot {
	usage := UsageSnapshot{Provider: event.Provider}
	switch event.Provider {
	case AgentTypeClaude:
		raw, _ := event.Meta["usage"].(map[string]interface{})
		usage.InputTokens = intFromMap(raw, "input_tokens")
		usage.CachedInputTokens = intFromMap(raw, "cache_read_input_tokens")
		usage.OutputTokens = intFromMap(raw, "output_tokens")
	case AgentTypeCodex:
		raw, _ := event.Meta["usage"].(map[string]interface{})
		usage.InputTokens = intFromMap(raw, "input_tokens")
		usage.CachedInputTokens = intFromMap(raw, "cached_input_tokens")
		usage.OutputTokens = intFromMap(raw, "output_tokens")
	}
	return usage
}

func intFromMap(values map[string]interface{}, key string) int {
	if values == nil {
		return 0
	}
	switch value := values[key].(type) {
	case float64:
		return int(value)
	case int:
		return value
	default:
		return 0
	}
}

func prettyJSON(raw []byte) string {
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return string(raw)
	}
	return out.String()
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
