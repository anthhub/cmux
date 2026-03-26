package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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
	defaultSessionName = "main"
	maxFallbackPreview = 3500
	singleOwnerUserID  = "__owner__"
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
	mu           sync.RWMutex

	workspaceEnsureMu sync.Mutex
}

// Session represents one IM conversation mapped to a cmux surface.
type Session struct {
	ID          string
	SurfaceID   string
	Name        string
	RouteKey    string
	AgentID     string
	WorkspaceID string
	SurfaceType string

	AgentType         string
	Model             string
	Effort            string
	Fast              bool
	Verbose           bool
	ProviderSessionID string
	PermissionMode    string
	Workdir           string
	ExtraArgs         []string
	PreviousModel     string
	PendingApproval   string
	LastUsage         UsageSnapshot
	LastOutput        string

	watchCancel context.CancelFunc
	runningTurn bool
	agentProc   *AgentProcess
	streamMu    sync.Mutex
	mu          sync.RWMutex
}

// SessionManager manages IM user -> agent -> session mappings.
type SessionManager struct {
	ctx              context.Context
	users            map[string]*UserState
	cmux             *CmuxClient
	channel          *channels.Manager
	state            *GatewayStateStore
	mediaDir         string
	aiCfg            config.AIConfig
	agentDefs        map[string]AgentDefinition
	defaultAgentID   string
	defaultAgentType string
	presenter        *IMPresenter
	persistMu        sync.Mutex
	mu               sync.RWMutex
}

// NewSessionManager creates a new session manager.
func NewSessionManager(ctx context.Context, cmux *CmuxClient, channel *channels.Manager, ai config.AIConfig) *SessionManager {
	cfg := &config.Config{AI: ai, DefaultAgent: ai.DefaultAgent}
	return NewSessionManagerWithConfig(ctx, cmux, channel, cfg)
}

// NewSessionManagerWithConfig creates a new session manager using the full gateway config.
func NewSessionManagerWithConfig(ctx context.Context, cmux *CmuxClient, channel *channels.Manager, cfg *config.Config) *SessionManager {
	if cfg == nil {
		cfg = &config.Config{}
	}

	defaultAgentID := normalizeKey(cfg.ResolvedDefaultAgent())
	if defaultAgentID == "" {
		defaultAgentID = AgentTypeClaude
	}
	agentDefs := resolveAgentDefinitions(cfg)
	defaultDef, ok := agentDefs[defaultAgentID]
	if !ok {
		defaultDef = agentDefs[AgentTypeClaude]
		defaultAgentID = defaultDef.ID
	}

	defaultType := normalizeAgentType(cfg.AI.DefaultAgent)
	log.Printf("[session] config default_agent=%q normalized=%q", cfg.ResolvedDefaultAgent(), defaultType)
	if defaultType == "" {
		defaultType = defaultDef.Provider
	}
	log.Printf("[session] using defaultAgent=%q defaultAgentType=%q", defaultAgentID, defaultType)

	var stateStore *GatewayStateStore
	statePath := strings.TrimSpace(cfg.Gateway.StatePath)
	if statePath != "" {
		var err error
		stateStore, err = OpenGatewayStateStore(statePath)
		if err != nil {
			log.Printf("[session] failed to open state store %q: %v", statePath, err)
		}
	}
	if stateStore != nil {
		if persisted, err := stateStore.DefaultAgent(); err == nil && normalizeKey(persisted) != "" {
			if persistedDef, ok := agentDefs[normalizeKey(persisted)]; ok {
				defaultAgentID = persistedDef.ID
				defaultType = persistedDef.Provider
			}
		}
	}

	return &SessionManager{
		ctx:              ctx,
		users:            make(map[string]*UserState),
		cmux:             cmux,
		channel:          channel,
		state:            stateStore,
		mediaDir:         strings.TrimSpace(cfg.Gateway.MediaDir),
		aiCfg:            cfg.AI,
		agentDefs:        agentDefs,
		defaultAgentID:   defaultAgentID,
		defaultAgentType: defaultType,
		presenter:        nil, // initialized per-turn in watchOutput
	}
}

// HandleMessage processes one inbound IM message.
func (sm *SessionManager) HandleMessage(msg channels.InboundMessage) {
	text := strings.TrimSpace(msg.Text)
	if text == "" && len(msg.Attachments) == 0 {
		return
	}

	user := sm.getOrCreateUser(singleOwnerUserID)
	isNewUser := user.activeAgent() == nil
	sm.selectDefaultAgent(user)
	agent, err := sm.ensureActiveAgent(user)
	if err != nil {
		log.Printf("[session] failed to ensure active agent for %s: %v", msg.UserID, err)
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	if isNewUser {
		sm.sendWelcome(msg)
	}

	if strings.HasPrefix(text, "!") {
		sm.handleBash(agent, msg, strings.TrimSpace(strings.TrimPrefix(text, "!")))
		return
	}

	if strings.HasPrefix(text, "/") {
		sm.handleCommand(user, agent, msg, text)
		return
	}

	if agent.Name != sm.defaultAgentID {
		if restored, ok := user.switchAgent(sm.defaultAgentID); ok {
			agent = restored
		}
	}

	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session. Use /new to create one.")
		return
	}

	if agent, session, err = sm.ensureMainSession(user, agent); err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	prompt, err := sm.prepareInboundPrompt(msg)
	if err != nil {
		sm.reply(msg, "Error preparing message: "+err.Error())
		return
	}

	switch session.agentType() {
	case AgentTypeShell:
		if len(msg.Attachments) > 0 {
			sm.reply(msg, "Attachments are currently supported only for AI sessions.")
			return
		}
		sm.handleBash(agent, msg, text)
	default:
		sm.handleAIInput(agent, session, msg, prompt)
	}
}

func (sm *SessionManager) handleCommand(user *UserState, agent *Agent, msg channels.InboundMessage, text string) {
	fields := strings.Fields(text)
	command := strings.TrimPrefix(fields[0], "/")
	args := fields[1:]

	switch command {
	case "cc":
		sm.handleOneShotAgentPrompt(user, msg, strings.TrimSpace(strings.TrimPrefix(text, "/cc")), AgentTypeClaude)
	case "cx":
		sm.handleOneShotAgentPrompt(user, msg, strings.TrimSpace(strings.TrimPrefix(text, "/cx")), AgentTypeCodex)
	case "claude", "codex", "shell":
		sm.handleDefaultAgentAlias(user, msg, command, args)
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
	case "deny", "reject":
		sm.handleDeny(agent, msg)
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

func (sm *SessionManager) handleDefaultAgentAlias(user *UserState, msg channels.InboundMessage, rawAgentID string, args []string) {
	agentID := normalizeAgentType(rawAgentID)
	if agentID == "" {
		sm.reply(msg, "Unknown agent: "+rawAgentID)
		return
	}
	if len(args) > 0 {
		sm.reply(msg, fmt.Sprintf("Usage: /%s", agentID))
		return
	}

	agent, err := sm.ensureNamedAgent(user, agentID)
	if err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}
	user.switchAgent(agent.Name)
	sm.setDefaultAgent(agent.Name)
	if workspaceID := agent.workspaceID(); workspaceID != "" {
		if err := sm.cmux.SelectWorkspace(workspaceID); err != nil {
			log.Printf("[session] failed to select workspace for %s: %v", agent.Name, err)
		}
	}
	sm.reply(msg, "Default agent set to: "+agent.Name)
}

func (sm *SessionManager) handleOneShotAgentPrompt(user *UserState, msg channels.InboundMessage, raw string, agentID string) {
	body := strings.TrimSpace(raw)
	if body == "" {
		sm.reply(msg, fmt.Sprintf("Usage: /%s <message>", map[string]string{
			AgentTypeClaude: "cc",
			AgentTypeCodex:  "cx",
			AgentTypeShell:  "shell",
		}[normalizeAgentType(agentID)]))
		return
	}

	agent, err := sm.ensureNamedAgent(user, agentID)
	if err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}
	_, session, err := sm.ensureMainSession(user, agent)
	if err != nil {
		sm.reply(msg, "Error: "+err.Error())
		return
	}

	promptMsg := msg
	promptMsg.Text = body
	prompt, err := sm.prepareInboundPrompt(promptMsg)
	if err != nil {
		sm.reply(msg, "Error preparing message: "+err.Error())
		return
	}
	switch session.agentType() {
	case AgentTypeShell:
		sm.handleBash(agent, msg, body)
	default:
		sm.handleAIInput(agent, session, msg, prompt)
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

func (sm *SessionManager) selectDefaultAgent(user *UserState) {
	if user == nil {
		return
	}
	def := sm.agentDefinition(sm.defaultAgentID)
	user.ensureAgent(def.ID, def.Provider)
}

func (sm *SessionManager) currentDefaultAgentID() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.defaultAgentID == "" {
		return AgentTypeClaude
	}
	return sm.defaultAgentID
}

func (sm *SessionManager) setDefaultAgent(agentID string) {
	agentID = normalizeKey(agentID)
	if agentID == "" {
		return
	}
	sm.mu.Lock()
	sm.defaultAgentID = agentID
	sm.defaultAgentType = sm.agentDefinition(agentID).Provider
	sm.mu.Unlock()
	if sm.state != nil {
		if err := sm.state.SetDefaultAgent(agentID); err != nil {
			log.Printf("[session] failed to persist default agent %q: %v", agentID, err)
		}
	}
}

func (sm *SessionManager) agentDefinition(agentID string) AgentDefinition {
	id := normalizeKey(agentID)
	if id == "" {
		id = AgentTypeClaude
	}
	if def, ok := sm.agentDefs[id]; ok {
		return def
	}
	return AgentDefinition{
		ID:       id,
		Provider: effectiveAgentType(id, AgentTypeClaude),
		Workdir:  sm.aiCfg.Workdir,
	}
}

func (sm *SessionManager) ensureNamedAgent(user *UserState, agentID string) (*Agent, error) {
	def := sm.agentDefinition(agentID)
	agent := user.ensureAgent(def.ID, def.Provider)

	created, err := sm.ensureAgentWorkspace(singleOwnerUserID, agent)
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
			session = agent.ActiveSession()
			if session != nil {
				sm.applyAgentDefaults(agent, session)
				if ShouldLaunchPersistentAgent(session) {
					if err := LaunchAgentInTerminal(sm.cmux, agent.workspaceID(), session.SurfaceID, session, sm.aiCfg); err != nil {
						log.Printf("[session] failed to launch agent in terminal for agent %s: %v", agent.Name, err)
					}
				}
			}
		}
	}

	if _, _, err := sm.ensureMainSession(user, agent); err != nil {
		return nil, err
	}
	return agent, nil
}

func (sm *SessionManager) ensureMainSession(user *UserState, agent *Agent) (*Agent, *Session, error) {
	if agent == nil {
		var err error
		agent, err = sm.ensureActiveAgent(user)
		if err != nil {
			return nil, nil, err
		}
	}
	if err := sm.syncAgentSessions(agent); err != nil {
		return nil, nil, err
	}

	if session, ok := agent.SessionByName(defaultSessionName); ok {
		agent.setActiveSession(session.ID)
		sm.applyAgentDefaults(agent, session)
		sm.persistSessionState(agent, session)
		return agent, session, nil
	}

	if session := agent.ActiveSession(); session != nil {
		sm.applyAgentDefaults(agent, session)
		sm.persistSessionState(agent, session)
		return agent, session, nil
	}

	surface, err := sm.cmux.CreateSurface(agent.workspaceID())
	if err != nil {
		return nil, nil, fmt.Errorf("create main session: %w", err)
	}
	if err := sm.cmux.RenameSurface(agent.workspaceID(), surface.ID, defaultSessionName); err != nil {
		log.Printf("[session] failed to rename main surface for agent %s: %v", agent.Name, err)
	}
	if err := sm.syncAgentSessions(agent); err != nil {
		return nil, nil, err
	}
	session, ok := agent.SessionByName(defaultSessionName)
	if !ok {
		session = agent.ActiveSession()
	}
	if session == nil {
		return nil, nil, fmt.Errorf("main session missing for agent %s", agent.Name)
	}
	agent.setActiveSession(session.ID)
	sm.applyAgentDefaults(agent, session)
	if ShouldLaunchPersistentAgent(session) {
		if err := LaunchAgentInTerminal(sm.cmux, agent.workspaceID(), session.SurfaceID, session, sm.aiCfg); err != nil {
			log.Printf("[session] failed to launch main session for agent %s: %v", agent.Name, err)
		}
	}
	sm.persistSessionState(agent, session)
	return agent, session, nil
}

func (sm *SessionManager) applyAgentDefaults(agent *Agent, session *Session) {
	if agent == nil || session == nil {
		return
	}
	def := sm.agentDefinition(agent.Name)
	session.RouteKey = sessionRouteKey(agent.Name, session.Name)
	if strings.TrimSpace(session.AgentType) == "" {
		session.setAgentType(def.Provider)
	}

	if strings.TrimSpace(session.Model) == "" && strings.TrimSpace(def.Model) != "" {
		session.setModel(def.Model)
	}
	if strings.TrimSpace(session.Workdir) == "" && strings.TrimSpace(def.Workdir) != "" {
		session.setWorkdir(def.Workdir)
	}
	if strings.TrimSpace(session.permissionMode()) == "" && strings.TrimSpace(def.PermissionMode) != "" {
		session.setPermissionMode(def.PermissionMode)
	}
	if len(session.extraArgs()) == 0 && len(def.Args) > 0 {
		session.setExtraArgs(def.Args)
	}

	if sm.state == nil {
		return
	}
	persisted, err := sm.state.LoadSession(session.RouteKey)
	if err != nil || persisted == nil {
		return
	}
	if session.providerSessionID() == "" && persisted.ProviderSessionID != "" {
		session.setProviderSessionID(persisted.ProviderSessionID)
	}
	if session.model() == "" && persisted.Model != "" {
		session.setModel(persisted.Model)
	}
	if session.permissionMode() == "" && persisted.PermissionMode != "" {
		session.setPermissionMode(persisted.PermissionMode)
	}
	if session.workdir() == "" && persisted.Workdir != "" {
		session.setWorkdir(persisted.Workdir)
	}
	if session.effort() == "" && persisted.Effort != "" {
		session.setEffort(persisted.Effort)
	}
	if !session.verbose() && persisted.Verbose {
		session.setVerbose(true)
	}
	if !session.fast() && persisted.Fast {
		session.setFast(true)
	}
}

func (sm *SessionManager) persistSessionState(agent *Agent, session *Session) {
	if sm.state == nil || agent == nil || session == nil {
		return
	}
	routeKey := session.RouteKey
	if routeKey == "" {
		routeKey = sessionRouteKey(agent.Name, session.Name)
	}
	sm.persistMu.Lock()
	defer sm.persistMu.Unlock()
	err := sm.state.SaveSession(PersistedSessionState{
		SessionKey:        routeKey,
		AgentID:           normalizeKey(agent.Name),
		WorkspaceID:       agent.workspaceID(),
		SurfaceID:         session.SurfaceID,
		Provider:          session.agentType(),
		ProviderSessionID: session.providerSessionID(),
		Model:             session.model(),
		PermissionMode:    session.permissionMode(),
		Workdir:           session.workdir(),
		Effort:            session.effort(),
		Verbose:           session.verbose(),
		Fast:              session.fast(),
	})
	if err != nil {
		log.Printf("[session] failed to persist session state %s: %v", routeKey, err)
	}
}

func (sm *SessionManager) persistSessionSnapshot(session *Session) {
	if sm.state == nil || session == nil || session.RouteKey == "" {
		return
	}
	sm.persistMu.Lock()
	defer sm.persistMu.Unlock()
	err := sm.state.SaveSession(PersistedSessionState{
		SessionKey:        session.RouteKey,
		AgentID:           normalizeKey(session.AgentID),
		WorkspaceID:       session.WorkspaceID,
		SurfaceID:         session.SurfaceID,
		Provider:          session.agentType(),
		ProviderSessionID: session.providerSessionID(),
		Model:             session.model(),
		PermissionMode:    session.permissionMode(),
		Workdir:           session.workdir(),
		Effort:            session.effort(),
		Verbose:           session.verbose(),
		Fast:              session.fast(),
	})
	if err != nil {
		log.Printf("[session] failed to persist session snapshot %s: %v", session.RouteKey, err)
	}
}

func (sm *SessionManager) prepareInboundPrompt(msg channels.InboundMessage) (string, error) {
	prepared, err := materializeAttachments(sm.mediaDir, msg.Attachments)
	if err != nil {
		return "", err
	}
	prompt := buildInboundPrompt(msg, prepared)
	if strings.TrimSpace(prompt) == "" {
		prompt = strings.TrimSpace(msg.Text)
	}
	return prompt, nil
}

func (sm *SessionManager) ensureActiveAgent(user *UserState) (*Agent, error) {
	agent := user.activeAgent()
	if agent == nil {
		return sm.ensureNamedAgent(user, sm.currentDefaultAgentID())
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
			if ShouldLaunchPersistentAgent(session) {
				if err := LaunchAgentInTerminal(sm.cmux, agent.workspaceID(), session.SurfaceID, session, sm.aiCfg); err != nil {
					log.Printf("[session] failed to launch agent in terminal for agent %s: %v", agent.Name, err)
				}
			}
		}
	}

	return agent, nil
}

func (sm *SessionManager) ensureAgentWorkspace(userID string, agent *Agent) (bool, error) {
	agent.workspaceEnsureMu.Lock()
	defer agent.workspaceEnsureMu.Unlock()

	title := workspaceTitle(userID, agent.Name)
	legacyTitle := legacyWorkspaceTitle(userID, agent.Name)
	listings, err := sm.cmux.ListAllWorkspaces()
	if err != nil {
		workspaces, fallbackErr := sm.cmux.ListWorkspaces()
		if fallbackErr != nil {
			return false, fmt.Errorf("list workspaces for agent %q: %w", agent.Name, err)
		}
		listings = make([]WorkspaceListing, 0, len(workspaces))
		for _, workspace := range workspaces {
			listings = append(listings, WorkspaceListing{Workspace: workspace})
		}
	}

	if workspaceID := agent.workspaceID(); workspaceID != "" {
		for _, listing := range listings {
			if listing.Workspace.ID == workspaceID {
				return false, nil
			}
		}
	}

	for _, listing := range listings {
		trimmed := strings.TrimSpace(listing.Workspace.Title)
		if trimmed == strings.TrimSpace(title) || trimmed == strings.TrimSpace(legacyTitle) {
			agent.setWorkspaceID(listing.Workspace.ID)
			return false, nil
		}
	}

	ws, err := sm.cmux.CreateWorkspace(title)
	if err != nil {
		return false, fmt.Errorf("create workspace for agent %q: %w", agent.Name, err)
	}
	agent.setWorkspaceID(ws.ID)
	return true, nil
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

	if agent.Sessions == nil {
		agent.Sessions = make(map[string]*Session)
	}
	defaultType := effectiveAgentType(agent.DefaultType, AgentTypeClaude)

	// Track which surface IDs are still present
	seen := make(map[string]bool, len(surfaces))
	order := make([]string, 0, len(surfaces))
	activeSID := agent.ActiveSID

	for index, surface := range surfaces {
		seen[surface.ID] = true
		session, ok := agent.Sessions[surface.ID]
		if !ok {
			session = &Session{
				ID:        surface.ID,
				SurfaceID: surface.ID,
				AgentType: defaultType,
			}
			agent.Sessions[surface.ID] = session
		}

		session.mu.Lock()
		session.Name = sessionNameForSurface(surface, index)
		session.SurfaceType = surface.Type
		session.AgentID = normalizeKey(agent.Name)
		session.WorkspaceID = workspaceID
		if session.AgentType == "" {
			session.AgentType = defaultType
		}
		session.RouteKey = sessionRouteKey(agent.Name, session.Name)
		session.mu.Unlock()
		sm.applyAgentDefaults(agent, session)
		order = append(order, session.ID)

		if surface.Focused {
			activeSID = session.ID
		}
	}

	// Remove stale sessions that no longer have a surface
	for id, session := range agent.Sessions {
		if !seen[id] {
			session.stopWatching()
			delete(agent.Sessions, id)
		}
	}

	if activeSID == "" || agent.Sessions[activeSID] == nil {
		if len(order) > 0 {
			activeSID = order[0]
		}
	}

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
	sessionType := effectiveAgentType(provider, agent.defaultType())
	if session := agent.ActiveSession(); session != nil {
		session.setAgentType(sessionType)
		sm.applyAgentDefaults(agent, session)
		if ShouldLaunchPersistentAgent(session) {
			if err := LaunchAgentInTerminal(sm.cmux, agent.workspaceID(), surface.ID, session, sm.aiCfg); err != nil {
				log.Printf("[session] failed to launch agent in new session %s: %v", name, err)
			}
		}
		sm.persistSessionState(agent, session)
	}
	sm.reply(msg, fmt.Sprintf("New session [%s] created (%s)", name, sessionType))
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

	session.stopWatching()
	_ = StopAgent(sm.cmux, agent.workspaceID(), session.SurfaceID)
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
	session.stopWatching()
	// Send Ctrl+C to stop any running process, then relaunch the agent
	_ = StopAgent(sm.cmux, agent.workspaceID(), session.SurfaceID)
	session.setProviderSessionID("")
	session.setPendingApproval("")
	session.setLastUsage(UsageSnapshot{})
	session.setRunningTurn(false)
	if ShouldLaunchPersistentAgent(session) {
		if err := LaunchAgentInTerminal(sm.cmux, agent.workspaceID(), session.SurfaceID, session, sm.aiCfg); err != nil {
			log.Printf("[session] failed to relaunch agent after reset: %v", err)
		}
	}
	sm.persistSessionState(agent, session)
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
	sm.persistSessionState(agent, session)
	sm.reply(msg, "Model set to: "+session.model()+". Use /reset to apply to running session.")
}

func (sm *SessionManager) handleThinkCommand(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}
	if session.agentType() != AgentTypeClaude {
		sm.reply(msg, "Thinking effort is currently supported only for Claude sessions. Use /model for Codex tuning.")
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
	sm.persistSessionState(agent, session)
	sm.reply(msg, "Thinking effort set to: "+value+". Use /reset to apply to running session.")
}

func (sm *SessionManager) handleFastCommand(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}
	if session.agentType() != AgentTypeClaude {
		sm.reply(msg, "Fast mode currently maps to Claude presets only. Use /model for Codex sessions.")
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
		sm.reply(msg, "Fast mode on — model set to: sonnet. Use /reset to apply to running session.")
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
		sm.reply(msg, "Fast mode off — model restored to: "+model+". Use /reset to apply to running session.")
	default:
		sm.reply(msg, "Usage: /fast on|off")
		return
	}
	sm.persistSessionState(agent, session)
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
	sm.persistSessionState(agent, session)
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
	// Use subprocess pipe for AI mode: launch agent directly, read stdout
	session.stopWatching()

	proc, err := LaunchAgentProcess(session, prompt, sm.aiCfg, sm.mediaDir)
	if err != nil {
		sm.reply(msg, "Error launching agent: "+err.Error())
		return
	}

	ctx, cancel := context.WithCancel(sm.ctx)
	session.setWatchCancel(cancel)
	session.setRunningTurn(true)

	// Store process for /stop command
	session.mu.Lock()
	session.agentProc = proc
	session.mu.Unlock()

	go sm.watchAgentProcess(ctx, proc, session, msg.ChannelName, msg.ChatID, msg.ContextToken)
	sm.persistSessionState(agent, session)
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
	session.setRunningTurn(true)
	go sm.watchOutput(ctx, agent.workspaceID(), session, msg.ChannelName, msg.ChatID, msg.ContextToken)

	if err := sm.cmux.SendText(agent.workspaceID(), session.SurfaceID, command+"\n"); err != nil {
		session.stopWatching()
		session.setRunningTurn(false)
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

	session.stopWatching()
	session.setRunningTurn(false)
	if err := StopAgent(sm.cmux, agent.workspaceID(), session.SurfaceID); err != nil {
		sm.reply(msg, "Error sending stop signal: "+err.Error())
		return
	}
	sm.reply(msg, "Stop signal sent.")
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

	if err := sm.cmux.SendText(agent.workspaceID(), session.SurfaceID, "y\n"); err != nil {
		sm.reply(msg, "Error sending approval: "+err.Error())
		return
	}
	session.setPendingApproval("")
	sm.persistSessionState(agent, session)
	sm.reply(msg, "✓ Approved.")
}

func (sm *SessionManager) handleDeny(agent *Agent, msg channels.InboundMessage) {
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

	if err := sm.cmux.SendText(agent.workspaceID(), session.SurfaceID, "n\n"); err != nil {
		sm.reply(msg, "Error sending denial: "+err.Error())
		return
	}
	session.setPendingApproval("")
	sm.persistSessionState(agent, session)
	sm.reply(msg, "✗ Denied.")
}

func (sm *SessionManager) handlePermissionCommand(agent *Agent, msg channels.InboundMessage, args []string) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session")
		return
	}
	if session.agentType() != AgentTypeClaude {
		sm.reply(msg, "Permission mode is currently supported only for Claude sessions.")
		return
	}

	if len(args) == 0 {
		perm := session.permissionMode()
		if perm == "" {
			perm = sm.aiCfg.ClaudePermissionMode
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
		sm.persistSessionState(agent, session)
		sm.reply(msg, "Permission mode set to: "+mode+". Use /reset to apply to running session.")
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
		if msg.ChannelName == "wechat" {
			location := defaultString(shot.Path, "(path unavailable)")
			sm.reply(msg, "Browser screenshot captured locally: "+location+"\nWeChat native image upload is not implemented yet.")
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
		if msg.ChannelName == "wechat" {
			sm.reply(msg, "Terminal screenshot captured locally: "+defaultString(shot.Path, "(path unavailable)")+"\nWeChat native image upload is not implemented yet.")
			return
		}
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
		sm.reply(msg, "Screenshot unavailable on this build: "+readErr.Error())
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

var rawAllowedMethods = map[string]bool{
	"system.ping":        true,
	"system.tree":        true,
	"workspace.list":     true,
	"window.list":        true,
	"surface.list":       true,
	"surface.read_text":  true,
	"pane.list":          true,
	"browser.screenshot": true,
}

func (sm *SessionManager) handleRaw(agent *Agent, msg channels.InboundMessage, raw string) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		sm.reply(msg, "Usage: /raw <method> [json]")
		return
	}
	method := fields[0]
	if !rawAllowedMethods[method] {
		sm.reply(msg, fmt.Sprintf("Method %q is not allowed via /raw", method))
		return
	}
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
				if ShouldLaunchPersistentAgent(session) {
					if err := LaunchAgentInTerminal(sm.cmux, agent.workspaceID(), session.SurfaceID, session, sm.aiCfg); err != nil {
						log.Printf("[session] failed to launch agent for new agent %s: %v", name, err)
					}
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
			name = sm.currentDefaultAgentID()
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
		"Default agent: %s\nActive agent: %s\nWorkspace: %s\nActive session: %s\nRoute: %s\nProvider: %s\nContext ID: %s\nVerbose: %t\nRunning: %t\nWatching output: %t",
		sm.currentDefaultAgentID(),
		agent.Name,
		agent.workspaceID(),
		session.Name,
		defaultString(session.RouteKey, sessionRouteKey(agent.Name, session.Name)),
		session.agentType(),
		defaultString(session.providerSessionID(), "(new)"),
		session.verbose(),
		session.isRunningTurn(),
		session.isWatching(),
	))
}

func (sm *SessionManager) handleHelp(msg channels.InboundMessage) {
	help := `cmux IM Bridge Commands:

/claude        Switch the default agent to Claude
/codex         Switch the default agent to Codex
/cc <message>  Send one message to Claude without switching default
/cx <message>  Send one message to Codex without switching default
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

Plain text goes to the default agent's main session.
/cc and /cx are one-shot prompts that do not change the default agent.
In shell sessions, plain text runs in the terminal.`

	help += "\n\nProvider notes:\n/think, /fast, /permission, /approve currently apply to Claude sessions only.\nClaude runs each turn through `claude -p --output-format stream-json` and resumes from the provider session id when available.\nCodex runs through `codex exec --json` and resumes from the provider session id when available."

	sm.reply(msg, help)
}

// handleStreamEvent processes a parsed stream event and updates session state accordingly.
// It is called by the presenter callback and by output_watcher for AI-mode sessions.
func (sm *SessionManager) handleStreamEvent(session *Session, event StreamEvent) {
	session.streamMu.Lock()
	defer session.streamMu.Unlock()

	switch event.Type {
	case StreamEventInit:
		if event.SessionID != "" {
			session.setProviderSessionID(event.SessionID)
		} else if threadID, ok := event.Meta["thread_id"].(string); ok && threadID != "" {
			session.setProviderSessionID(threadID)
		}
	case StreamEventControlRequest:
		session.setPendingApproval(event.Content)
		// Presenter already notified IM via HandleEvent
	case StreamEventResult:
		session.setRunningTurn(false)
		if id, ok := event.Meta["session_id"].(string); ok && id != "" {
			session.setProviderSessionID(id)
		}
		if usage, ok := event.Meta["usage"].(map[string]interface{}); ok {
			snap := UsageSnapshot{Provider: event.Provider}
			if v, ok := usage["input_tokens"].(float64); ok {
				snap.InputTokens = int(v)
			}
			if v, ok := usage["cache_read_input_tokens"].(float64); ok {
				snap.CachedInputTokens = int(v)
			}
			if v, ok := usage["output_tokens"].(float64); ok {
				snap.OutputTokens = int(v)
			}
			session.setLastUsage(snap)
		}
	}
	sm.persistSessionSnapshot(session)
}

func (sm *SessionManager) teardownAgent(agent *Agent) {
	for _, session := range agent.ListSessions() {
		session.stopWatching()
		_ = StopAgent(sm.cmux, agent.workspaceID(), session.SurfaceID)
	}
}

func (sm *SessionManager) sendWelcome(msg channels.InboundMessage) {
	sm.reply(msg, "cmux IM bridge is ready.\n"+
		"Plain text goes to the default agent main session.\n"+
		"/claude or /codex switches the default agent.\n"+
		"/cc and /cx send one-shot prompts.\n"+
		"/help shows the full command list.")
}

func (sm *SessionManager) send(channelName, chatID string, msg channels.OutboundMessage) {
	if err := sm.channel.Send(channelName, chatID, msg); err != nil {
		log.Printf("[session] failed to send outbound message: %v", err)
	}
}

func (sm *SessionManager) reply(msg channels.InboundMessage, text string) {
	sm.sendTextWithContext(msg.ChannelName, msg.ChatID, msg.ContextToken, text)
}

func (sm *SessionManager) sendTextWithContext(channelName, chatID, contextToken, text string) {
	chunks := splitMessage(text, sm.channel.MaxMessageLength(channelName))
	if len(chunks) == 0 {
		chunks = []string{text}
	}
	for _, chunk := range chunks {
		sm.send(channelName, chatID, channels.OutboundMessage{
			Text:         chunk,
			Format:       "text",
			ContextToken: contextToken,
		})
	}
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
	oldCancel := s.watchCancel
	s.watchCancel = cancel
	s.mu.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
}

func (s *Session) stopWatching() {
	s.mu.Lock()
	cancel := s.watchCancel
	s.watchCancel = nil
	proc := s.agentProc
	s.agentProc = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	// Kill agent subprocess if running
	if proc != nil && proc.Cmd != nil && proc.Cmd.Process != nil {
		proc.Cmd.Process.Kill()
	}
}

func (s *Session) isWatching() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.watchCancel != nil
}

func (s *Session) setLastOutput(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastOutput = text
}

// swapLastOutput atomically reads the old LastOutput and replaces it with newOutput,
// avoiding the TOCTOU race between separate lastOutput()/setLastOutput() calls.
func (s *Session) swapLastOutput(newOutput string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.LastOutput
	s.LastOutput = newOutput
	return old
}

func (s *Session) isRunningTurn() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.runningTurn
}

func (s *Session) setRunningTurn(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runningTurn = value
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

func (s *Session) workdir() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return strings.TrimSpace(s.Workdir)
}

func (s *Session) setWorkdir(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Workdir = strings.TrimSpace(value)
}

func (s *Session) extraArgs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.ExtraArgs))
	copy(out, s.ExtraArgs)
	return out
}

func (s *Session) setExtraArgs(args []string) {
	cleaned := make([]string, 0, len(args))
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg != "" {
			cleaned = append(cleaned, arg)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ExtraArgs = cleaned
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
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, "\\", "-")
	for strings.Contains(name, "  ") {
		name = strings.ReplaceAll(name, "  ", " ")
	}
	return name
}

func workspaceTitle(userID, agentName string) string {
	return "OC/" + sanitizeName(agentName)
}

func legacyWorkspaceTitle(userID, agentName string) string {
	return fmt.Sprintf("IM %s / %s", userID, agentName)
}

func sessionRouteKey(agentName, sessionName string) string {
	agentKey := normalizeKey(agentName)
	if agentKey == "" {
		agentKey = AgentTypeClaude
	}
	sessionKey := normalizeKey(sessionName)
	if sessionKey == "" {
		sessionKey = defaultSessionName
	}
	return fmt.Sprintf("agent:%s:%s", agentKey, sessionKey)
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
