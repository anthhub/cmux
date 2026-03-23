package bridge

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/channels"
)

// Agent represents an AI assistant mapped to a cmux Workspace.
// One Agent can have multiple Sessions (Tabs).
type Agent struct {
	ID          string
	Name        string
	WorkspaceID string
	Sessions    map[string]*Session
	ActiveSID   string
	mu          sync.RWMutex
}

// Session represents an independent conversation mapped to a cmux Tab.
type Session struct {
	ID         string
	SurfaceID  string
	Name       string
	LastOutput string
	Watching   bool
}

// ActiveSession returns the currently active session.
func (a *Agent) ActiveSession() *Session {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Sessions[a.ActiveSID]
}

// NewSession creates a new session in this agent.
func (a *Agent) NewSession(name, surfaceID string) *Session {
	a.mu.Lock()
	defer a.mu.Unlock()

	s := &Session{
		ID:        fmt.Sprintf("%s-%s", a.ID, name),
		SurfaceID: surfaceID,
		Name:      name,
	}
	a.Sessions[s.ID] = s
	a.ActiveSID = s.ID
	return s
}

// SwitchSession switches to a session by name.
func (a *Agent) SwitchSession(name string) (*Session, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for id, s := range a.Sessions {
		if s.Name == name {
			a.ActiveSID = id
			return s, true
		}
	}
	return nil, false
}

// SessionManager manages the mapping between IM users and cmux workspaces.
type SessionManager struct {
	agents  map[string]*Agent // userID → Agent
	cmux    *CmuxClient
	channel *channels.Manager
	mu      sync.RWMutex
}

// NewSessionManager creates a new session manager.
func NewSessionManager(cmux *CmuxClient, channel *channels.Manager) *SessionManager {
	return &SessionManager{
		agents:  make(map[string]*Agent),
		cmux:    cmux,
		channel: channel,
	}
}

// GetOrCreateAgent returns the agent for a user, creating one if needed.
func (sm *SessionManager) GetOrCreateAgent(userID string) *Agent {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if agent, ok := sm.agents[userID]; ok {
		return agent
	}

	agent := &Agent{
		ID:       userID,
		Name:     "agent-" + userID,
		Sessions: make(map[string]*Session),
	}
	sm.agents[userID] = agent
	return agent
}

// HandleMessage processes an inbound IM message.
func (sm *SessionManager) HandleMessage(msg channels.InboundMessage) {
	agent := sm.GetOrCreateAgent(msg.UserID)

	// Initialize workspace if needed
	if agent.WorkspaceID == "" {
		ws, err := sm.cmux.CreateWorkspace("im-" + msg.UserID)
		if err != nil {
			log.Printf("[session] failed to create workspace: %v", err)
			sm.reply(msg, "Error: failed to create workspace: "+err.Error())
			return
		}
		agent.WorkspaceID = ws.ID

		// Get the default surface
		surfaces, err := sm.cmux.ListSurfaces(ws.ID)
		if err != nil || len(surfaces) == 0 {
			log.Printf("[session] failed to list surfaces: %v", err)
			sm.reply(msg, "Error: workspace created but no surfaces found")
			return
		}
		agent.NewSession("default", surfaces[0].ID)
		log.Printf("[session] created workspace %s for user %s", ws.ID, msg.UserID)
	}

	// Handle commands
	text := strings.TrimSpace(msg.Text)

	switch {
	case strings.HasPrefix(text, "/new "):
		sm.handleNewSession(agent, msg, strings.TrimPrefix(text, "/new "))

	case text == "/list":
		sm.handleListSessions(agent, msg)

	case strings.HasPrefix(text, "/switch "):
		sm.handleSwitchSession(agent, msg, strings.TrimPrefix(text, "/switch "))

	case text == "/screenshot":
		sm.handleScreenshot(agent, msg)

	case strings.HasPrefix(text, "/agent "):
		sm.handleAgentCommand(msg, strings.TrimPrefix(text, "/agent "))

	case text == "/help":
		sm.handleHelp(msg)

	default:
		sm.handleTextInput(agent, msg)
	}
}

func (sm *SessionManager) handleNewSession(agent *Agent, msg channels.InboundMessage, name string) {
	surface, err := sm.cmux.CreateSurface(agent.WorkspaceID)
	if err != nil {
		sm.reply(msg, "Error creating session: "+err.Error())
		return
	}
	agent.NewSession(name, surface.ID)
	sm.reply(msg, fmt.Sprintf("New session [%s] created and activated", name))
}

func (sm *SessionManager) handleListSessions(agent *Agent, msg channels.InboundMessage) {
	agent.mu.RLock()
	defer agent.mu.RUnlock()

	var lines []string
	for _, s := range agent.Sessions {
		prefix := "  "
		if s.ID == agent.ActiveSID {
			prefix = "▶ "
		}
		lines = append(lines, prefix+s.Name)
	}
	sm.reply(msg, "Sessions:\n"+strings.Join(lines, "\n"))
}

func (sm *SessionManager) handleSwitchSession(agent *Agent, msg channels.InboundMessage, name string) {
	session, ok := agent.SwitchSession(name)
	if !ok {
		sm.reply(msg, "Session not found: "+name)
		return
	}
	_ = sm.cmux.FocusSurface(session.SurfaceID)
	sm.reply(msg, "Switched to session: "+name)
}

func (sm *SessionManager) handleScreenshot(_ *Agent, msg channels.InboundMessage) {
	// TODO: implement screenshot via cmux API
	sm.reply(msg, "Screenshot not yet implemented")
}

func (sm *SessionManager) handleAgentCommand(msg channels.InboundMessage, subcmd string) {
	// TODO: implement /agent new, /agent list, /agent switch
	sm.reply(msg, "Agent commands not yet implemented: "+subcmd)
}

func (sm *SessionManager) handleHelp(msg channels.InboundMessage) {
	help := `cmux IM Bridge Commands:

/new <name>     - Create new session (tab) in current agent
/list           - List all sessions
/switch <name>  - Switch to another session
/screenshot     - Take screenshot of current session
/agent new <n>  - Create new agent (workspace)
/agent list     - List all agents
/agent switch   - Switch agent
/help           - Show this help

Any other text is sent directly to the terminal.`

	sm.reply(msg, help)
}

func (sm *SessionManager) handleTextInput(agent *Agent, msg channels.InboundMessage) {
	session := agent.ActiveSession()
	if session == nil {
		sm.reply(msg, "No active session. Use /new <name> to create one.")
		return
	}

	log.Printf("[session] sending to surface %s: %s", session.SurfaceID, msg.Text)

	// Capture screen state BEFORE sending command (for diff baseline)
	if !session.Watching {
		baseline, _ := sm.cmux.ReadText(session.SurfaceID)
		session.LastOutput = baseline
	}

	// Send text to terminal
	if err := sm.cmux.SendText(session.SurfaceID, msg.Text+"\n"); err != nil {
		sm.reply(msg, "Error sending to terminal: "+err.Error())
		return
	}

	// Start output watcher if not already running
	if !session.Watching {
		session.Watching = true
		go sm.watchOutput(agent, session, msg.ChannelName, msg.ChatID)
	}
}

func (sm *SessionManager) reply(msg channels.InboundMessage, text string) {
	if err := sm.channel.Send(msg.ChannelName, msg.ChatID, channels.OutboundMessage{
		Text:   text,
		Format: "text",
	}); err != nil {
		log.Printf("[session] failed to send reply: %v", err)
	}
}
