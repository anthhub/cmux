package bridge

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PersistedSessionState captures the bridge-owned session metadata that must survive restarts.
type PersistedSessionState struct {
	SessionKey        string `json:"session_key"`
	AgentID           string `json:"agent_id"`
	WorkspaceID       string `json:"workspace_id"`
	SurfaceID         string `json:"surface_id"`
	Provider          string `json:"provider"`
	ProviderSessionID string `json:"provider_session_id"`
	Model             string `json:"model"`
	PermissionMode    string `json:"permission_mode"`
	Workdir           string `json:"workdir"`
	Effort            string `json:"effort"`
	Verbose           bool   `json:"verbose"`
	Fast              bool   `json:"fast"`
	UpdatedAtUnix     int64  `json:"updated_at_unix"`
}

type gatewayStateFile struct {
	DefaultAgent string                           `json:"default_agent"`
	Sessions     map[string]PersistedSessionState `json:"sessions"`
}

// GatewayStateStore persists the bridge routing state in a local JSON file.
type GatewayStateStore struct {
	path string
	mu   sync.Mutex
	data gatewayStateFile
}

func OpenGatewayStateStore(path string) (*GatewayStateStore, error) {
	if path == "" {
		return nil, fmt.Errorf("state store path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create state store dir: %w", err)
	}

	store := &GatewayStateStore{
		path: path,
		data: gatewayStateFile{Sessions: map[string]PersistedSessionState{}},
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *GatewayStateStore) load() error {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read state store: %w", err)
	}
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return fmt.Errorf("parse state store: %w", err)
	}
	if s.data.Sessions == nil {
		s.data.Sessions = map[string]PersistedSessionState{}
	}
	return nil
}

func (s *GatewayStateStore) flushLocked() error {
	if s.data.Sessions == nil {
		s.data.Sessions = map[string]PersistedSessionState{}
	}

	payload, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state store: %w", err)
	}

	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0600); err != nil {
		return fmt.Errorf("write temp state store: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		os.Remove(tmpPath) // best-effort cleanup
		return fmt.Errorf("replace state store: %w", err)
	}
	return nil
}

func (s *GatewayStateStore) Close() error {
	return nil
}

func (s *GatewayStateStore) DefaultAgent() (string, error) {
	if s == nil {
		return "", nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.DefaultAgent, nil
}

func (s *GatewayStateStore) SetDefaultAgent(agentID string) error {
	if s == nil || agentID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.DefaultAgent = agentID
	return s.flushLocked()
}

func (s *GatewayStateStore) LoadSession(sessionKey string) (*PersistedSessionState, error) {
	if s == nil || sessionKey == "" {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.data.Sessions[sessionKey]
	if !ok {
		return nil, nil
	}
	copyState := state
	return &copyState, nil
}

func (s *GatewayStateStore) SaveSession(state PersistedSessionState) error {
	if s == nil || state.SessionKey == "" {
		return nil
	}
	if state.UpdatedAtUnix == 0 {
		state.UpdatedAtUnix = time.Now().Unix()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Sessions == nil {
		s.data.Sessions = map[string]PersistedSessionState{}
	}
	s.data.Sessions[state.SessionKey] = state
	return s.flushLocked()
}
