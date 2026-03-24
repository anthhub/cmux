package bridge

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Instance represents a discovered cmux instance.
type Instance struct {
	ID         string      `json:"id"`
	Label      string      `json:"label"`
	SocketPath string      `json:"socket_path"`
	Client     *CmuxClient `json:"-"`
	Healthy    bool        `json:"healthy"`
	LastPing   time.Time   `json:"last_ping"`
}

// InstanceManager manages connections to multiple cmux instances.
type InstanceManager struct {
	mu        sync.RWMutex
	instances map[string]*Instance
	defaultID string
}

// NewInstanceManager creates a new InstanceManager.
func NewInstanceManager() *InstanceManager {
	return &InstanceManager{
		instances: make(map[string]*Instance),
	}
}

// Discover scans known socket paths and pings each to find live cmux instances.
func (im *InstanceManager) Discover() error {
	home, _ := os.UserHomeDir()

	candidates := []string{
		filepath.Join(home, "Library", "Application Support", "cmux", "cmux.sock"),
		"/tmp/cmux.sock",
		"/tmp/cmux-staging.sock",
		"/tmp/cmux-nightly.sock",
	}

	// Glob for debug tagged sockets
	debugGlob, _ := filepath.Glob("/tmp/cmux-debug-*.sock")
	candidates = append(candidates, debugGlob...)

	var firstErr error
	for _, path := range candidates {
		if _, err := os.Stat(path); err != nil {
			continue
		}
		if _, err := im.Register(path); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Register manually registers a cmux instance by socket path.
// If the socket is already registered, it returns the existing instance.
func (im *InstanceManager) Register(socketPath string) (*Instance, error) {
	// Verify connectivity before registering
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to socket %s: %w", socketPath, err)
	}
	conn.Close()

	client := NewCmuxClient(socketPath)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect client for %s: %w", socketPath, err)
	}

	// Ping to verify liveness
	if err := client.Ping(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping failed for %s: %w", socketPath, err)
	}

	id := socketPath
	inst := &Instance{
		ID:         id,
		Label:      filepath.Base(socketPath),
		SocketPath: socketPath,
		Client:     client,
		Healthy:    true,
		LastPing:   time.Now(),
	}

	im.mu.Lock()
	defer im.mu.Unlock()

	im.instances[id] = inst
	if im.defaultID == "" {
		im.defaultID = id
	}

	return inst, nil
}

// Route returns the CmuxClient for the given instance ID.
// If instanceID is empty, the default instance is used.
func (im *InstanceManager) Route(instanceID string) (*CmuxClient, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	if instanceID == "" {
		instanceID = im.defaultID
	}
	inst, ok := im.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", instanceID)
	}
	if !inst.Healthy {
		return nil, fmt.Errorf("instance %q is unhealthy", instanceID)
	}
	return inst.Client, nil
}

// Default returns the default instance's client.
func (im *InstanceManager) Default() (*CmuxClient, error) {
	return im.Route("")
}

// SetDefault sets the default instance by ID.
func (im *InstanceManager) SetDefault(id string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, ok := im.instances[id]; !ok {
		return fmt.Errorf("instance %q not found", id)
	}
	im.defaultID = id
	return nil
}

// List returns all known instances.
func (im *InstanceManager) List() []*Instance {
	im.mu.RLock()
	defer im.mu.RUnlock()

	result := make([]*Instance, 0, len(im.instances))
	for _, inst := range im.instances {
		result = append(result, inst)
	}
	return result
}

// HealthCheck pings all instances and updates their health status.
func (im *InstanceManager) HealthCheck() {
	im.mu.Lock()
	defer im.mu.Unlock()

	for _, inst := range im.instances {
		_, err := inst.Client.Call("system.ping", nil)
		inst.Healthy = err == nil
		if inst.Healthy {
			inst.LastPing = time.Now()
		}
	}
}

// StartHealthLoop starts a background goroutine that periodically health checks all instances.
func (im *InstanceManager) StartHealthLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				im.HealthCheck()
			}
		}
	}()
}
