package channels

import "sync"

// ChannelFactory is a function that creates a Channel from a typed config value.
type ChannelFactory func(cfg interface{}) (Channel, error)

var (
	factories   = make(map[string]ChannelFactory)
	factoriesMu sync.RWMutex
)

// RegisterFactory registers a factory function for the given channel name.
// It panics if name is empty or the factory is nil.
func RegisterFactory(name string, f ChannelFactory) {
	if name == "" {
		panic("channels: RegisterFactory called with empty name")
	}
	if f == nil {
		panic("channels: RegisterFactory called with nil factory for " + name)
	}
	factoriesMu.Lock()
	defer factoriesMu.Unlock()
	factories[name] = f
}

// GetFactory returns the registered factory for the given channel name.
func GetFactory(name string) (ChannelFactory, bool) {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	f, ok := factories[name]
	return f, ok
}

// RegisteredNames returns the names of all registered channel factories.
func RegisteredNames() []string {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	return names
}
