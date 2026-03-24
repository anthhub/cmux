package channels

import "sync/atomic"

// BaseChannel provides common functionality for all channel adapters.
type BaseChannel struct {
	name      string
	running   atomic.Bool
	allowList []string
	maxMsgLen int
}

// NewBaseChannel creates a BaseChannel with the given name, max message length, and allow-list.
// An empty allowList means all users are allowed.
func NewBaseChannel(name string, maxMsgLen int, allowList []string) *BaseChannel {
	b := &BaseChannel{
		name:      name,
		maxMsgLen: maxMsgLen,
	}
	if len(allowList) > 0 {
		b.allowList = make([]string, len(allowList))
		copy(b.allowList, allowList)
	}
	return b
}

// Name returns the channel name.
func (b *BaseChannel) Name() string { return b.name }

// IsRunning returns whether the channel is currently running.
func (b *BaseChannel) IsRunning() bool { return b.running.Load() }

// SetRunning sets the running state of the channel.
func (b *BaseChannel) SetRunning(v bool) { b.running.Store(v) }

// IsAllowed returns whether the given userID is permitted to use this channel.
// An empty allowList permits all users.
func (b *BaseChannel) IsAllowed(userID string) bool {
	if len(b.allowList) == 0 {
		return true
	}
	for _, id := range b.allowList {
		if id == userID {
			return true
		}
	}
	return false
}

// MaxMessageLength returns the maximum safe outbound message length.
func (b *BaseChannel) MaxMessageLength() int { return b.maxMsgLen }
