package ws

import (
	"context"
	"sync"
	"time"
)

// PingWheel keeps every hot connection alive from a SINGLE goroutine, instead of
// the official SDK's one ping ticker per connection. At thousands of bots that
// is thousands of goroutines + timers saved.
type PingWheel struct {
	interval time.Duration
	mu       sync.Mutex
	conns    map[*Conn]struct{}
}

// NewPingWheel creates a wheel pinging every interval (Mezon expects ~10s).
func NewPingWheel(interval time.Duration) *PingWheel {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &PingWheel{interval: interval, conns: make(map[*Conn]struct{})}
}

// Add registers a connection for keepalive.
func (w *PingWheel) Add(c *Conn) {
	w.mu.Lock()
	w.conns[c] = struct{}{}
	w.mu.Unlock()
}

// Remove deregisters a connection.
func (w *PingWheel) Remove(c *Conn) {
	w.mu.Lock()
	delete(w.conns, c)
	w.mu.Unlock()
}

// Len reports how many connections are registered.
func (w *PingWheel) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.conns)
}

// Run pings all registered connections every interval until ctx is cancelled.
// Closed connections are pruned (the owner is expected to reconnect/demote).
func (w *PingWheel) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			snapshot := make([]*Conn, 0, len(w.conns))
			for c := range w.conns {
				snapshot = append(snapshot, c)
			}
			w.mu.Unlock()
			for _, c := range snapshot {
				if c.Closed() {
					w.Remove(c)
					continue
				}
				_ = c.ping()
			}
		}
	}
}
