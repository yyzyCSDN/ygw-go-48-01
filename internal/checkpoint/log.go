package checkpoint

import (
	"sort"
	"sync"
)

// LogEntry is one checkpoint record in the coordinator's history.
type LogEntry struct {
	Offset int64
	Seq    int64
	Kind   string
}

// Log keeps the most recent checkpoint history so operators can see restore
// and commit activity. Older entries are dropped when the capacity is reached.
type Log struct {
	mu       sync.Mutex
	capacity int
	entries  []LogEntry
}

// NewLog creates a checkpoint log with the given capacity.
func NewLog(capacity int) *Log {
	return &Log{capacity: capacity}
}

// Append records one checkpoint event.
func (l *Log) Append(entry LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
	if len(l.entries) > l.capacity {
		l.entries = l.entries[len(l.entries)-l.capacity:]
	}
}

// Recent returns the newest entries in chronological order.
func (l *Log) Recent() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := append([]LogEntry(nil), l.entries...)
	sort.Slice(out, func(i, j int) bool { return out[i].Offset < out[j].Offset })
	return out
}

// Size returns how many entries are currently retained.
func (l *Log) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}
