package window

import (
	"sort"
	"sync"

	"streamengine/internal/model"
)

// sessionState tracks one keyed session window. The window end slides forward
// with every touch so that a timeout scan never splits a live session.
type sessionState struct {
	window     model.WindowID
	lastActive int64
	values     []int64
	state      model.WindowState
}

// SessionManager owns session windows keyed by routing key. Touch and
// ScanTimeout share one lock, which guarantees that an event arriving before a
// scan observes the boundary is never misclassified as a timeout.
type SessionManager struct {
	mu       sync.Mutex
	timeout  int64
	sink     ResultSink
	sessions map[string]*sessionState
}

// NewSessionManager creates a session window manager with the given gap.
func NewSessionManager(timeout int64, sink ResultSink) *SessionManager {
	return &SessionManager{timeout: timeout, sink: sink, sessions: make(map[string]*sessionState)}
}

// Touch refreshes the session for a key. Data values are appended to the
// session accumulation; pings only advance liveness. Both the last-active
// time and the session end are updated under the same lock the scanner uses.
func (s *SessionManager) Touch(key string, ts int64, value int64, hasValue bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss, ok := s.sessions[key]
	if !ok {
		ss = &sessionState{
			window:     model.WindowID{Start: ts, End: ts + s.timeout, Key: key},
			lastActive: ts,
			state:      model.WindowOpen,
		}
		s.sessions[key] = ss
	} else {
		if ts < ss.window.Start {
			ss.window.Start = ts
		}
		// Slide the session end forward with the latest activity so a timeout
		// scan never closes a session while an event is still within the live
		// gap. Only the largest event time seen advances liveness: a late,
		// smaller-timestamp event must not pull the end backward and reopen a
		// premature timeout window.
		if ts > ss.lastActive {
			ss.lastActive = ts
			ss.window.End = ts + s.timeout
		}
	}
	if hasValue {
		ss.values = append(ss.values, value)
	}
}

// ScanTimeout closes sessions whose live gap has elapsed. The session end is
// lastActive + timeout, refreshed on every touch, so an event that just
// arrived (which is at most the confirmed watermark driving this scan) has
// already pushed the end past `now` and the session is never split. A session
// only fires here when no event for the key has been seen for the whole gap.
func (s *SessionManager) ScanTimeout(now int64) []model.WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	var emitted []model.WindowResult
	for key, ss := range s.sessions {
		if ss.state == model.WindowClosed {
			continue
		}
		// `now` is the confirmed watermark, which only advances after every
		// event up to it has been touched. Since each touch sets
		// End = lastActive + timeout, an event at `now` leaves End > now and
		// skips this close — the session survives until a full gap is idle.
		if now < ss.window.End {
			continue
		}
		ss.state = model.WindowTriggering
		result, ok := s.sink.EmitResult(ss.window, ss.values)
		if ok {
			emitted = append(emitted, *result)
		}
		ss.state = model.WindowClosed
		delete(s.sessions, key)
	}
	sort.Slice(emitted, func(i, j int) bool { return emitted[i].Window.Start < emitted[j].Window.Start })
	return emitted
}

// IsActive reports whether a session still exists and is open.
func (s *SessionManager) IsActive(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss, ok := s.sessions[key]
	return ok && ss.state != model.WindowClosed
}

// SessionCount returns the number of tracked sessions.
func (s *SessionManager) SessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// Snapshot returns a stable view of every tracked session.
func (s *SessionManager) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.sessions))
	for key, ss := range s.sessions {
		out[key] = ss.window.String()
	}
	return out
}
