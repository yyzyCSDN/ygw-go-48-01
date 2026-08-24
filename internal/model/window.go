package model

import "fmt"

// WindowID uniquely identifies a window instance. Tumbling windows use an
// empty Key, while session windows embed the session key so that each key owns
// an independent window timeline.
type WindowID struct {
	Start int64
	End   int64
	Key   string
}

func (w WindowID) String() string {
	if w.Key == "" {
		return fmt.Sprintf("window[%d,%d)", w.Start, w.End)
	}
	return fmt.Sprintf("session[%s][%d,%d)", w.Key, w.Start, w.End)
}

// WindowState is the lifecycle state machine of a window:
// open -> triggering -> closing -> closed.
type WindowState int

const (
	WindowOpen WindowState = iota
	WindowTriggering
	WindowClosing
	WindowClosed
)

func (s WindowState) String() string {
	switch s {
	case WindowOpen:
		return "open"
	case WindowTriggering:
		return "triggering"
	case WindowClosing:
		return "closing"
	case WindowClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// CanClose reports whether a window may transition into the closed state.
// Only open windows can be closed; triggering and closing are transient states
// that require an explicit confirmation before becoming closed.
func (s WindowState) CanClose() bool {
	return s == WindowOpen || s == WindowTriggering
}

// WindowType distinguishes tumbling windows from session windows.
type WindowType int

const (
	WindowTumbling WindowType = iota
	WindowSession
)

func (t WindowType) String() string {
	if t == WindowSession {
		return "session"
	}
	return "tumbling"
}
