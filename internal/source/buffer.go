package source

import (
	"container/heap"

	"streamengine/internal/model"
)

// Buffer reorders events by event time. It keeps at most maxSize events and
// returns overflow events through the overflow callback so callers can decide
// whether to emit them anyway or drop them.
type Buffer struct {
	entries  entryHeap
	maxSize  int
	overflow func(model.Event)
}

type entryHeap []model.Event

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].Timestamp < h[j].Timestamp }
func (h entryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)        { *h = append(*h, x.(model.Event)) }
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// NewBuffer creates a reorder buffer with the given capacity.
func NewBuffer(maxSize int, overflow func(model.Event)) *Buffer {
	return &Buffer{maxSize: maxSize, overflow: overflow}
}

// Add inserts an event; overflow events are routed to the overflow callback.
func (b *Buffer) Add(ev model.Event) {
	heap.Push(&b.entries, ev)
	if b.entries.Len() > b.maxSize {
		over := heap.Pop(&b.entries).(model.Event)
		if b.overflow != nil {
			b.overflow(over)
		}
	}
}

// PopMin removes the event with the smallest event time.
func (b *Buffer) PopMin() model.Event {
	return heap.Pop(&b.entries).(model.Event)
}

// MinTimestamp returns the smallest event time currently buffered.
func (b *Buffer) MinTimestamp() int64 {
	if len(b.entries) == 0 {
		return 1 << 62
	}
	return b.entries[0].Timestamp
}

// Empty reports whether the buffer holds no events.
func (b *Buffer) Empty() bool {
	return len(b.entries) == 0
}

// Size returns the number of buffered events.
func (b *Buffer) Size() int {
	return len(b.entries)
}
