package window

import (
	"sort"
	"sync"

	"streamengine/internal/model"
)

// CatalogEntry is one row of the window monitoring catalog.
type CatalogEntry struct {
	Window   string
	State    string
	Events   int64
	Late     int64
	OpenedAt int64
	ClosedAt int64
}

// Catalog records per-window event counts and lifecycle timestamps so the
// monitoring page can show how every window transitioned.
type Catalog struct {
	mu      sync.Mutex
	entries map[model.WindowID]*CatalogEntry
}

// NewCatalog creates an empty window catalog.
func NewCatalog() *Catalog {
	return &Catalog{entries: make(map[model.WindowID]*CatalogEntry)}
}

// Open registers a window as open at the given engine time.
func (c *Catalog) Open(w model.WindowID, at int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[w]
	if !ok {
		entry = &CatalogEntry{Window: w.String(), State: model.WindowOpen.String(), OpenedAt: at}
		c.entries[w] = entry
	}
	entry.State = model.WindowOpen.String()
}

// RecordEvent counts one accepted event for a window.
func (c *Catalog) RecordEvent(w model.WindowID, at int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[w]
	if !ok {
		entry = &CatalogEntry{Window: w.String(), State: model.WindowOpen.String(), OpenedAt: at}
		c.entries[w] = entry
	}
	entry.Events++
}

// RecordLate counts one late event for a closed window.
func (c *Catalog) RecordLate(w model.WindowID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[w]
	if !ok {
		entry = &CatalogEntry{Window: w.String(), State: model.WindowClosed.String()}
		c.entries[w] = entry
	}
	entry.Late++
}

// Close marks a window closed at the given engine time.
func (c *Catalog) Close(w model.WindowID, at int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[w]
	if !ok {
		entry = &CatalogEntry{Window: w.String(), State: model.WindowClosed.String(), OpenedAt: at}
		c.entries[w] = entry
	}
	entry.State = model.WindowClosed.String()
	entry.ClosedAt = at
}

// Summary returns a stable, sorted view of the catalog.
func (c *Catalog) Summary() []CatalogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]CatalogEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Window < out[j].Window })
	return out
}
