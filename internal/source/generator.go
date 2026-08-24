package source

import (
	"sync"

	"streamengine/internal/model"
)

// GeneratorSpec describes a scripted event feed used by the demo runtime.
type GeneratorSpec struct {
	Keys       []string
	StartTime  int64
	StepTime   int64
	JitterSpan int64
	PingEvery  int
}

// Generator produces deterministic synthetic events. It is safe for concurrent
// use and remembers the next offset so replay can resume from any position.
type Generator struct {
	mu      sync.Mutex
	spec    GeneratorSpec
	next    int64
	emitted int64
}

// NewGenerator creates a generator with the given script.
func NewGenerator(spec GeneratorSpec) *Generator {
	return &Generator{spec: spec, next: 0}
}

// Next emits one synthetic event and returns it with its assigned offset.
func (g *Generator) Next() model.Event {
	g.mu.Lock()
	defer g.mu.Unlock()
	index := int(g.next)
	key := g.spec.Keys[index%len(g.spec.Keys)]
	ts := g.spec.StartTime + int64(index)*g.spec.StepTime + (int64(index)*7)%g.spec.JitterSpan
	if g.spec.PingEvery > 0 && index%g.spec.PingEvery == 0 {
		ev := model.NewPing(key, ts, g.next)
		g.next++
		g.emitted++
		return ev
	}
	value := int64(index%97) + 1
	ev := model.NewData(key, value, ts, g.next)
	g.next++
	g.emitted++
	return ev
}

// Emitted returns how many events the generator has produced.
func (g *Generator) Emitted() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.emitted
}

// ReplayFrom resets the generator so the next call produces the event with the
// given offset. Used by the checkpoint retry path.
func (g *Generator) ReplayFrom(offset int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if offset >= 0 {
		g.next = offset
	}
}

// SkipTo fast-forwards the generator past the given offset so a retry that
// rewound the cursor does not emit duplicate events afterwards.
func (g *Generator) SkipTo(offset int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if offset > g.next {
		g.next = offset
	}
}

// Keys returns a copy of the configured routing keys.
func (g *Generator) Keys() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.spec.Keys...)
}
