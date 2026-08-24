package agg

import (
	"sync"

	"streamengine/internal/model"
)

// Combiner merges multiple results that belong to the same logical window. The
// pipeline uses it to combine partition-level outputs before exposing them.
type Combiner struct {
	mu sync.Mutex
}

// NewCombiner creates a result combiner.
func NewCombiner() *Combiner {
	return &Combiner{}
}

// Combine folds a list of results into one result per window. Late values are
// merged separately and never affect the published aggregate.
func (c *Combiner) Combine(results []model.WindowResult) []model.WindowResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	merged := make(map[model.WindowID]*model.WindowResult)
	var order []model.WindowID
	for _, result := range results {
		target, ok := merged[result.Window]
		if !ok {
			copy := result
			merged[result.Window] = &copy
			order = append(order, result.Window)
			continue
		}
		target.Count += result.Count
		target.Sum += result.Sum
		target.LateCount += result.LateCount
		target.LateSum += result.LateSum
		if result.Min < target.Min || target.Count == result.Count {
			target.Min = result.Min
		}
		if result.Max > target.Max {
			target.Max = result.Max
		}
		target.Emitted = target.Emitted || result.Emitted
	}
	out := make([]model.WindowResult, 0, len(merged))
	for _, id := range order {
		out = append(out, *merged[id])
	}
	return out
}

// Count returns the number of results held by the last Combine call.
func (c *Combiner) Count(results []model.WindowResult) int {
	return len(c.Combine(results))
}
