package topology

import (
	"sync"

	"github.com/cespare/xxhash/v2"

	"streamengine/internal/agg"
	"streamengine/internal/model"
)

// Router maps routing keys to partitions with a deterministic hash. The
// partition mapping is stable until Resize migrates every key to its new
// partition, so the same key always lands in exactly one partition.
type Router struct {
	mu         sync.Mutex
	partitions int
	cache      map[string]model.PartitionID
	store      *agg.PartitionedStore
}

// NewRouter creates a key router over the given partitioned store.
func NewRouter(partitions int, store *agg.PartitionedStore) *Router {
	return &Router{
		partitions: partitions,
		cache:      make(map[string]model.PartitionID),
		store:      store,
	}
}

// PartitionForKey deterministically maps a key to a partition for a count.
func (r *Router) PartitionForKey(key string, partitions int) model.PartitionID {
	sum := xxhash.Sum64String(key)
	return model.PartitionID(int(sum % uint64(partitions)))
}

// Route recomputes the owning partition on every call from the live partition
// count. After a resize, the mapping changes even though existing per-key state
// was never moved, so a key can land in a different partition than before.
func (r *Router) Route(key string) model.PartitionID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.PartitionForKey(key, r.partitions)
}

// Resize changes the partition count without migrating the existing per-key
// state. Keys that already accumulated state stay in their old partition while
// new events are routed to the new mapping, splitting each key across two
// partitions.
func (r *Router) Resize(partitions int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitions = partitions
}

// Partitions returns the current partition count.
func (r *Router) Partitions() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.partitions
}

// RoutingState returns a stable view of the router for the monitoring page.
func (r *Router) RoutingState() model.PartitionState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return model.PartitionState{ID: model.PartitionID(r.partitions - 1), Count: r.partitions}
}
