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

// Route returns the partition that owns a key under the current count.
func (r *Router) Route(key string) model.PartitionID {
	r.mu.Lock()
	defer r.mu.Unlock()
	if part, ok := r.cache[key]; ok {
		return part
	}
	part := r.PartitionForKey(key, r.partitions)
	r.cache[key] = part
	return part
}

// Resize changes the partition count and migrates every key's state to its new
// partition before the new count is published, so no key is ever split across
// two partitions.
func (r *Router) Resize(partitions int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if partitions == r.partitions {
		return
	}
	r.store.Repartition(r.partitions, partitions, r.PartitionForKey)
	r.partitions = partitions
	r.cache = make(map[string]model.PartitionID)
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
