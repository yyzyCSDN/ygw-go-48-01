package topology_test

import (
	"fmt"
	"testing"

	"streamengine/internal/agg"
	"streamengine/internal/topology"
)

func TestRepartitionRoutesKeysConsistently(t *testing.T) {
	ps := agg.NewPartitionedStore(2)
	r := topology.NewRouter(2, ps)

	key := "cart"
	for i := 0; i < 200; i++ {
		candidate := fmt.Sprintf("k%d", i)
		if r.PartitionForKey(candidate, 2) != r.PartitionForKey(candidate, 4) {
			key = candidate
			break
		}
	}

	first := r.Route(key)
	ps.Add(first, key, 5)

	r.Resize(4)

	second := r.Route(key)
	ps.Add(second, key, 7)

	if got := ps.PartitionCount(key); got != 1 {
		t.Fatalf("key split across %d partitions after repartition", got)
	}
	if got := ps.Total(key); got != 12 {
		t.Fatalf("unexpected total after repartition: %d", got)
	}
}
