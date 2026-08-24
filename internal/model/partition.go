package model

import "strconv"

// PartitionID addresses one parallel partition of a stateful operator.
type PartitionID int

func (p PartitionID) String() string {
	return strconv.Itoa(int(p))
}

// PartitionState bundles the per-partition runtime data that must stay
// consistent while a repartition is in flight.
type PartitionState struct {
	ID    PartitionID
	Count int
}
