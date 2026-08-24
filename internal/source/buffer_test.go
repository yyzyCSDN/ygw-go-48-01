package source

import (
	"testing"

	"streamengine/internal/model"
)

func TestBufferReordersByTimestamp(t *testing.T) {
	b := NewBuffer(16, nil)
	b.Add(model.NewData("k", 1, 300, 1))
	b.Add(model.NewData("k", 2, 100, 2))
	b.Add(model.NewData("k", 3, 200, 3))
	got := []int64{b.PopMin().Timestamp, b.PopMin().Timestamp, b.PopMin().Timestamp}
	if got[0] != 100 || got[1] != 200 || got[2] != 300 {
		t.Fatalf("buffer did not reorder: %v", got)
	}
}

func TestBufferOverflowRoutesToCallback(t *testing.T) {
	var overflow []model.Event
	b := NewBuffer(2, func(ev model.Event) { overflow = append(overflow, ev) })
	b.Add(model.NewData("k", 1, 100, 1))
	b.Add(model.NewData("k", 2, 200, 2))
	b.Add(model.NewData("k", 3, 300, 3))
	if len(overflow) != 1 || overflow[0].Timestamp != 100 {
		t.Fatalf("overflow did not receive the oldest event: %+v", overflow)
	}
}
