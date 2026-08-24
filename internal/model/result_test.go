package model

import "testing"

func TestWindowResultApply(t *testing.T) {
	r := &WindowResult{Window: WindowID{Start: 0, End: 100}}
	r.Apply(5)
	r.Apply(3)
	if r.Count != 2 || r.Sum != 8 || r.Min != 3 || r.Max != 5 {
		t.Fatalf("unexpected fold: %+v", r)
	}
	if r.Emitted {
		t.Fatalf("result must start un-emitted")
	}
}

func TestWindowResultLateKeepsPublishedValues(t *testing.T) {
	r := &WindowResult{Window: WindowID{Start: 0, End: 100}}
	r.Apply(7)
	r.ApplyLate(2)
	if r.Sum != 7 || r.LateSum != 2 || r.LateCount != 1 {
		t.Fatalf("late fold leaked into published values: %+v", r)
	}
}
