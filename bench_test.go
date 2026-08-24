package main

import "testing"

func TestDatasetIsFixedSeed(t *testing.T) {
	a, b := dataset(100), dataset(100)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("dataset not deterministic at %d: %d != %d", i, a[i], b[i])
		}
	}
	if a[0] == a[1] {
		t.Fatal("dataset looks constant")
	}
}

func TestPercentile(t *testing.T) {
	s := []int64{1e6, 2e6, 3e6, 4e6, 5e6, 6e6, 7e6, 8e6, 9e6, 10e6}
	if got := percentile(s, 0.5); got != 5 {
		t.Fatalf("p50 = %v, want 5", got)
	}
	if got := percentile(s, 1.0); got != 10 {
		t.Fatalf("max = %v, want 10", got)
	}
	if got := percentile(nil, 0.5); got != 0 {
		t.Fatalf("empty = %v, want 0", got)
	}
}

// recoverySeconds must ignore messages written after the broker returned, and
// messages that never arrived at all.
func TestRecoverySeconds(t *testing.T) {
	back := int64(1_000_000_000)
	w := &writerResult{commitNs: []int64{back - 100, back - 50, back + 10}}
	written := map[int64]bool{0: true, 1: true, 2: true}
	seen := map[int64]int64{0: back + 500_000_000, 2: back + 9_000_000_000}
	if got := recoverySeconds(written, seen, w, back); got != 0.5 {
		t.Fatalf("recovery = %v, want 0.5", got)
	}
	if got := recoverySeconds(written, map[int64]int64{}, w, back); got != 0 {
		t.Fatalf("nothing received should be 0, got %v", got)
	}
}

func TestCustomerVariesWithSeq(t *testing.T) {
	if customer(0) == customer(1) {
		t.Fatal("customer should vary with seq")
	}
	if itoa(0) != "0" || itoa(4207) != "4207" {
		t.Fatalf("itoa broken: %q %q", itoa(0), itoa(4207))
	}
}
