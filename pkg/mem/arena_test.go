package mem

import (
	"testing"
)

func TestArenaSlots(t *testing.T) {
	a, err := NewArena(8, 4096, false)
	if err != nil {
		t.Fatalf("NewArena: %v", err)
	}
	defer a.Close()
	if a.N() != 8 || a.Size() < 4096 {
		t.Fatalf("N=%d Size=%d", a.N(), a.Size())
	}
	if len(a.Data) != a.N()*a.Size() {
		t.Fatalf("Data len %d != N*Size", len(a.Data))
	}
	for i := 0; i < a.N(); i++ {
		s := a.Slot(i)
		if len(s) != a.Size() {
			t.Fatalf("slot %d len %d", i, len(s))
		}
		for j := 0; j < i; j++ {
			t0 := a.Slot(j)
			over := s[:0]
			if cap(t0[:0:0]) == cap(over) && &t0[0] == &s[0] {
				t.Fatalf("slots %d and %d alias", i, j)
			}
		}
		s[0] = byte(i + 1) // writable
	}
}

func TestArenaRoundUp(t *testing.T) {
	a, err := NewArena(4, 10, false)
	if err != nil {
		t.Fatalf("NewArena: %v", err)
	}
	defer a.Close()
	if a.Size() < 4096 {
		t.Fatalf("slot size %d not page-rounded", a.Size())
	}
}

func TestArenaCloseIdempotent(t *testing.T) {
	a, err := NewArena(2, 4096, false)
	if err != nil {
		t.Fatalf("NewArena: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close twice: %v", err)
	}
	var nilA *Arena
	if err := nilA.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
}
