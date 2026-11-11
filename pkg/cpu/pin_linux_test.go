//go:build linux

package cpu

import (
	"errors"
	"testing"
)

func TestPinCurrentLinux(t *testing.T) {
	if err := PinCurrent("0"); err != nil {
		t.Fatalf("PinCurrent(0): %v", err)
	}
	if !Pinned() {
		t.Fatal("Pinned() false after successful PinCurrent")
	}
	if err := PinCurrent("99999"); err == nil {
		t.Fatal("PinCurrent should reject a CPU that does not exist")
	}
	if err := PinCurrent("nonsense"); err == nil {
		t.Fatal("PinCurrent should reject a malformed list")
	}
}

func TestPinCurrentSupportedOnLinux(t *testing.T) {
	if err := PinCurrent("0"); errors.Is(err, ErrUnsupported) {
		t.Fatalf("PinCurrent on Linux returned ErrUnsupported: %v", err)
	}
}
