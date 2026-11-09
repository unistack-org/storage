package vduse

import "testing"

func TestFeatureMask(t *testing.T) {
	base := FeatureMask(true, true, false)
	if base&featBlkRO != 0 {
		t.Fatal("F_RO must not be advertised by default")
	}
	if base&featBlkDiscard == 0 || base&featBlkWriteZeroe == 0 {
		t.Fatal("trim/write-zeroes advertised when enabled")
	}
	ro := FeatureMask(true, true, true)
	if ro&featBlkRO == 0 {
		t.Fatal("F_RO missing for read-only device")
	}
	if ro&^base != featBlkRO {
		t.Fatalf("read-only mask changes more than F_RO: +%x", ro&^base)
	}
	noTrimNoWZ := FeatureMask(false, false, false)
	if noTrimNoWZ&featBlkDiscard != 0 || noTrimNoWZ&featBlkWriteZeroe != 0 {
		t.Fatal("discard/write-zeroes must be droppable")
	}
	if noTrimNoWZ&featAccessPlatform == 0 {
		t.Fatal("VIRTIO_F_ACCESS_PLATFORM is mandatory and must stay")
	}
}
