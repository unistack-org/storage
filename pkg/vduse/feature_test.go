package vduse

import "testing"

func TestFeatureMask(t *testing.T) {
	base := FeatureMask(true, true, false, false)
	if base&featBlkRO != 0 {
		t.Fatal("F_RO must not be advertised by default")
	}
	if base&featBlkMQ != 0 {
		t.Fatal("F_MQ must not be advertised for a single-queue device")
	}
	if base&featBlkDiscard == 0 || base&featBlkWriteZeroe == 0 {
		t.Fatal("trim/write-zeroes advertised when enabled")
	}
	if base&(1<<11) != 0 {
		t.Fatal("CONFIG_WCE (bit 11) must never be advertised: VDUSE rejects it for block")
	}
	mq := FeatureMask(true, true, false, true)
	if mq&featBlkMQ == 0 {
		t.Fatal("F_MQ missing for multi-queue device")
	}
	if mq&^base != featBlkMQ {
		t.Fatalf("mq mask changes more than F_MQ: +%x", mq&^base)
	}
	ro := FeatureMask(true, true, true, false)
	if ro&featBlkRO == 0 {
		t.Fatal("F_RO missing for read-only device")
	}
	if ro&^base != featBlkRO {
		t.Fatalf("read-only mask changes more than F_RO: +%x", ro&^base)
	}
	noTrimNoWZ := FeatureMask(false, false, false, false)
	if noTrimNoWZ&featBlkDiscard != 0 || noTrimNoWZ&featBlkWriteZeroe != 0 {
		t.Fatal("discard/write-zeroes must be droppable")
	}
	if noTrimNoWZ&featAccessPlatform == 0 {
		t.Fatal("VIRTIO_F_ACCESS_PLATFORM is mandatory and must stay")
	}
}
