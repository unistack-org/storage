package main

import "testing"

// TestCanonicalExportType: the canonical wire name is "vhost-user-blk"; the
// old shorthand "vub" (and other spellings) map onto it.
func TestCanonicalExportType(t *testing.T) {
	for _, in := range []string{"", "nbd", "vduse", "vhost-user-blk"} {
		got, err := canonicalExportType(in)
		if err != nil || got != in {
			t.Fatalf("canonicalExportType(%q) = %q, %v", in, got, err)
		}
	}
	for _, in := range []string{"vub", "Vhost-User-Blk", " vhost_user_blk ", "vhostuserblk", "vhost-user"} {
		got, err := canonicalExportType(in)
		if err != nil || got != "vhost-user-blk" {
			t.Fatalf("canonicalExportType(%q) = %q, %v; want vhost-user-blk", in, got, err)
		}
	}
	if _, err := canonicalExportType("iscsi"); err == nil {
		t.Fatal("iscsi must be rejected")
	}
}
