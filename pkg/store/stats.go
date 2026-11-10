// Write-activity counters (write-amplification observability, ideas §2.3).
//
// The store counts, per process lifetime:
//
//	ClientBytes  — bytes of guest data handed to WriteAt (logical writes,
//	               before journaling/flattening).
//	JournalBytes — bytes appended to the write-ahead log (record headers
//	               included): the real device traffic journal mode adds.
//	JournalSyncs — fsyncs of the log (one per acknowledged write today).
//	FlattenBytes — bytes the journal applier wrote into object files
//	               (the second, flattening pass over the same data).
//
// Write amplification of the write path is read off these counters:
// journal-mode WA = (JournalBytes + FlattenBytes) / ClientBytes, i.e.
// how many bytes hit the device per byte the guest wrote. A perfect
// journal that packs records into 4 KiB blocks and flattens once sits
// slightly above 1; the unpacked per-record layout costs ~1.1 for 4 KiB
// random writes (21 B header + padding to record boundaries), and a
// torn (double) write shows up as ~2.
//
// The counters are cumulative and monotonic: a delta between two
// snapshots (the unistord admin API /v1/status, the Prometheus gauges,
// sdsbench's before/after query) is what any ratio is computed from.

package store

// WriteStats is a snapshot of the store's write-activity counters.
type WriteStats struct {
	ClientBytes  int64 `json:"client_bytes"`
	JournalBytes int64 `json:"journal_bytes"`
	JournalSyncs int64 `json:"journal_syncs"`
	FlattenBytes int64 `json:"flatten_bytes"`
	// FillBytes counts zero-fill writes (TRIM fallback, punch edges):
	// device writes that carry no guest data.
	FillBytes int64 `json:"fill_bytes"`
	// PunchBytes counts bytes of punches issued (TRIM/discard): not
	// device writes, tracked separately so a discard-heavy workload is
	// visible without polluting the write counters.
	PunchBytes int64 `json:"punch_bytes"`
}

// WriteStats returns the current cumulative counters.
func (s *Store) WriteStats() WriteStats {
	ws := WriteStats{
		ClientBytes:  s.clientBytes.Load(),
		JournalBytes: s.journalBytes.Load(),
		FlattenBytes: s.flattenBytes.Load(),
		FillBytes:    s.fillBytes.Load(),
		PunchBytes:   s.punchBytes.Load(),
	}
	if s.jrn != nil {
		ws.JournalSyncs = s.jrn.syncs.Load()
	}
	return ws
}
