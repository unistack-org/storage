package store

// Test-only applier freeze. pauseApplier stops the background flattener
// from applying on its ticks, so a staged record provably stays in the
// overlay for tests that inspect overlay-visible state (Stat/StatInfo/
// ObjectState). Drain passes — Sync, Remove, Close — deliberately ignore
// the freeze: they are checkpoints and must always terminate.

func (s *Store) pauseApplier() {
	if s.jrn == nil {
		return
	}
	s.jrn.mu.Lock()
	s.jrn.paused = true
	s.jrn.mu.Unlock()
}

func (s *Store) resumeApplier() {
	if s.jrn == nil {
		return
	}
	s.jrn.mu.Lock()
	s.jrn.paused = false
	s.jrn.mu.Unlock()
}
