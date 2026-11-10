// Durable records for the ingest/export control plane: import jobs
// (so a daemon restart no longer drops job state, and a transfer can
// be resumed from its persisted offset), per-volume import locks (two
// concurrent ingests writing one volume would interleave), and runtime
// export declarations (so NBD/VDUSE/vub exports added over the admin
// API are reproduced when the daemon restarts).
//
// All three ride the same meta Store (dir-backed or raft-replicated)
// as volume records, so the state is cluster-visible, not local to one
// daemon's memory.
package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Canonical key prefixes for ingest control records.
const (
	// PrefixImportJobs holds async ingest job records;
	// key: job id, value: ImportJobRecord.
	PrefixImportJobs = "importjobs"
	// PrefixImportLocks holds the per-volume ingest lock;
	// key: volume name, value: ImportLock.
	PrefixImportLocks = "importlocks"
	// PrefixExports holds runtime export declarations;
	// key: "<type>:<name>", value: ExportRecord.
	PrefixExports = "exports"
)

// Import job states.
const (
	JobStateRunning   = "running"
	JobStateCompleted = "completed"
	JobStateFailed    = "failed"
)

// ImportJobRecord is one (async) ingest job. The daemon mirrors its
// live progress in memory and persists the record (throttled), so:
// GET /v1/jobs/{id} survives a restart, and a "running" record whose
// owning daemon is back booting is resumed from Offset+Written.
type ImportJobRecord struct {
	ID     string `json:"id"`
	Volume string `json:"volume"`
	Type   string `json:"type"` // "import_url"
	State  string `json:"state"`
	// URL is the source to fetch (resume needs it; body imports have none).
	URL string `json:"url,omitempty"`
	// Offset is the volume offset the ingest started at.
	Offset int64 `json:"offset"`
	// Size is the expected total bytes of this ingest (0 = unknown).
	// Completion is only reported when Written reaches Size.
	Size int64 `json:"size,omitempty"`
	// SHA256 is the hex checksum of the source bytes, verified over
	// exactly the bytes written (on resume: recomputed by re-reading
	// the volume range).
	SHA256 string `json:"sha256,omitempty"`
	// Written is the bytes landed since Offset.
	Written int64 `json:"written"`
	// Error is set on a failed job.
	Error string `json:"error,omitempty"`
	// Owner identifies the daemon running the job ("host:admin-addr");
	// boot resume only touches records owned by this daemon.
	Owner string `json:"owner,omitempty"`
	// Resumed counts boot-resume restarts (bounded retries).
	Resumed int `json:"resumed,omitempty"`
	// CreatedAt/FinishedAt are unix seconds.
	CreatedAt  int64 `json:"created_at"`
	FinishedAt int64 `json:"finished_at,omitempty"`
}

// Total returns the bytes the job reports for progress (the expected
// size when known).
func (j *ImportJobRecord) Total() int64 {
	if j.Size > 0 {
		return j.Size
	}
	return 0
}

func importJobKey(id string) string { return id }

func PutImportJob(ctx context.Context, s Store, rec ImportJobRecord) (int64, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixImportJobs, importJobKey(rec.ID), b, -1)
}

// PutImportJobCAS stores the record only if the current revision is
// casRev: a daemon resuming a job it believes it owns fails the CAS
// when another writer (the boot resume, a late original) moved it.
func PutImportJobCAS(ctx context.Context, s Store, rec ImportJobRecord, casRev int64) (int64, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixImportJobs, importJobKey(rec.ID), b, casRev)
}

// UpdateImportJob read-modify-writes a job record under CAS (same
// merge semantics as MutateVolume): mutate returns false to keep the
// record untouched.
func UpdateImportJob(ctx context.Context, s Store, id string, mutate func(*ImportJobRecord) bool) (ImportJobRecord, error) {
	for {
		b, rev, err := s.Get(ctx, PrefixImportJobs, importJobKey(id))
		if err != nil {
			return ImportJobRecord{}, err
		}
		var rec ImportJobRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return ImportJobRecord{}, fmt.Errorf("meta: import job %s: %w", id, err)
		}
		if !mutate(&rec) {
			return rec, nil
		}
		nb, err := json.Marshal(rec)
		if err != nil {
			return ImportJobRecord{}, err
		}
		if _, err := s.Put(ctx, PrefixImportJobs, importJobKey(id), nb, rev); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return ImportJobRecord{}, err
		}
		return rec, nil
	}
}

func GetImportJob(ctx context.Context, s Store, id string) (ImportJobRecord, error) {
	b, _, err := s.Get(ctx, PrefixImportJobs, importJobKey(id))
	if err != nil {
		return ImportJobRecord{}, err
	}
	var rec ImportJobRecord
	return rec, json.Unmarshal(b, &rec)
}

func DeleteImportJob(ctx context.Context, s Store, id string) error {
	return s.Delete(ctx, PrefixImportJobs, importJobKey(id), -1)
}

// ListImportJobs returns every job record sorted by key.
func ListImportJobs(ctx context.Context, s Store) ([]ImportJobRecord, error) {
	keys, err := s.List(ctx, PrefixImportJobs)
	if err != nil {
		return nil, err
	}
	out := make([]ImportJobRecord, 0, len(keys))
	for _, k := range keys {
		b, _, err := s.Get(ctx, PrefixImportJobs, k)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		var rec ImportJobRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("meta: import job %s: %w", k, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

// ImportLock is the per-volume ingest lock: one writer per volume at a
// time, cluster-visible. The holder refreshes Heartbeat (unix seconds)
// while it runs; a lock whose heartbeat is older than the staleness
// window is considered dead and can be taken over.
type ImportLock struct {
	JobID     string `json:"job_id"`
	Owner     string `json:"owner"`
	Heartbeat int64  `json:"heartbeat"`
}

// AcquireImportLock takes the volume's ingest lock. It reports
// acquired=false with the current holder when a fresh lock exists
// (caller answers 409). A stale lock (dead holder) is taken over.
func AcquireImportLock(ctx context.Context, s Store, volume string, lock ImportLock) (acquired bool, cur ImportLock, err error) {
	b, rev, err := s.Get(ctx, PrefixImportLocks, volume)
	if errors.Is(err, ErrNotFound) {
		nb, merr := json.Marshal(lock)
		if merr != nil {
			return false, cur, merr
		}
		if _, perr := s.Put(ctx, PrefixImportLocks, volume, nb, 0); perr != nil {
			if errors.Is(perr, ErrConflict) {
				return false, cur, nil // raced another acquirer
			}
			return false, cur, perr
		}
		return true, lock, nil
	}
	if err != nil {
		return false, cur, err
	}
	if uerr := json.Unmarshal(b, &cur); uerr != nil {
		return false, cur, uerr
	}
	nb, merr := json.Marshal(lock)
	if merr != nil {
		return false, cur, merr
	}
	if _, perr := s.Put(ctx, PrefixImportLocks, volume, nb, rev); perr != nil {
		if errors.Is(perr, ErrConflict) {
			return false, cur, nil
		}
		return false, cur, perr
	}
	return true, lock, nil
}

// TakeoverImportLock replaces the volume lock when the current holder
// is older than staleSeconds (a dead daemon). It refuses a fresh lock.
func TakeoverImportLock(ctx context.Context, s Store, volume string, lock ImportLock, now, staleSeconds int64) (acquired bool, cur ImportLock, err error) {
	b, rev, err := s.Get(ctx, PrefixImportLocks, volume)
	if errors.Is(err, ErrNotFound) {
		return AcquireImportLock(ctx, s, volume, lock)
	}
	if err != nil {
		return false, cur, err
	}
	if uerr := json.Unmarshal(b, &cur); uerr != nil {
		return false, cur, uerr
	}
	if cur.JobID == lock.JobID {
		// Our own lock (job restart for resume): refresh it.
		nb, merr := json.Marshal(lock)
		if merr != nil {
			return false, cur, merr
		}
		if _, perr := s.Put(ctx, PrefixImportLocks, volume, nb, rev); perr != nil {
			if errors.Is(perr, ErrConflict) {
				return false, cur, nil
			}
			return false, cur, perr
		}
		return true, lock, nil
	}
	if now-cur.Heartbeat < staleSeconds {
		return false, cur, nil
	}
	nb, merr := json.Marshal(lock)
	if merr != nil {
		return false, cur, merr
	}
	if _, perr := s.Put(ctx, PrefixImportLocks, volume, nb, rev); perr != nil {
		if errors.Is(perr, ErrConflict) {
			return false, cur, nil
		}
		return false, cur, perr
	}
	return true, lock, nil
}

// RefreshImportLock updates the heartbeat of the lock the job holds
// (no-op when the lock is gone or held by someone else).
func RefreshImportLock(ctx context.Context, s Store, volume string, jobID, owner string, now int64) error {
	b, rev, err := s.Get(ctx, PrefixImportLocks, volume)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	var cur ImportLock
	if uerr := json.Unmarshal(b, &cur); uerr != nil {
		return uerr
	}
	if cur.JobID != jobID || cur.Owner != owner {
		return nil
	}
	cur.Heartbeat = now
	nb, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	_, err = s.Put(ctx, PrefixImportLocks, volume, nb, rev)
	if errors.Is(err, ErrConflict) {
		return nil // lost the lock: the next refresh sees the new holder
	}
	return err
}

// ReleaseImportLock removes the volume lock when the job still owns it.
func ReleaseImportLock(ctx context.Context, s Store, volume, jobID string) error {
	b, rev, err := s.Get(ctx, PrefixImportLocks, volume)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var cur ImportLock
	if uerr := json.Unmarshal(b, &cur); uerr != nil || cur.JobID != jobID {
		return uerr
	}
	return s.Delete(ctx, PrefixImportLocks, volume, rev)
}

// ExportRecord declares one runtime export (added over the admin API).
// The daemon replays these at boot, so exports of live VMs are
// reproduced after a restart instead of silently vanishing.
type ExportRecord struct {
	Type   string `json:"type"` // "nbd" | "vduse" | "vub"
	Name   string `json:"name"` // nbd/vub: export name; vduse: devname
	Volume string `json:"volume"`
	// ReadOnly is kept in sync with the volume's read_only flag (that
	// flag remains the source of truth; this copy documents intent).
	ReadOnly  bool  `json:"read_only,omitempty"`
	CreatedAt int64 `json:"created_at"`
}

// ExportKey builds the record key for a type+name pair. ":" is not a
// legal character in export names, devnames, or volume names, so the
// pair encodes unambiguously.
func ExportKey(exportType, name string) string {
	return exportType + ":" + name
}

func PutExportRecord(ctx context.Context, s Store, rec ExportRecord) (int64, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixExports, ExportKey(rec.Type, rec.Name), b, -1)
}

func GetExportRecord(ctx context.Context, s Store, exportType, name string) (ExportRecord, error) {
	b, _, err := s.Get(ctx, PrefixExports, ExportKey(exportType, name))
	if err != nil {
		return ExportRecord{}, err
	}
	var rec ExportRecord
	return rec, json.Unmarshal(b, &rec)
}

func DeleteExportRecord(ctx context.Context, s Store, exportType, name string) error {
	return s.Delete(ctx, PrefixExports, ExportKey(exportType, name), -1)
}

// ListExportRecords returns every declared export (sorted by key).
func ListExportRecords(ctx context.Context, s Store) ([]ExportRecord, error) {
	keys, err := s.List(ctx, PrefixExports)
	if err != nil {
		return nil, err
	}
	out := make([]ExportRecord, 0, len(keys))
	for _, k := range keys {
		b, _, err := s.Get(ctx, PrefixExports, k)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		var rec ExportRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("meta: export %s: %w", k, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

// ParseExportKey splits an ExportKey back into (type, name).
func ParseExportKey(key string) (exportType, name string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == ':' {
			return key[:i], key[i+1:], i > 0 && i+1 < len(key)
		}
	}
	return "", "", false
}

// ImportStaleSeconds is the import-lock staleness window: a lock whose
// heartbeat is older is considered dead and may be taken over.
const ImportStaleSeconds = 60

// Itoa is a tiny helper for record keys built from numbers.
func Itoa(n int64) string { return strconv.FormatInt(n, 10) }
