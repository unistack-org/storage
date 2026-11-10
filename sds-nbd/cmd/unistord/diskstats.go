// Disk stats registry: the OSDs hosted in this process publish their
// per-disk accounting (bytes stored, filesystem free/total) here so the
// admin /v1/status can report the storage polygon's capacity locally,
// without a network round-trip to each OSD.
//
// In a multi-process deployment (one unistord per OSD, shared meta
// directory) a process only sees the OSDs it hosts; status then reports
// disk stats for those and stays silent about remote ones — placement
// and liveness for remote OSDs come from meta and probes and are always
// complete.

package main

import (
	"sort"
	"sync"

	"github.com/unistack-org/storage/pkg/osd"
)

// diskRegistry maps OSD id -> the daemon hosted in this process.
type diskRegistry struct {
	mu sync.Mutex
	m  map[int32]*osd.Daemon
}

func newDiskRegistry() *diskRegistry {
	return &diskRegistry{m: map[int32]*osd.Daemon{}}
}

func (r *diskRegistry) add(id int32, d *osd.Daemon) {
	r.mu.Lock()
	r.m[id] = d
	r.mu.Unlock()
}

// disk returns the daemon hosted here for one OSD id (nil = not local).
func (r *diskRegistry) disk(id int32) *osd.Daemon {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[id]
}

// stats returns per-OSD disk stats for the OSDs hosted here, sorted by id.
func (r *diskRegistry) stats() []diskStatView {
	r.mu.Lock()
	ids := make([]int, 0, len(r.m))
	for id := range r.m {
		ids = append(ids, int(id))
	}
	r.mu.Unlock()
	sort.Ints(ids)
	var out []diskStatView
	for _, id := range ids {
		r.mu.Lock()
		d := r.m[int32(id)]
		r.mu.Unlock()
		if d == nil {
			continue
		}
		usage, free, total, ok := d.DiskStats()
		ws := d.WriteStats()
		metaObjects, metaRAM := d.MetaInfo()
		out = append(out, diskStatView{
			OSD: int32(id), Usage: usage, Free: free, Total: total, OK: ok,
			Writes: writeStatView{
				ClientBytes:  ws.ClientBytes,
				JournalBytes: ws.JournalBytes,
				JournalSyncs: ws.JournalSyncs,
				FlattenBytes: ws.FlattenBytes,
				FillBytes:    ws.FillBytes,
				PunchBytes:   ws.PunchBytes,
			},
			MetaObjects: metaObjects,
			MetaRAM:     metaRAM,
		})
		if lb, pend, jfree, jtotal, jok := d.JournalStats(); jok {
			out[len(out)-1].Journal = &journalStatView{
				LogBytes: lb, Pending: pend, Free: jfree, Total: jtotal,
			}
		}
	}
	return out
}
