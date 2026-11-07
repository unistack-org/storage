// Package meta is the cluster metadata store: OSD registry, volume specs
// and crush-map epochs.
//
// MVP implementation is a directory of JSON files with atomic renames and
// polling watch - enough to run a real multi-process cluster on one host
// (and a shared filesystem). The Store interface is deliberately etcd
// shaped (keys, revisions, compare-and-swap, watch) so a distributed
// backend can replace it without touching callers.
package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Errors returned by Store implementations.
var (
	// ErrNotFound is returned by Get/Delete for missing keys.
	ErrNotFound = errors.New("meta: not found")
	// ErrConflict is returned by Put/Delete when a compare-and-swap
	// revision does not match.
	ErrConflict = errors.New("meta: revision conflict")
	// ErrClosed is returned after the store is closed.
	ErrClosed = errors.New("meta: closed")
)

// Event is one change notification from Watch.
type Event struct {
	Type string // "put" or "delete"
	Key  string // key (without prefix)
	Rev  int64
}

// Watcher delivers change notifications for a prefix.
type Watcher interface {
	// Events returns a channel closed when the watcher stops.
	Events() <-chan Event
	// Stop ends the watch.
	Stop()
}

// Store is the metadata plane contract. Keys are namespaced by prefix
// (a directory segment), values are opaque JSON blobs.
type Store interface {
	// Get returns the value and its revision.
	Get(ctx context.Context, prefix, key string) (value []byte, rev int64, err error)
	// Put stores a value. When casRev >= 0 the put only applies if the
	// current revision equals casRev (0 for a nonexistent key). Returns
	// the new revision.
	Put(ctx context.Context, prefix, key string, value []byte, casRev int64) (rev int64, err error)
	// Delete removes a key. casRev works as in Put (-1 = unconditional).
	Delete(ctx context.Context, prefix, key string, casRev int64) error
	// List returns sorted keys under a prefix.
	List(ctx context.Context, prefix string) (keys []string, err error)
	// Watch notifies callers about changes under a prefix.
	Watch(ctx context.Context, prefix string) (Watcher, error)
	// Close releases resources.
	Close() error
}

// record is the on-disk envelope: a revision counter next to the value.
type record struct {
	Rev   int64  `json:"rev"`
	Value []byte `json:"value"`
}

// DirStore is a file-based Store: <dir>/<prefix>/<key>.json files,
// written via tmp-file + rename. Suitable for single-host clusters and
// tests; see the package doc for the etcd upgrade path.
type DirStore struct {
	dir string

	mu     sync.Mutex
	watch  map[*dirWatcher]struct{}
	closed bool

	// PollInterval is how often watchers scan the prefix directory.
	PollInterval time.Duration
}

var _ Store = (*DirStore)(nil)

// OpenDir opens (creating if needed) a directory-backed metadata store.
func OpenDir(dir string) (*DirStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &DirStore{dir: dir, watch: map[*dirWatcher]struct{}{}, PollInterval: 250 * time.Millisecond}, nil
}

func (s *DirStore) path(prefix, key string) string {
	return filepath.Join(s.dir, prefix, key+".json")
}

// Get implements Store.
func (s *DirStore) Get(_ context.Context, prefix, key string) ([]byte, int64, error) {
	b, err := os.ReadFile(s.path(prefix, key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	var r record
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, 0, fmt.Errorf("meta: corrupt record %s/%s: %w", prefix, key, err)
	}
	return r.Value, r.Rev, nil
}

// Put implements Store.
func (s *DirStore) Put(_ context.Context, prefix, key string, value []byte, casRev int64) (int64, error) {
	if err := os.MkdirAll(filepath.Join(s.dir, prefix), 0o755); err != nil {
		return 0, err
	}
	p := s.path(prefix, key)
	var next int64 = 1
	if cur, err := os.ReadFile(p); err == nil {
		var r record
		if err := json.Unmarshal(cur, &r); err != nil {
			return 0, fmt.Errorf("meta: corrupt record %s/%s: %w", prefix, key, err)
		}
		next = r.Rev + 1
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if casRev >= 0 && next-1 != casRev {
		return 0, fmt.Errorf("%w: have %d want %d", ErrConflict, next-1, casRev)
	}
	b, err := json.Marshal(record{Rev: next, Value: value})
	if err != nil {
		return 0, err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	s.notify(prefix, key, "put", next)
	return next, nil
}

// Delete implements Store.
func (s *DirStore) Delete(_ context.Context, prefix, key string, casRev int64) error {
	p := s.path(prefix, key)
	if casRev >= 0 {
		cur, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var r record
		if err := json.Unmarshal(cur, &r); err != nil {
			return err
		}
		if r.Rev != casRev {
			return fmt.Errorf("%w: have %d want %d", ErrConflict, r.Rev, casRev)
		}
	}
	if err := os.Remove(p); errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	s.notify(prefix, key, "delete", 0)
	return nil
}

// List implements Store.
func (s *DirStore) List(_ context.Context, prefix string) ([]string, error) {
	des, err := os.ReadDir(filepath.Join(s.dir, prefix))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, de := range des {
		n := de.Name()
		if de.IsDir() || !strings.HasSuffix(n, ".json") {
			continue
		}
		keys = append(keys, strings.TrimSuffix(n, ".json"))
	}
	sort.Strings(keys)
	return keys, nil
}

// dirWatcher polls a prefix directory and emits per-key change events.
type dirWatcher struct {
	prefix string
	events chan Event
	stop   chan struct{}
	once   sync.Once

	mu  sync.Mutex
	rev map[string]int64 // last seen revision per key
}

// Watch implements Store.
func (s *DirStore) Watch(_ context.Context, prefix string) (Watcher, error) {
	w := &dirWatcher{
		prefix: prefix,
		events: make(chan Event, 64),
		stop:   make(chan struct{}),
		rev:    map[string]int64{},
	}
	// Prime the baseline so the first poll only reports real changes.
	w.scan(s)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	s.watch[w] = struct{}{}
	s.mu.Unlock()
	go s.poll(w)
	return w, nil
}

func (s *DirStore) poll(w *dirWatcher) {
	t := time.NewTicker(s.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-t.C:
			w.scan(s)
		}
	}
}

func (w *dirWatcher) scan(s *DirStore) {
	keys, err := s.List(context.Background(), w.prefix)
	if err != nil {
		return
	}
	seen := make(map[string]int64, len(keys))
	for _, k := range keys {
		_, rev, err := s.Get(context.Background(), w.prefix, k)
		if err != nil {
			continue
		}
		seen[k] = rev
		w.mu.Lock()
		prev, had := w.rev[k]
		w.mu.Unlock()
		if !had || prev != rev {
			w.emit(Event{Type: "put", Key: k, Rev: rev})
		}
	}
	w.mu.Lock()
	var deleted []string
	for k := range w.rev {
		if _, ok := seen[k]; !ok {
			deleted = append(deleted, k)
		}
	}
	w.rev = seen
	w.mu.Unlock()
	for _, k := range deleted {
		w.emit(Event{Type: "delete", Key: k})
	}
}

// emit records the new revision and delivers the event without holding
// the watcher lock (so a full buffer cannot deadlock a notify caller).
func (w *dirWatcher) emit(e Event) {
	w.mu.Lock()
	if e.Type == "put" {
		w.rev[e.Key] = e.Rev
	} else {
		delete(w.rev, e.Key)
	}
	w.mu.Unlock()
	select {
	case w.events <- e:
	case <-w.stop:
	}
}

// Events implements Watcher.
func (w *dirWatcher) Events() <-chan Event { return w.events }

// Stop implements Watcher.
func (w *dirWatcher) Stop() {
	w.once.Do(func() { close(w.stop); close(w.events) })
}

// notify lets in-process watchers learn about a change immediately,
// without waiting for the next poll.
func (s *DirStore) notify(prefix, key, typ string, rev int64) {
	s.mu.Lock()
	ws := make([]*dirWatcher, 0, len(s.watch))
	for w := range s.watch {
		ws = append(ws, w)
	}
	s.mu.Unlock()
	for _, w := range ws {
		if w.prefix == prefix {
			w.emit(Event{Type: typ, Key: key, Rev: rev})
		}
	}
}

// Close implements Store.
func (s *DirStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for w := range s.watch {
		w.Stop()
	}
	s.watch = nil
	return nil
}
