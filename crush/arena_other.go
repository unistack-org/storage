//go:build !unix

package crush

// Portable fallback (Windows, plan9, js/wasm): blocks come from the Go heap.
// Semantics are identical; only the GC-invisibility is lost.
var arenaPageSize = 4096

func arenaMmap(size int) []byte { return make([]byte, size) }

func arenaMunmap(b []byte) {}
