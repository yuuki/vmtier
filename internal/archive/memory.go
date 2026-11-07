package archive

import "sync/atomic"

// MemoryStats observes explicitly retained archive components. These values
// exclude codec/JSON scratch, allocator/GC overhead and SQLite; they are not RSS.
// A service owns one instance and archives one stream window at a time.
type MemoryStats struct {
	verifier, verifierPeak, pack, packPeak atomic.Int64
}

type MemorySnapshot struct {
	VerifierBytes, VerifierPeakBytes, PackBytes, PackPeakBytes int64
}

func (m *MemoryStats) Snapshot() MemorySnapshot {
	if m == nil {
		return MemorySnapshot{}
	}
	return MemorySnapshot{m.verifier.Load(), m.verifierPeak.Load(), m.pack.Load(), m.packPeak.Load()}
}

func memoryPeak(dst *atomic.Int64, n int64) {
	for old := dst.Load(); old < n; old = dst.Load() {
		if dst.CompareAndSwap(old, n) {
			return
		}
	}
}

func (m *MemoryStats) setVerifier(n int64) {
	if m != nil {
		m.verifier.Store(n)
		memoryPeak(&m.verifierPeak, n)
	}
}

func (m *MemoryStats) setPack(n int64) {
	if m != nil {
		m.pack.Store(n)
		memoryPeak(&m.packPeak, n)
	}
}
