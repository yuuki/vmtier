package cold

import "sync/atomic"

// WorkSnapshot separates logical reader work from process memory. Totals count
// charged bytes across completed searches (including failed searches); peaks
// are per-search high-water marks, not concurrent heap measurements.
type WorkSnapshot struct {
	ReadBytes, DecodedBytes, ResultEstimateBytes uint64
	IndexEstimatePeakBytes, PeakBytes            uint64
}

type workCounters struct {
	read, decoded, result, indexPeak, peak atomic.Uint64
}

type queryWork struct {
	read, decoded, result, index, indexPeak, peak int64
}

func (w *queryWork) used() int64 { return w.read + w.decoded + w.result + w.index }

func (w *queryWork) observe() {
	w.peak = max(w.peak, w.used())
	w.indexPeak = max(w.indexPeak, w.index)
}

func storePeak(dst *atomic.Uint64, n uint64) {
	for old := dst.Load(); old < n; old = dst.Load() {
		if dst.CompareAndSwap(old, n) {
			return
		}
	}
}

func (r *Reader) recordWork(w *queryWork) {
	r.work.read.Add(uint64(w.read))
	r.work.decoded.Add(uint64(w.decoded))
	r.work.result.Add(uint64(w.result))
	storePeak(&r.work.indexPeak, uint64(w.indexPeak))
	storePeak(&r.work.peak, uint64(w.peak))
}

func (r *Reader) WorkStats() WorkSnapshot {
	return WorkSnapshot{r.work.read.Load(), r.work.decoded.Load(), r.work.result.Load(), r.work.indexPeak.Load(), r.work.peak.Load()}
}
