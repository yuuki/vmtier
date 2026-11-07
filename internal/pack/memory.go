package pack

// IndexMemoryEstimate estimates retained parsed-directory work after validation.
// It charges structural allowances, all label/name/checksum bytes and slice
// capacities. It excludes JSON decoder scratch, allocator size classes and GC;
// callers must not present this estimate as measured heap or RSS.
func IndexMemoryEstimate(idx *Index) int64 {
	if idx == nil {
		return 0
	}
	n := int64(256 + cap(idx.Series)*128)
	for _, s := range idx.Series {
		n += int64(cap(s.NativeName)) + 128 + int64(len(s.Labels))*96
		for k, v := range s.Labels {
			n += int64(len(k) + len(v))
		}
		n += int64(cap(s.Blocks)) * 128
		for _, b := range s.Blocks {
			n += int64(len(b.SHA256))
		}
	}
	return n
}
