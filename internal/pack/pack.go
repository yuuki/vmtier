// Package pack stores immutable, bounded collections of VM portable blocks.
// Its codec is deliberately pinned; it is not a general native-export decoder.
package pack

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/klauspost/compress/zstd"
)

const (
	FormatVersion        = 1
	VMVersion            = "v1.155.0-cluster"
	VMRevision           = "cab3d5fbeeedd5d0374c1dabde93939af4c2499c"
	maxRows              = 16384 // pinned VM blockHeader.validate bound
	maxDecodedBlockBytes = 1 << 20
)

type Tenant struct {
	AccountID uint32 `json:"account_id"`
	ProjectID uint32 `json:"project_id"`
}

func (t Tenant) String() string { return fmt.Sprintf("%d:%d", t.AccountID, t.ProjectID) }

func ParseTenant(s string) (Tenant, error) {
	p := strings.Split(s, ":")
	if len(p) < 1 || len(p) > 2 {
		return Tenant{}, fmt.Errorf("invalid tenant %q", s)
	}
	a, err := strconv.ParseUint(p[0], 10, 32)
	if err != nil {
		return Tenant{}, fmt.Errorf("invalid account: %w", err)
	}
	var b uint64
	if len(p) == 2 {
		b, err = strconv.ParseUint(p[1], 10, 32)
		if err != nil {
			return Tenant{}, fmt.Errorf("invalid project: %w", err)
		}
	}
	return Tenant{uint32(a), uint32(b)}, nil
}

// Limits bounds memory per pack, not the total size of a historical archive.
// BuildPacks rotates between complete records; whole-window input and sample
// bounds still apply. Window width cannot change in an existing catalog.
type Limits struct {
	MaxPackBytes       int64
	MaxIndexBytes      int64
	MaxInputBytes      int64
	MaxSeries          int
	MaxBlocks          int
	MaxBlockBytes      int64
	MaxMetricNameBytes int64
}

func DefaultLimits() Limits {
	return Limits{64 << 20, 16 << 20, 128 << 20, 10000, 100000, 512 << 10, 64 << 10}
}

func (l Limits) normalized() (Limits, error) {
	d := DefaultLimits()
	if l.MaxPackBytes == 0 {
		l.MaxPackBytes = d.MaxPackBytes
	}
	if l.MaxIndexBytes == 0 {
		l.MaxIndexBytes = d.MaxIndexBytes
	}
	if l.MaxInputBytes == 0 {
		l.MaxInputBytes = d.MaxInputBytes
	}
	if l.MaxSeries == 0 {
		l.MaxSeries = d.MaxSeries
	}
	if l.MaxBlocks == 0 {
		l.MaxBlocks = d.MaxBlocks
	}
	if l.MaxBlockBytes == 0 {
		l.MaxBlockBytes = d.MaxBlockBytes
	}
	if l.MaxMetricNameBytes == 0 {
		l.MaxMetricNameBytes = d.MaxMetricNameBytes
	}
	if l.MaxPackBytes < 1 || l.MaxPackBytes > 1<<30 || l.MaxIndexBytes < 1 || l.MaxIndexBytes > 64<<20 || l.MaxInputBytes < 16 || l.MaxInputBytes > 2<<30 || l.MaxSeries < 1 || l.MaxSeries > 100000 || l.MaxBlocks < 1 || l.MaxBlocks > 1000000 || l.MaxBlockBytes < 1 || l.MaxBlockBytes > 1<<20 || l.MaxMetricNameBytes < 1 || l.MaxMetricNameBytes > 1<<20 {
		return l, errors.New("pack limits exceed safe bounds or are negative")
	}
	return l, nil
}

type Index struct {
	Version    int      `json:"version"`
	VMVersion  string   `json:"vm_version"`
	VMRevision string   `json:"vm_revision"`
	Tenant     Tenant   `json:"tenant"`
	Start      int64    `json:"start_ms"` // inclusive logical coverage
	End        int64    `json:"end_ms"`   // exclusive logical coverage
	Size       int64    `json:"size"`
	SHA256     string   `json:"sha256"`
	Series     []Series `json:"series"`
}

type Series struct {
	Labels map[string]string `json:"labels"`
	// NativeName retains VM's tag ordering (job/instance keys have special
	// ordering). Sorting it lexically would split Hot/Cold wire identities.
	NativeName []byte     `json:"native_name"`
	Blocks     []BlockRef `json:"blocks"`
}

type BlockRef struct {
	Offset       int64  `json:"offset"`
	Length       int64  `json:"length"`
	MinTimestamp int64  `json:"min_timestamp"` // physical payload extent
	MaxTimestamp int64  `json:"max_timestamp"`
	Rows         int    `json:"rows"` // physical row count
	SHA256       string `json:"sha256"`
}

func Sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// Build consumes a complete native export. It requires the stream header to
// match the requested logical window exactly. Native blocks can extend beyond
// that window; the original bytes are retained and LoadBlock clips the rows.
// An EOF at a record boundary is valid native framing, not proof of source
// completeness. The caller must establish successful source completion.
func Build(r io.Reader, tenant Tenant, start, end int64, limits Limits) (*Index, []byte, error) {
	l, err := limits.normalized()
	if err != nil {
		return nil, nil, err
	}
	if start >= end {
		return nil, nil, errors.New("coverage must satisfy start < end")
	}
	lr := &io.LimitedReader{R: r, N: l.MaxInputBytes + 1}
	if err := readNativeHeader(lr, start, end); err != nil {
		return nil, nil, err
	}
	b := packBuilder{
		idx:       &Index{Version: FormatVersion, VMVersion: VMVersion, VMRevision: VMRevision, Tenant: tenant, Start: start, End: end, Series: []Series{}},
		seriesIDs: make(map[string]int),
		limits:    l,
	}
	for {
		mnBytes, payload, err := readNativeRecord(lr, l)
		if err == io.EOF { //nolint:errorlint // readNativeRecord returns a bare io.EOF only at a record boundary; anything wrapped is truncation.
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if err := b.add(mnBytes, payload); err != nil {
			return nil, nil, err
		}
	}
	if lr.N <= 0 {
		return nil, nil, errors.New("native export exceeds input limit")
	}
	b.idx.Size = int64(len(b.data))
	b.idx.SHA256 = Sum(b.data)
	if _, err := MarshalIndex(b.idx, l); err != nil {
		return nil, nil, err
	}
	return b.idx, b.data, nil
}

// readNativeHeader requires the native export header to declare exactly the
// requested logical window [start,end).
func readNativeHeader(r io.Reader, start, end int64) error {
	var header [16]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return fmt.Errorf("native header: %w", err)
	}
	if encoding.UnmarshalInt64(header[:]) != start || encoding.UnmarshalInt64(header[8:]) != end-1 {
		return errors.New("native export header differs from requested [start,end)")
	}
	return nil
}

// readNativeRecord reads the metric name and block of the next native record.
// It returns io.EOF only at a record boundary.
func readNativeRecord(lr *io.LimitedReader, l Limits) (name, payload []byte, err error) {
	name, err = readFrame(lr, l.MaxMetricNameBytes, true)
	if err == io.EOF { //nolint:errorlint // readFrame returns a bare io.EOF only before a frame's first byte.
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, fmt.Errorf("native metric name: %w", err)
	}
	payload, err = readFrame(lr, l.MaxBlockBytes, false)
	if err != nil {
		return nil, nil, fmt.Errorf("native block: %w", err)
	}
	if lr.N <= 0 {
		return nil, nil, errors.New("native export exceeds input limit")
	}
	return name, payload, nil
}

// packBuilder accumulates the single pack of Build. Unlike rotatingBuilder, it
// fails instead of starting a new pack when a bound is exceeded.
type packBuilder struct {
	idx         *Index
	data        []byte
	seriesIDs   map[string]int
	blockCount  int
	indexBudget int64
	limits      Limits
}

// add appends one native record if it has rows in the logical window.
func (b *packBuilder) add(mnBytes, payload []byte) error {
	b.blockCount++
	if b.blockCount > b.limits.MaxBlocks {
		return errors.New("native export exceeds block limit")
	}
	pb, err := decodePortable(payload)
	if err != nil {
		return fmt.Errorf("portable block: %w", err)
	}
	start, end := b.idx.Start, b.idx.End
	if pb.ts[len(pb.ts)-1] < start || pb.ts[0] >= end {
		return nil
	}
	lo := sort.Search(len(pb.ts), func(i int) bool { return pb.ts[i] >= start })
	if lo == len(pb.ts) || pb.ts[lo] >= end {
		return nil
	}
	labels, err := nativeLabels(mnBytes)
	if err != nil {
		return err
	}
	canonical, err := json.Marshal(labels)
	if err != nil {
		return err
	}
	si, err := b.series(string(canonical), labels, mnBytes)
	if err != nil {
		return err
	}
	b.indexBudget += 256
	if b.indexBudget > b.limits.MaxIndexBytes {
		return errors.New("pack exceeds index budget")
	}
	if int64(len(b.data))+int64(len(payload)) > b.limits.MaxPackBytes {
		return errors.New("pack exceeds payload limit")
	}
	b.idx.Series[si].Blocks = append(b.idx.Series[si].Blocks, BlockRef{Offset: int64(len(b.data)), Length: int64(len(payload)), MinTimestamp: pb.ts[0], MaxTimestamp: pb.ts[len(pb.ts)-1], Rows: len(pb.ts), SHA256: Sum(payload)})
	b.data = append(b.data, payload...)
	return nil
}

// series returns the dictionary position of the series with the canonical
// label encoding key, adding it within the series limit if it is new.
func (b *packBuilder) series(key string, labels map[string]string, mnBytes []byte) (int, error) {
	si, ok := b.seriesIDs[key]
	if ok {
		if !bytes.Equal(b.idx.Series[si].NativeName, mnBytes) {
			return 0, errors.New("inconsistent native name ordering for identical labels")
		}
		return si, nil
	}
	if len(b.idx.Series) >= b.limits.MaxSeries {
		return 0, errors.New("pack exceeds series limit")
	}
	b.indexBudget += int64(len(key) + len(mnBytes)*2)
	si = len(b.idx.Series)
	b.seriesIDs[key] = si
	b.idx.Series = append(b.idx.Series, Series{Labels: labels, NativeName: append([]byte(nil), mnBytes...)})
	return si, nil
}

// nativeLabels decodes the labels of a native metric name, rejecting empty
// and duplicate keys.
func nativeLabels(mnBytes []byte) (map[string]string, error) {
	var mn storage.MetricName
	if err := mn.UnmarshalNoAccountIDProjectID(mnBytes); err != nil {
		return nil, fmt.Errorf("metric name: %w", err)
	}
	labels := map[string]string{"__name__": string(mn.MetricGroup)}
	for _, tag := range mn.Tags {
		k, v := string(tag.Key), string(tag.Value)
		if k == "" {
			return nil, errors.New("empty label key in native metric name")
		}
		if _, exists := labels[k]; exists {
			return nil, errors.New("duplicate label key in native metric name")
		}
		labels[k] = v
	}
	return labels, nil
}

func readFrame(r io.Reader, max int64, eofOK bool) ([]byte, error) {
	var b [4]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		if err == io.EOF && !eofOK {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	n := int64(encoding.UnmarshalUint32(b[:]))
	if n < 1 || n > max {
		return nil, fmt.Errorf("frame length %d exceeds bound %d", n, max)
	}
	data := make([]byte, int(n))
	_, err = io.ReadFull(r, data)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return data, err
}

func MarshalIndex(idx *Index, limits Limits) ([]byte, error) {
	l, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	if err := ValidateIndex(idx, l); err != nil {
		return nil, err
	}
	b, err := json.Marshal(idx)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > l.MaxIndexBytes {
		return nil, errors.New("index exceeds byte limit")
	}
	return b, nil
}

func ParseIndex(data []byte, limits Limits) (*Index, error) {
	l, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > l.MaxIndexBytes {
		return nil, errors.New("index exceeds byte limit")
	}
	var idx Index
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&idx); err != nil {
		return nil, fmt.Errorf("decode pack index: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, errors.New("trailing data in pack index")
	}
	if err := ValidateIndex(&idx, l); err != nil {
		return nil, err
	}
	return &idx, nil
}

func validSum(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == sha256.Size && s == strings.ToLower(s)
}

func ValidateIndex(idx *Index, limits Limits) error {
	l, err := limits.normalized()
	if err != nil {
		return err
	}
	if idx == nil {
		return errors.New("nil pack index")
	}
	if idx.Version != FormatVersion || idx.VMVersion != VMVersion || idx.VMRevision != VMRevision {
		return errors.New("unsupported pack format or VM codec revision")
	}
	if idx.Start >= idx.End || idx.Size < 0 || idx.Size > l.MaxPackBytes || !validSum(idx.SHA256) {
		return errors.New("invalid pack coverage, size or checksum")
	}
	if len(idx.Series) > l.MaxSeries {
		return errors.New("index exceeds series limit")
	}
	seen := make(map[string]bool, len(idx.Series))
	var blocks []BlockRef
	var budget int64
	for _, s := range idx.Series {
		lb, err := validateSeries(s, idx.Tenant, l)
		if err != nil {
			return err
		}
		if seen[string(lb)] {
			return errors.New("duplicate series dictionary entry")
		}
		seen[string(lb)] = true
		budget += int64(len(lb)+len(s.NativeName)*2) + int64(len(s.Blocks))*256
		if budget > l.MaxIndexBytes {
			return errors.New("index exceeds memory budget")
		}
		if len(s.Blocks) == 0 || len(blocks)+len(s.Blocks) > l.MaxBlocks {
			return errors.New("invalid block count")
		}
		for _, b := range s.Blocks {
			if !validBlockRef(b, idx, l) {
				return errors.New("invalid block directory entry")
			}
			blocks = append(blocks, b)
		}
	}
	return validateBlockLayout(blocks, idx.Size)
}

// validateSeries checks one series dictionary entry against its native name
// and returns the JSON encoding of its labels.
func validateSeries(s Series, tenant Tenant, l Limits) ([]byte, error) {
	if int64(len(s.NativeName)) > l.MaxMetricNameBytes {
		return nil, errors.New("native name exceeds name limit")
	}
	if _, err := s.metricName(tenant); err != nil {
		return nil, err
	}
	if len(s.Labels) == 0 {
		return nil, errors.New("empty series labels")
	}
	if _, ok := s.Labels["__name__"]; !ok {
		return nil, errors.New("series missing metric name")
	}
	for k, v := range s.Labels {
		if k == "" {
			return nil, errors.New("empty label key")
		}
		// The index stores labels as JSON, which replaces invalid UTF-8 with
		// U+FFFD; the parsed labels would no longer match NativeName.
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return nil, errors.New("series label is not valid UTF-8; pack indexes store labels as JSON")
		}
	}
	lb, _ := json.Marshal(s.Labels)
	if int64(len(lb)) > l.MaxMetricNameBytes*6 {
		return nil, errors.New("series labels exceed name limit")
	}
	return lb, nil
}

// validBlockRef reports whether a block lies within the pack, has a bounded
// row count and physical extent that overlaps the logical window, and has a
// well-formed checksum.
func validBlockRef(b BlockRef, idx *Index, l Limits) bool {
	if b.Offset < 0 || b.Length < 1 || b.Length > l.MaxBlockBytes || b.Offset > idx.Size || b.Length > idx.Size-b.Offset {
		return false
	}
	if b.Rows < 1 || b.Rows > maxRows || b.MinTimestamp > b.MaxTimestamp || b.MaxTimestamp < idx.Start || b.MinTimestamp >= idx.End {
		return false
	}
	return validSum(b.SHA256)
}

// validateBlockLayout requires every byte of a pack of the given size to
// belong to exactly one block.
func validateBlockLayout(blocks []BlockRef, size int64) error {
	// Every byte belongs to exactly one block; reject aliasing, holes and tails.
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Offset < blocks[j].Offset })
	var end int64
	for _, b := range blocks {
		if b.Offset != end {
			return errors.New("non-contiguous or overlapping block offsets")
		}
		end += b.Length
	}
	if end != size {
		return errors.New("pack size differs from block directory")
	}
	return nil
}

// LoadBlock verifies a single Range GET and builds a VM wire-ready block.
// It clips to committed coverage, never merely to physical payload extents.
func LoadBlock(idx *Index, seriesIndex, blockIndex int, data []byte) (*storage.MetricBlock, error) {
	if idx == nil || idx.Version != FormatVersion || idx.VMVersion != VMVersion || idx.VMRevision != VMRevision || idx.Start >= idx.End {
		return nil, errors.New("unsupported or invalid index")
	}
	if seriesIndex < 0 || seriesIndex >= len(idx.Series) {
		return nil, errors.New("series index out of range")
	}
	s := idx.Series[seriesIndex]
	if blockIndex < 0 || blockIndex >= len(s.Blocks) {
		return nil, errors.New("block index out of range")
	}
	b := s.Blocks[blockIndex]
	if int64(len(data)) != b.Length || b.Length > 1<<20 || Sum(data) != b.SHA256 {
		return nil, errors.New("block length or checksum mismatch")
	}
	pb, err := decodePortable(data)
	if err != nil {
		return nil, err
	}
	if len(pb.ts) != b.Rows || pb.ts[0] != b.MinTimestamp || pb.ts[len(pb.ts)-1] != b.MaxTimestamp {
		return nil, errors.New("block directory disagrees with payload")
	}
	lo := sort.Search(len(pb.ts), func(i int) bool { return pb.ts[i] >= idx.Start })
	hi := sort.Search(len(pb.ts), func(i int) bool { return pb.ts[i] >= idx.End })
	if lo == hi {
		return nil, nil
	}
	mn, err := s.metricName(idx.Tenant)
	if err != nil {
		return nil, err
	}
	mb := &storage.MetricBlock{MetricName: mn.Marshal(nil)}
	// Source TSID/MetricID is intentionally discarded. The full canonical
	// metric name plus tenant is the wire series identity used by vmselect.
	mb.Block.Init(&storage.TSID{}, pb.ts[lo:hi], pb.values[lo:hi], pb.scale, 64)
	mb.Block.MarshalData(0, 0)
	return mb, nil
}

// MetricName builds a lexically ordered name for standalone fixture generation.
// Production reads MUST retain the source's VM-specific ordering via NativeName.
func MetricName(tenant Tenant, labels map[string]string) *storage.MetricName {
	mn := &storage.MetricName{AccountID: tenant.AccountID, ProjectID: tenant.ProjectID, MetricGroup: []byte(labels["__name__"])}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		if k != "__name__" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		mn.AddTag(k, labels[k])
	}
	return mn
}

func (s Series) metricName(tenant Tenant) (*storage.MetricName, error) {
	var mn storage.MetricName
	if err := mn.UnmarshalNoAccountIDProjectID(s.NativeName); err != nil {
		return nil, fmt.Errorf("native series name: %w", err)
	}
	if len(s.Labels) != len(mn.Tags)+1 || s.Labels["__name__"] != string(mn.MetricGroup) {
		return nil, errors.New("native name differs from label dictionary")
	}
	seen := map[string]bool{"__name__": true}
	for _, tag := range mn.Tags {
		k, v := string(tag.Key), string(tag.Value)
		if k == "" || seen[k] {
			return nil, errors.New("invalid native name keys")
		}
		seen[k] = true
		if sv, ok := s.Labels[k]; !ok || sv != v {
			return nil, errors.New("native name differs from label dictionary")
		}
	}
	mn.AccountID = tenant.AccountID
	mn.ProjectID = tenant.ProjectID
	return &mn, nil
}

type portable struct {
	ts, values []int64
	scale      int16
}

// decodePortable independently reads the small pinned portable header. Public
// VM integer decoders do the codec work; bounded zstd expansion is performed
// before those decoders because VM's general UnmarshalPortable has no byte cap.
func decodePortable(data []byte) (*portable, error) {
	if len(data) > 1<<20 {
		return nil, errors.New("portable block too large")
	}
	src := data
	readInt := func() (int64, error) {
		v, n := encoding.UnmarshalVarInt64(src)
		if n <= 0 {
			return 0, errors.New("invalid portable varint")
		}
		src = src[n:]
		return v, nil
	}
	min, err := readInt()
	if err != nil {
		return nil, err
	}
	max, err := readInt()
	if err != nil {
		return nil, err
	}
	first, err := readInt()
	if err != nil {
		return nil, err
	}
	rows, n := encoding.UnmarshalVarUint64(src)
	if n <= 0 || rows < 1 || rows > maxRows {
		return nil, errors.New("invalid portable row count")
	}
	src = src[n:]
	scale, err := readInt()
	if err != nil {
		return nil, err
	}
	if scale < math.MinInt16 || scale > math.MaxInt16 || min > max || len(src) < 3 {
		return nil, errors.New("invalid portable header")
	}
	tm, vm, precision := encoding.MarshalType(src[0]), encoding.MarshalType(src[1]), src[2]
	src = src[3:]
	if tm < 1 || tm > 6 || vm < 1 || vm > 6 || precision < 1 || precision > 64 {
		return nil, errors.New("unsupported portable encoding")
	}
	if rows < 2 && (tm == encoding.MarshalTypeNearestDelta2 || tm == encoding.MarshalTypeZSTDNearestDelta2 || vm == encoding.MarshalTypeNearestDelta2 || vm == encoding.MarshalTypeZSTDNearestDelta2) {
		return nil, errors.New("delta2 encoding requires at least two rows")
	}
	tdata, n := boundedBytes(src)
	if n <= 0 {
		return nil, errors.New("invalid timestamps frame")
	}
	src = src[n:]
	vdata, n := boundedBytes(src)
	if n <= 0 {
		return nil, errors.New("invalid values frame")
	}
	src = src[n:]
	if len(src) != 0 {
		return nil, errors.New("trailing portable block bytes")
	}
	// Each encoded int64 delta needs at most ten bytes. Reject oversized
	// decompression before entering upstream's unbounded decoder.
	tdata, tm, err = expandWithLimit(tdata, tm, int(rows)*10)
	if err != nil {
		return nil, err
	}
	vdata, vm, err = expandWithLimit(vdata, vm, int(rows)*10)
	if err != nil {
		return nil, err
	}
	ts, err := encoding.UnmarshalTimestamps(nil, tdata, tm, min, int(rows))
	if err != nil {
		return nil, fmt.Errorf("timestamps: %w", err)
	}
	vs, err := encoding.UnmarshalValues(nil, vdata, vm, first, int(rows))
	if err != nil {
		return nil, fmt.Errorf("values: %w", err)
	}
	if len(ts) != int(rows) || len(vs) != int(rows) {
		return nil, errors.New("decoded row count mismatch")
	}
	if precision < 64 {
		encoding.EnsureNonDecreasingSequence(ts, min, max)
	}
	if ts[0] != min || ts[len(ts)-1] != max {
		return nil, errors.New("timestamp bounds disagree with header")
	}
	for i := 1; i < len(ts); i++ {
		if ts[i] < ts[i-1] {
			return nil, errors.New("unordered timestamps")
		}
	}
	return &portable{ts: ts, values: vs, scale: int16(scale)}, nil
}

func boundedBytes(src []byte) ([]byte, int) {
	n, size := encoding.UnmarshalVarUint64(src)
	if size <= 0 || size > len(src) || n > uint64(len(src)-size) {
		return nil, 0
	}
	end := size + int(n)
	return src[size:end], end
}

func expand(src []byte, mt encoding.MarshalType) ([]byte, encoding.MarshalType, error) {
	return expandWithLimit(src, mt, maxDecodedBlockBytes)
}

func expandWithLimit(src []byte, mt encoding.MarshalType, limit int) ([]byte, encoding.MarshalType, error) {
	if mt != encoding.MarshalTypeZSTDNearestDelta && mt != encoding.MarshalTypeZSTDNearestDelta2 {
		if len(src) > limit {
			return nil, mt, errors.New("integer codec input exceeds row-count bound")
		}
		return src, mt, nil
	}
	d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxDecodedBlockBytes), zstd.WithDecoderMaxWindow(maxDecodedBlockBytes), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, mt, err
	}
	defer d.Close()
	b, err := d.DecodeAll(src, make([]byte, 0, limit))
	if err != nil {
		return nil, mt, fmt.Errorf("bounded zstd decode: %w", err)
	}
	if mt == encoding.MarshalTypeZSTDNearestDelta {
		mt = encoding.MarshalTypeNearestDelta
	} else {
		mt = encoding.MarshalTypeNearestDelta2
	}
	return b, mt, nil
}
