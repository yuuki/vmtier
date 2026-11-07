package pack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// BuildPacks consumes one native export and emits bounded packs synchronously.
// The callback must not retain payloads: the caller can stage each pack before
// reading the next one. Emitted packs are not proof of successful completion;
// no coverage may be published until the entire stream and independent oracle
// have been verified. An empty export emits no packs.
//
// maxSamples counts physical rows, including replicas and rows outside the
// logical interval. MaxInputBytes applies to the entire stream, not each pack.
type BuildStats struct {
	NativeInputBytes int64 `json:"native_input_bytes"`
	PhysicalSamples  int   `json:"physical_samples"`
	Records          int   `json:"records"`
}

func BuildPacks(r io.Reader, tenant Tenant, start, end int64, limits Limits, maxPacks, maxSamples int, emit func(*Index, []byte) error) error {
	_, err := BuildPacksMeasured(r, tenant, start, end, limits, maxPacks, maxSamples, emit)
	return err
}

// BuildPacksMeasured also reports consumed input and physical rows before
// clipping or deduplication. Partial statistics remain available on failure.
func BuildPacksMeasured(r io.Reader, tenant Tenant, start, end int64, limits Limits, maxPacks, maxSamples int, emit func(*Index, []byte) error) (BuildStats, error) {
	var stats BuildStats
	err := buildPacks(r, tenant, start, end, limits, maxPacks, maxSamples, emit, &stats)
	return stats, err
}

func buildPacks(r io.Reader, tenant Tenant, start, end int64, limits Limits, maxPacks, maxSamples int, emit func(*Index, []byte) error, stats *BuildStats) error {
	l, err := limits.normalized()
	if err != nil {
		return err
	}
	if start >= end || maxPacks < 1 || maxPacks > 1024 || maxSamples < 1 || maxSamples > 10000000 || emit == nil {
		return errors.New("invalid rotating pack coverage, callback, or window bounds")
	}
	lr := &io.LimitedReader{R: r, N: l.MaxInputBytes + 1}
	defer func() { stats.NativeInputBytes = l.MaxInputBytes + 1 - lr.N }()
	if err := readNativeHeader(lr, start, end); err != nil {
		return err
	}
	p := &packRotation{tenant: tenant, start: start, end: end, limits: l, maxPacks: maxPacks, maxSamples: maxSamples, emit: emit, stats: stats}
	p.builder = p.newBuilder()
	for {
		name, payload, err := readNativeRecord(lr, l)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := p.add(name, payload); err != nil {
			return err
		}
	}
	if lr.N <= 0 {
		return errors.New("native export exceeds input limit")
	}
	return p.flush()
}

// packRotation splits the records of one window into bounded packs, emitting
// each pack before it starts the next.
type packRotation struct {
	tenant       Tenant
	start, end   int64
	limits       Limits
	maxPacks     int
	maxSamples   int
	emit         func(*Index, []byte) error
	stats        *BuildStats
	builder      *rotatingBuilder
	packs        int
	physicalRows int
}

func (p *packRotation) newBuilder() *rotatingBuilder {
	return &rotatingBuilder{idx: &Index{Version: FormatVersion, VMVersion: VMVersion, VMRevision: VMRevision, Tenant: p.tenant, Start: p.start, End: p.end, SHA256: strings64Zero, Series: []Series{}}, ids: map[string]int{}}
}

// flush emits the current pack, unless it is empty, and starts a new one.
func (p *packRotation) flush() error {
	if p.builder.blocks == 0 {
		return nil
	}
	if p.packs >= p.maxPacks {
		return errors.New("archive window exceeds pack count limit")
	}
	p.builder.idx.Size = int64(len(p.builder.data))
	p.builder.idx.SHA256 = Sum(p.builder.data)
	if _, err := MarshalIndex(p.builder.idx, p.limits); err != nil {
		return err
	}
	if err := p.emit(p.builder.idx, p.builder.data); err != nil {
		return err
	}
	p.packs++
	p.builder = p.newBuilder()
	return nil
}

// add counts the physical rows of one native record and places it if it has
// rows in the logical window.
func (p *packRotation) add(name, payload []byte) error {
	pb, err := decodePortable(payload)
	if err != nil {
		return fmt.Errorf("portable block: %w", err)
	}
	if len(pb.ts) > p.maxSamples-p.physicalRows {
		return errors.New("archive physical sample verification limit exceeded")
	}
	p.physicalRows += len(pb.ts)
	p.stats.PhysicalSamples = p.physicalRows
	p.stats.Records++
	var mn storage.MetricName
	if err := mn.UnmarshalNoAccountIDProjectID(name); err != nil {
		return fmt.Errorf("metric name: %w", err)
	}
	labels := map[string]string{"__name__": string(mn.MetricGroup)}
	for _, tag := range mn.Tags {
		key := string(tag.Key)
		if _, exists := labels[key]; key == "" || exists {
			return errors.New("empty or duplicate label key in native metric name")
		}
		labels[key] = string(tag.Value)
	}
	lo := sort.Search(len(pb.ts), func(i int) bool { return pb.ts[i] >= p.start })
	if lo == len(pb.ts) || pb.ts[lo] >= p.end {
		return nil
	}
	canonical, _ := json.Marshal(labels)
	ref := BlockRef{Length: int64(len(payload)), MinTimestamp: pb.ts[0], MaxTimestamp: pb.ts[len(pb.ts)-1], Rows: len(pb.ts), SHA256: Sum(payload)}
	return p.place(string(canonical), labels, name, payload, ref)
}

// place adds a record to the current pack. A record that does not fit is
// added to a new pack after the current one is emitted.
func (p *packRotation) place(key string, labels map[string]string, name, payload []byte, ref BlockRef) error {
	added, err := p.builder.add(key, labels, name, payload, ref, p.limits)
	if err != nil {
		return err
	}
	if added {
		return nil
	}
	if p.builder.blocks == 0 {
		return errors.New("native record cannot fit within configured pack bounds")
	}
	if err := p.flush(); err != nil {
		return err
	}
	if p.packs >= p.maxPacks {
		return errors.New("archive window exceeds pack count limit")
	}
	added, err = p.builder.add(key, labels, name, payload, ref, p.limits)
	if err != nil {
		return err
	}
	if !added {
		return errors.New("native record cannot fit within configured pack bounds")
	}
	return nil
}

const strings64Zero = "0000000000000000000000000000000000000000000000000000000000000000"

type rotatingBuilder struct {
	idx         *Index
	data        []byte
	ids         map[string]int
	blocks      int
	indexBudget int64
	seriesBytes int64 // encoded Series contents including intervening commas
}

// add computes exact encoded index length without re-encoding all prior blocks
// for each record. A false return does not mutate the builder.
func (b *rotatingBuilder) add(key string, labels map[string]string, name, payload []byte, ref BlockRef, l Limits) (bool, error) {
	si, exists := b.ids[key]
	if exists && !bytes.Equal(b.idx.Series[si].NativeName, name) {
		return false, errors.New("inconsistent native name ordering for identical labels")
	}
	if b.blocks >= l.MaxBlocks || (!exists && len(b.idx.Series) >= l.MaxSeries) || int64(len(payload)) > l.MaxPackBytes-int64(len(b.data)) {
		return false, nil
	}
	budget := b.indexBudget + 256
	seriesBytes := b.seriesBytes
	if !exists {
		budget += int64(len(key) + len(name)*2)
		encoded, _ := json.Marshal(Series{Labels: labels, NativeName: name, Blocks: []BlockRef{}})
		seriesBytes += int64(len(encoded))
		if len(b.idx.Series) > 0 {
			seriesBytes++
		}
	} else if len(b.idx.Series[si].Blocks) > 0 {
		seriesBytes++
	}
	ref.Offset = int64(len(b.data))
	encodedRef, _ := json.Marshal(ref)
	seriesBytes += int64(len(encodedRef))
	header := *b.idx
	header.Size = int64(len(b.data)) + int64(len(payload))
	header.Series = []Series{}
	encodedHeader, _ := json.Marshal(&header)
	if budget > l.MaxIndexBytes || int64(len(encodedHeader))+seriesBytes > l.MaxIndexBytes {
		return false, nil
	}
	if !exists {
		si = len(b.idx.Series)
		b.ids[key] = si
		b.idx.Series = append(b.idx.Series, Series{Labels: labels, NativeName: name, Blocks: []BlockRef{}})
	}
	b.idx.Series[si].Blocks = append(b.idx.Series[si].Blocks, ref)
	b.data = append(b.data, payload...)
	b.blocks++
	b.indexBudget, b.seriesBytes = budget, seriesBytes
	return true, nil
}
