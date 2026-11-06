package pack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
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
	var header [16]byte
	if _, err := io.ReadFull(lr, header[:]); err != nil {
		return fmt.Errorf("native header: %w", err)
	}
	if encoding.UnmarshalInt64(header[:]) != start || encoding.UnmarshalInt64(header[8:]) != end-1 {
		return errors.New("native export header differs from requested [start,end)")
	}
	newBuilder := func() *rotatingBuilder {
		return &rotatingBuilder{idx: &Index{Version: FormatVersion, VMVersion: VMVersion, VMRevision: VMRevision, Tenant: tenant, Start: start, End: end, SHA256: strings64Zero, Series: []Series{}}, ids: map[string]int{}}
	}
	builder := newBuilder()
	packs, physicalRows := 0, 0
	flush := func() error {
		if builder.blocks == 0 {
			return nil
		}
		if packs >= maxPacks {
			return errors.New("archive window exceeds pack count limit")
		}
		builder.idx.Size = int64(len(builder.data))
		builder.idx.SHA256 = Sum(builder.data)
		if _, err := MarshalIndex(builder.idx, l); err != nil {
			return err
		}
		if err := emit(builder.idx, builder.data); err != nil {
			return err
		}
		packs++
		builder = newBuilder()
		return nil
	}
	for {
		name, err := readFrame(lr, l.MaxMetricNameBytes, true)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("native metric name: %w", err)
		}
		payload, err := readFrame(lr, l.MaxBlockBytes, false)
		if err != nil {
			return fmt.Errorf("native block: %w", err)
		}
		if lr.N <= 0 {
			return errors.New("native export exceeds input limit")
		}
		pb, err := decodePortable(payload)
		if err != nil {
			return fmt.Errorf("portable block: %w", err)
		}
		if len(pb.ts) > maxSamples-physicalRows {
			return errors.New("archive physical sample verification limit exceeded")
		}
		physicalRows += len(pb.ts)
		stats.PhysicalSamples = physicalRows
		stats.Records++
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
		lo := sort.Search(len(pb.ts), func(i int) bool { return pb.ts[i] >= start })
		if lo == len(pb.ts) || pb.ts[lo] >= end {
			continue
		}
		canonical, _ := json.Marshal(labels)
		ref := BlockRef{Length: int64(len(payload)), MinTimestamp: pb.ts[0], MaxTimestamp: pb.ts[len(pb.ts)-1], Rows: len(pb.ts), SHA256: Sum(payload)}
		added, err := builder.add(string(canonical), labels, name, payload, ref, l)
		if err != nil {
			return err
		}
		if !added {
			if builder.blocks == 0 {
				return errors.New("native record cannot fit within configured pack bounds")
			}
			if err := flush(); err != nil {
				return err
			}
			if packs >= maxPacks {
				return errors.New("archive window exceeds pack count limit")
			}
			added, err = builder.add(string(canonical), labels, name, payload, ref, l)
			if err != nil {
				return err
			}
			if !added {
				return errors.New("native record cannot fit within configured pack bounds")
			}
		}
	}
	if lr.N <= 0 {
		return errors.New("native export exceeds input limit")
	}
	return flush()
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
