package archive

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"sort"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

// stageVerifiedPacks holds one pack at a time and one bounded window-wide
// expected-sample map. Objects staged before source completion remain invisible
// until the caller atomically publishes every reference and the coverage window.
func stageVerifiedPacks(ctx context.Context, blobs objectstore.BlobStore, cfg Config, source Source, tenant pack.Tenant, start, end int64) ([]catalog.PackRef, uint64, error) {
	refs := []catalog.PackRef{}
	_, total, err := buildVerifiedPacks(ctx, cfg, source, tenant, start, end, func(idx *pack.Index, data []byte) error {
		ref, err := uploadPack(ctx, blobs, cfg, idx, data)
		if err != nil {
			return err
		}
		refs = append(refs, ref)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return refs, total, nil
}

func buildVerifiedPacks(ctx context.Context, cfg Config, source Source, tenant pack.Tenant, start, end int64, emit func(*pack.Index, []byte) error) (pack.BuildStats, uint64, error) {
	var stats pack.BuildStats
	q := url.Values{"match[]": {`{__name__!=""}`}, "start": {seconds(start)}, "end": {seconds(end - 1)}, "deny_partial_response": {"1"}, "nocache": {"1"}}
	resp, err := request(ctx, cfg, endpoint(source, tenant, "export/native", q))
	if err != nil {
		return stats, 0, err
	}
	defer resp.Body.Close()
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "VictoriaMetrics/native") {
		return stats, 0, errors.New("unexpected native export content type")
	}
	samples := map[string][]sample{}
	names := map[string]string{}
	var verifierBytes int64
	defer cfg.Memory.setVerifier(0)
	stats, err = pack.BuildPacksMeasured(contextReader{ctx, resp.Body}, tenant, start, end, cfg.PackLimits, catalog.SQLiteMaxPacksPerWindow, cfg.MaxSamples, func(idx *pack.Index, data []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for si, series := range idx.Series {
			keyBytes, _ := json.Marshal(series.Labels)
			key := string(keyBytes)
			if previous, ok := names[key]; ok && previous != string(series.NativeName) {
				return errors.New("inconsistent native name ordering across packs")
			}
			if _, exists := names[key]; !exists {
				// Each map can retain its own equal key across pack boundaries.
				// Charge both, the native name and map/slice header allowances.
				// Values use actual slice capacity below.
				verifierBytes += int64(2*len(key)+len(series.NativeName)) + 256
				names[key] = string(series.NativeName)
				cfg.Memory.setVerifier(verifierBytes)
			}
			for bi, block := range series.Blocks {
				mb, err := pack.LoadBlock(idx, si, bi, data[block.Offset:block.Offset+block.Length])
				if err != nil {
					return err
				}
				if mb == nil {
					continue
				}
				if err := mb.Block.UnmarshalData(); err != nil {
					return err
				}
				ts, vs := mb.Block.AppendRowsWithTimeRangeFilter(nil, nil, storage.TimeRange{MinTimestamp: start, MaxTimestamp: end - 1})
				rows := samples[key]
				previousCapacity := cap(rows)
				for i, timestamp := range ts {
					if math.IsNaN(vs[i]) && !decimal.IsStaleNaN(vs[i]) {
						return errors.New("ordinary NaN is unsupported by the pinned VM native decimal codec")
					}
					rows = append(rows, sample{timestamp, vs[i]})
				}
				samples[key] = rows
				verifierBytes += int64(cap(rows)-previousCapacity) * 16
				cfg.Memory.setVerifier(verifierBytes)
			}
		}
		return emit(idx, data)
	})
	if err != nil {
		return stats, 0, fmt.Errorf("export tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	// The pinned VM deduplicator must see every physical sample for a series,
	// including conflicts and stale markers separated by pack boundaries.
	for key, rows := range samples {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].timestamp < rows[j].timestamp })
		cfg.Memory.setVerifier(verifierBytes + int64(len(rows))*16)
		ts, vs := make([]int64, len(rows)), make([]float64, len(rows))
		for i, row := range rows {
			ts[i], vs[i] = row.timestamp, row.value
		}
		ts, vs = storage.DeduplicateSamples(ts, vs, 1)
		rows = rows[:len(ts)]
		for i := range rows {
			rows[i] = sample{ts[i], vs[i]}
		}
		samples[key] = rows
		cfg.Memory.setVerifier(verifierBytes)
	}
	counts := countsOf(samples)
	if err := verifyRaw(ctx, cfg, source, tenant, start, end, samples); err != nil {
		return stats, 0, fmt.Errorf("raw verification tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	if err := verifyCounts(ctx, cfg, source, tenant, start, end, counts); err != nil {
		return stats, 0, fmt.Errorf("verify tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	if err := hook(cfg, "exported"); err != nil {
		return stats, 0, err
	}
	var total uint64
	for _, count := range counts {
		total += uint64(count.normal + count.stale)
	}
	return stats, total, nil
}

// Check the attempt deadline even when net/http still has buffered input.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// Both publication backends use the same independent completeness checks.
func verifiedExport(ctx context.Context, cfg Config, source Source, tenant pack.Tenant, start, end int64) (*pack.Index, []byte, uint64, error) {
	idx, data, err := export(ctx, cfg, source, tenant, start, end)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("export tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	samples, err := extractSamples(idx, data, cfg.MaxSamples)
	if err != nil {
		return nil, nil, 0, err
	}
	counts := countsOf(samples)
	if err = verifyRaw(ctx, cfg, source, tenant, start, end, samples); err != nil {
		return nil, nil, 0, fmt.Errorf("raw verification tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	if err = verifyCounts(ctx, cfg, source, tenant, start, end, counts); err != nil {
		return nil, nil, 0, fmt.Errorf("verify tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	if err = hook(cfg, "exported"); err != nil {
		return nil, nil, 0, err
	}
	var total uint64
	for _, count := range counts {
		total += uint64(count.normal + count.stale)
	}
	return idx, data, total, nil
}

// A fresh attempt namespace prevents a retry from modifying published objects.
func uploadPack(ctx context.Context, blobs objectstore.BlobStore, cfg Config, idx *pack.Index, data []byte) (catalog.PackRef, error) {
	var ref catalog.PackRef
	indexData, err := pack.MarshalIndex(idx, cfg.PackLimits)
	if err != nil {
		return ref, err
	}
	packBytes := int64(cap(data)+cap(indexData)) + pack.IndexMemoryEstimate(idx)
	cfg.Memory.setPack(packBytes)
	defer cfg.Memory.setPack(0)
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return ref, fmt.Errorf("allocate unique archive attempt: %w", err)
	}
	prefix := fmt.Sprintf("tenants/%s/attempts/%s/%s", idx.Tenant.String(), hex.EncodeToString(nonce[:]), pack.Sum(indexData))
	ref = catalog.PackRef{
		DataKey: prefix + ".pack", IndexKey: prefix + ".json",
		DataSHA256: pack.Sum(data), IndexSHA256: pack.Sum(indexData),
		DataBytes: int64(len(data)), IndexBytes: int64(len(indexData)),
	}
	if err = blobs.Put(ctx, ref.DataKey, data); err != nil {
		return ref, fmt.Errorf("upload payload: %w", err)
	}
	if err = blobs.Put(ctx, ref.IndexKey, indexData); err != nil {
		return ref, fmt.Errorf("upload index: %w", err)
	}
	if err = hook(cfg, "uploaded"); err != nil {
		return ref, err
	}
	for _, item := range []struct {
		key  string
		data []byte
	}{{ref.DataKey, data}, {ref.IndexKey, indexData}} {
		got, err := blobs.Get(ctx, item.key)
		if err != nil {
			return ref, fmt.Errorf("verify uploaded object: %w", err)
		}
		cfg.Memory.setPack(packBytes + int64(cap(got)))
		if !bytes.Equal(got, item.data) {
			return ref, errors.New("uploaded object checksum or length mismatch")
		}
		cfg.Memory.setPack(packBytes)
	}
	return ref, nil
}
