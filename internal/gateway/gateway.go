package gateway

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/vmselectapi"
)

// Backend returns exclusively committed blocks for the requested tenant and
// query range. It must enforce bounded reads before allocating its result and
// honor ctx. Blocks must be independently owned (no shared mutable cache data).
// Any absent/corrupt object or unavailable catalog must be returned as an error.
type Backend interface {
	Search(context.Context, *storage.SearchQuery) ([]storage.MetricBlock, error)
}

type Limits struct {
	MaxSeries          int
	MaxBlocks          int
	MaxResponseBytes   int64
	MaxRequestDuration time.Duration
}

// API implements the upstream public vmselectapi.API without a patched VM.
// Search is fully validated before the RPC server sends its success prefix.
type API struct {
	backend  Backend
	limits   Limits
	Requests atomic.Uint64
	Errors   atomic.Uint64
	Blocks   atomic.Uint64
}

var _ vmselectapi.API = (*API)(nil)

func New(backend Backend, limits Limits) *API {
	if limits.MaxSeries <= 0 {
		limits.MaxSeries = 100000
	}
	if limits.MaxBlocks <= 0 {
		limits.MaxBlocks = 100000
	}
	if limits.MaxResponseBytes <= 0 {
		limits.MaxResponseBytes = 128 << 20
	}
	if limits.MaxRequestDuration <= 0 {
		limits.MaxRequestDuration = 30 * time.Second
	}
	return &API{backend: backend, limits: limits}
}

// Tenant returns the only tenant allowed by the read protocol. Multi-tenant
// frontend requests are split by stock vmselect into single-tenant RPCs.
func Tenant(sq *storage.SearchQuery) (uint32, uint32, error) {
	if sq == nil {
		return 0, 0, fmt.Errorf("nil search query")
	}
	if sq.IsMultiTenant || len(sq.TenantTokens) > 1 {
		return 0, 0, fmt.Errorf("multi-tenant storage RPC is unsupported")
	}
	if len(sq.TenantTokens) == 1 {
		return sq.TenantTokens[0].AccountID, sq.TenantTokens[0].ProjectID, nil
	}
	return sq.AccountID, sq.ProjectID, nil
}

func (a *API) search(sq *storage.SearchQuery, deadline uint64) (blocks []storage.MetricBlock, err error) {
	a.Requests.Add(1)
	defer func() {
		if err != nil {
			a.Errors.Add(1)
		}
	}()
	account, project, err := Tenant(sq)
	if err != nil {
		return nil, err
	}
	if sq.MinTimestamp > sq.MaxTimestamp {
		return nil, fmt.Errorf("invalid query interval")
	}
	matcher, err := CompileMatcher(sq.TagFilterss)
	if err != nil {
		return nil, err
	}
	dl := time.Now().Add(a.limits.MaxRequestDuration)
	if deadline != 0 {
		if deadline > math.MaxInt64 {
			return nil, fmt.Errorf("invalid RPC deadline")
		}
		requested := time.Unix(int64(deadline), 0)
		if requested.Before(dl) {
			dl = requested
		}
	}
	ctx, cancel := context.WithDeadline(context.Background(), dl)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.backend == nil {
		return nil, fmt.Errorf("Cold backend is unavailable")
	}
	blocks, err = a.backend.Search(ctx, sq)
	if err != nil {
		return nil, fmt.Errorf("Cold search: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(blocks) > a.limits.MaxBlocks {
		return nil, fmt.Errorf("Cold block limit exceeded")
	}
	maxSeries := a.limits.MaxSeries
	if sq.MaxMetrics > 0 && sq.MaxMetrics < maxSeries {
		maxSeries = sq.MaxMetrics
	}
	names := make(map[string]struct{})
	var byteCount int64
	result := blocks[:0]
	var ts []int64
	var vs []float64
	for i := range blocks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mb := &blocks[i]
		var mn storage.MetricName
		if err := mn.Unmarshal(mb.MetricName); err != nil {
			return nil, fmt.Errorf("invalid Cold metric name: %w", err)
		}
		if mn.AccountID != account || mn.ProjectID != project {
			return nil, fmt.Errorf("Cold tenant mismatch")
		}
		if !matcher.Match(&mn) {
			continue
		}
		if mb.Block.RowsCount() <= 0 {
			return nil, fmt.Errorf("invalid empty Cold block")
		}
		if err := mb.Block.UnmarshalData(); err != nil {
			return nil, fmt.Errorf("invalid Cold block: %w", err)
		}
		ts, vs = mb.Block.AppendRowsWithTimeRangeFilter(ts[:0], vs[:0], sq.GetTimeRange())
		if len(ts) == 0 {
			continue
		}
		// Preserve the pinned VM's native tag order. vmselect groups blocks by
		// these exact bytes; lexical sorting would split a Hot/Cold series
		// because VM prioritizes job-like and instance-like tags.
		names[string(mb.MetricName)] = struct{}{}
		if len(names) > maxSeries {
			return nil, fmt.Errorf("Cold series limit exceeded")
		}
		h, t, v := mb.Block.MarshalData(0, 0)
		byteCount += int64(len(mb.MetricName) + len(h) + len(t) + len(v) + 32)
		if byteCount > a.limits.MaxResponseBytes {
			return nil, fmt.Errorf("Cold response byte limit exceeded")
		}
		result = append(result, *mb)
	}
	a.Blocks.Add(uint64(len(result)))
	return result, nil
}

func (a *API) InitSearch(_ *querytracer.Tracer, sq *storage.SearchQuery, deadline uint64) (vmselectapi.BlockIterator, error) {
	blocks, err := a.search(sq, deadline)
	if err != nil {
		return nil, err
	}
	return &iterator{blocks: blocks}, nil
}

type iterator struct {
	blocks []storage.MetricBlock
	next   int
}

func (it *iterator) NextBlock(dst *storage.MetricBlock) bool {
	if it.next == len(it.blocks) {
		return false
	}
	dst.CopyFrom(&it.blocks[it.next])
	it.next++
	return true
}
func (it *iterator) MustClose()   { it.blocks = nil; it.next = 0 }
func (it *iterator) Error() error { return nil }

func (a *API) SearchMetricNames(_ *querytracer.Tracer, sq *storage.SearchQuery, deadline uint64) ([]string, error) {
	blocks, err := a.search(sq, deadline)
	if err != nil {
		return nil, err
	}
	names := make(map[string]struct{})
	for i := range blocks {
		names[string(blocks[i].MetricName)] = struct{}{}
	}
	return sortedKeys(names), nil
}

func (a *API) LabelNames(qt *querytracer.Tracer, sq *storage.SearchQuery, max int, deadline uint64) ([]string, error) {
	names, err := a.SearchMetricNames(qt, sq, deadline)
	if err != nil {
		return nil, err
	}
	labels := make(map[string]struct{})
	for _, raw := range names {
		var mn storage.MetricName
		if err := mn.UnmarshalString(raw); err != nil {
			return nil, err
		}
		if len(mn.MetricGroup) > 0 {
			labels["__name__"] = struct{}{}
		}
		for _, tag := range mn.Tags {
			if len(tag.Value) > 0 {
				labels[string(tag.Key)] = struct{}{}
			}
		}
	}
	return limitKeys(labels, max), nil
}

func (a *API) LabelValues(qt *querytracer.Tracer, sq *storage.SearchQuery, label string, max int, deadline uint64) ([]string, error) {
	names, err := a.SearchMetricNames(qt, sq, deadline)
	if err != nil {
		return nil, err
	}
	values := make(map[string]struct{})
	for _, raw := range names {
		var mn storage.MetricName
		if err := mn.UnmarshalString(raw); err != nil {
			return nil, err
		}
		var value []byte
		if label == "" || label == "__name__" {
			value = mn.MetricGroup
		} else {
			value = mn.GetTagValue(label)
		}
		if len(value) > 0 {
			values[string(value)] = struct{}{}
		}
	}
	return limitKeys(values, max), nil
}

func sortedKeys(set map[string]struct{}) []string {
	result := make([]string, 0, len(set))
	for key := range set {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}
func limitKeys(set map[string]struct{}, max int) []string {
	result := sortedKeys(set)
	if max > 0 && len(result) > max {
		result = result[:max]
	}
	return result
}

func unsupported(operation string) error {
	return fmt.Errorf("vmtier read-only gateway: unsupported RPC %s", operation)
}
func (a *API) TagValueSuffixes(_ *querytracer.Tracer, _, _ uint32, _ storage.TimeRange, _, _ string, _ byte, _ int, _ uint64) ([]string, error) {
	return nil, unsupported("tagValueSuffixes")
}
func (a *API) SeriesCount(_ *querytracer.Tracer, _, _ uint32, _ uint64) (uint64, error) {
	return 0, unsupported("seriesCount")
}
func (a *API) TSDBStatus(_ *querytracer.Tracer, _ *storage.SearchQuery, _ string, _ int, _ uint64) (*storage.TSDBStatus, error) {
	return nil, unsupported("tsdbStatus")
}
func (a *API) DeleteSeries(_ *querytracer.Tracer, _ *storage.SearchQuery, _ uint64) (int, error) {
	return 0, unsupported("deleteSeries")
}
func (a *API) RegisterMetricNames(_ *querytracer.Tracer, _ []storage.MetricRow, _ uint64) error {
	return unsupported("registerMetricNames")
}
func (a *API) Tenants(_ *querytracer.Tracer, _ storage.TimeRange, _ uint64) ([]string, error) {
	return nil, unsupported("tenants")
}
func (a *API) GetMetricNamesUsageStats(_ *querytracer.Tracer, _ *storage.TenantToken, _, _ int, _ string, _ uint64) (storage.MetricNamesStatsResponse, error) {
	return storage.MetricNamesStatsResponse{}, unsupported("metricNamesUsageStats")
}
func (a *API) ResetMetricNamesUsageStats(_ *querytracer.Tracer, _ uint64) error {
	return unsupported("resetMetricNamesUsageStats")
}
