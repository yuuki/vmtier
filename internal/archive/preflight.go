package archive

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/pack"
)

// PreflightResult describes one source observation, not a published window or a
// workload guarantee. On failure, counters describe only work completed so far.
type PreflightResult struct {
	Verified            bool             `json:"verified"`
	Error               string           `json:"error,omitempty"`
	Tenant              string           `json:"tenant"`
	Source              string           `json:"source"`
	StartMS             int64            `json:"start_ms"`
	EndMS               int64            `json:"end_ms"`
	Policy              catalog.Contract `json:"policy"`
	PackLimits          pack.Limits      `json:"pack_limits"`
	MaxPacks            int              `json:"max_packs_per_window"`
	MaxPhysicalSamples  int              `json:"max_physical_samples"`
	MaxRawResponseBytes int64            `json:"max_raw_response_bytes"`
	HotRetentionMS      int64            `json:"hot_retention_ms"`
	SafetyMarginMS      int64            `json:"safety_margin_ms"`
	AttemptBudgetMS     int64            `json:"attempt_budget_ms"`
	NativeInputBytes    int64            `json:"native_input_bytes"`
	NativeRecords       int              `json:"native_records"`
	PhysicalSamples     int              `json:"physical_samples"`
	VerifiedSamples     uint64           `json:"verified_deduplicated_samples"`
	UniqueSeries        int              `json:"unique_series"`
	Blocks              int              `json:"blocks"`
	DataBytes           int64            `json:"data_bytes"`
	IndexBytes          int64            `json:"index_bytes"`
	Packs               []PreflightPack  `json:"packs"`
	ElapsedSeconds      float64          `json:"elapsed_seconds"`
}

type PreflightPack struct {
	DataBytes       int64 `json:"data_bytes"`
	IndexBytes      int64 `json:"index_bytes"`
	Series          int   `json:"series"`
	Blocks          int   `json:"blocks"`
	PhysicalSamples int   `json:"physical_samples"`
}

// Preflight uses the SQLite archive builder and independent completeness oracle
// without a catalog or blob store. Restrict the observation to one stream/window
// and require an attempt deadline, just as the service does for a window.
func Preflight(ctx context.Context, cfg Config, now time.Time) (out *PreflightResult, err error) {
	started := time.Now()
	out = &PreflightResult{StartMS: cfg.StartMS, EndMS: cfg.EndMS, Packs: []PreflightPack{}, MaxPacks: catalog.SQLiteMaxPacksPerWindow}
	defer func() {
		out.ElapsedSeconds = time.Since(started).Seconds()
		if err != nil {
			out.Error = err.Error()
		}
	}()
	if deadline, ok := ctx.Deadline(); !ok {
		return out, errors.New("preflight requires a bounded attempt deadline")
	} else {
		out.AttemptBudgetMS = time.Until(deadline).Milliseconds()
	}
	if err = cfg.Validate(now); err != nil {
		return out, err
	}
	if len(cfg.Tenants) != 1 || len(cfg.Sources) != 1 || cfg.EndMS-cfg.StartMS != cfg.Window.Milliseconds() {
		return out, errors.New("preflight requires exactly one configured tenant, source, and complete logical window")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 2 * time.Minute}
	}
	if cfg.MaxSamples == 0 {
		cfg.MaxSamples = 5000000
	}
	cfg.PackLimits, err = pack.EffectiveLimits(cfg.PackLimits)
	if err != nil {
		return out, err
	}
	out.PackLimits, out.MaxPhysicalSamples, out.Policy = cfg.PackLimits, cfg.MaxSamples, archiveContract(cfg)
	out.MaxRawResponseBytes, out.HotRetentionMS, out.SafetyMarginMS = maxOracleBytes, cfg.Retention.Milliseconds(), cfg.SafetyMargin.Milliseconds()
	out.Tenant, out.Source = cfg.Tenants[0], cfg.Sources[0].ID
	tenant, _ := pack.ParseTenant(out.Tenant)
	seriesKeys := map[string]bool{}
	stats, samples, buildErr := buildVerifiedPacks(ctx, cfg, cfg.Sources[0], tenant, cfg.StartMS, cfg.EndMS, func(idx *pack.Index, data []byte) error {
		encoded, err := pack.MarshalIndex(idx, cfg.PackLimits)
		if err != nil {
			return err
		}
		p := PreflightPack{DataBytes: int64(len(data)), IndexBytes: int64(len(encoded)), Series: len(idx.Series)}
		for _, series := range idx.Series {
			key, _ := json.Marshal(series.Labels)
			seriesKeys[string(key)] = true
			p.Blocks += len(series.Blocks)
			for _, block := range series.Blocks {
				p.PhysicalSamples += block.Rows
			}
		}
		out.Packs = append(out.Packs, p)
		out.Blocks += p.Blocks
		out.DataBytes += p.DataBytes
		out.IndexBytes += p.IndexBytes
		return nil
	})
	out.NativeInputBytes, out.NativeRecords, out.PhysicalSamples = stats.NativeInputBytes, stats.Records, stats.PhysicalSamples
	out.UniqueSeries = len(seriesKeys)
	if buildErr != nil {
		return out, buildErr
	}
	if err = ctx.Err(); err != nil {
		return out, err
	}
	if err = cfg.validateTime(cfg.StartMS, cfg.EndMS, now.Add(time.Since(started))); err != nil {
		return out, err
	}
	out.Verified, out.VerifiedSamples = true, samples
	return out, nil
}
