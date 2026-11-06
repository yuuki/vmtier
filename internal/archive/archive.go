// Package archive publishes verified age-selected windows from a fixed Hot-only
// source set. It never deletes Hot samples or changes VictoriaMetrics retention.
package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

type Source struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type Config struct {
	Tenants                                        []string
	Sources                                        []Source
	StartMS, EndMS                                 int64
	CoverageStart                                  int64
	Age, Retention, Lateness, SafetyMargin, Window time.Duration
	LatenessEnforced                               bool
	HTTPClient                                     *http.Client
	PackLimits                                     pack.Limits
	// MaxSamples bounds verifier expansion independently from compressed pack size.
	MaxSamples int
	// Hook is for failure-injection tests. It is never enabled by the CLI.
	Hook func(stage string) error
}

type Result struct {
	CommittedWindows int    `json:"committed_windows"`
	SkippedWindows   int    `json:"skipped_windows"`
	Entries          int    `json:"entries"`
	Samples          uint64 `json:"samples"`
	Bytes            int64  `json:"bytes"`
}

// Validate checks the declared operational contract. LatenessEnforced is an
// operator assertion, not an ingress filter: direct Hot writes remain outside
// this process's control. The supported deduplication interval is exactly 1ms.
func (c Config) Validate(now time.Time) error {
	if e := c.validateConfig(); e != nil {
		return e
	}
	return c.validateTime(c.StartMS, c.EndMS, now)
}
func (c Config) validateTime(start, end int64, now time.Time) error {
	if end > now.UnixMilli()-c.Age.Milliseconds() {
		return errors.New("requested end contains samples younger than archive age H")
	}
	if start <= now.UnixMilli()-c.Retention.Milliseconds()+c.SafetyMargin.Milliseconds() {
		return errors.New("archive start is expired or inside the Hot retention safety margin; completeness cannot be established")
	}
	return nil
}
func (c Config) validateConfig() error {
	if !c.LatenessEnforced {
		return errors.New("bounded lateness must be enforced at every write/backfill entry point; explicit acknowledgement required")
	}
	if c.Age <= 0 || c.Lateness < 0 || c.Age <= c.Lateness || c.SafetyMargin <= 0 || c.Retention <= c.Age || c.Retention-c.Age <= c.SafetyMargin || c.Window < time.Millisecond {
		return errors.New("require H > L >= 0, safety > 0, R > H+safety and window >= 1ms")
	}
	for _, d := range []time.Duration{c.Age, c.Retention, c.Lateness, c.SafetyMargin, c.Window} {
		if d%time.Millisecond != 0 {
			return errors.New("durations must be whole milliseconds")
		}
	}
	if c.StartMS < 0 || c.StartMS >= c.EndMS {
		return errors.New("require nonnegative start_ms < end_ms")
	}
	if c.CoverageStart <= 0 || c.CoverageStart%c.Window.Milliseconds() != 0 || c.StartMS < c.CoverageStart || c.StartMS%c.Window.Milliseconds() != 0 || c.EndMS%c.Window.Milliseconds() != 0 {
		return errors.New("coverage origin and archive range must be positive UTC window aligned; start must not precede origin")
	}
	if len(c.Tenants) == 0 || len(c.Tenants) > 100 || len(c.Sources) == 0 || len(c.Sources) > 100 {
		return errors.New("require 1..100 fixed tenants and source endpoints")
	}
	seen := map[string]bool{}
	for _, s := range c.Tenants {
		t, e := pack.ParseTenant(s)
		if e != nil {
			return e
		}
		if seen[t.String()] {
			return errors.New("duplicate tenant")
		}
		seen[t.String()] = true
	}
	seen = map[string]bool{}
	urls := map[string]bool{}
	for _, s := range c.Sources {
		if s.ID == "" || len(s.ID) > 128 || strings.ContainsAny(s.ID, "/\\\x00") || seen[s.ID] {
			return errors.New("source IDs must be unique nonempty safe strings")
		}
		seen[s.ID] = true
		u, e := url.Parse(s.URL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("source URL must be HTTP(S) without credentials, query or fragment")
		}
		normalized := strings.TrimRight(s.URL, "/")
		if urls[normalized] {
			return errors.New("duplicate source endpoint")
		}
		urls[normalized] = true
	}
	if c.MaxSamples < 0 || c.MaxSamples > 10000000 {
		return errors.New("max samples must be 0 (default) or 1..10000000")
	}
	return nil
}

// Run commits each full window atomically across every configured tenant/source.
// A retry resumes from catalog state; uploaded but unpublished objects are safe
// orphans. A conflicting writer causes an error, never a blind retry/overwrite.
func Run(ctx context.Context, blobs objectstore.BlobStore, catalogs catalog.Store, cfg Config, now time.Time) (*Result, error) {
	if err := cfg.validateConfig(); err != nil {
		return nil, err
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 2 * time.Minute}
	}
	if cfg.MaxSamples == 0 {
		cfg.MaxSamples = 5000000
	}
	out := &Result{}
	m, generation, err := catalog.Load(ctx, catalogs)
	if err != nil {
		return nil, err
	}
	if err = validateTopology(m, cfg); err != nil {
		return nil, err
	}
	window := cfg.Window.Milliseconds()
	runStart := time.Now()
	for start := cfg.StartMS; start < cfg.EndMS; {
		end := start + window
		if end < start || end > cfg.EndMS {
			end = cfg.EndMS
		}
		committed, e := existingWindow(m, cfg, start, end)
		if e != nil {
			return out, e
		}
		if committed {
			out.SkippedWindows++
			start = end
			continue
		}
		if err = cfg.validateTime(start, end, now.Add(time.Since(runStart))); err != nil {
			return out, err
		}
		// Re-evaluate wall time for a long-running archive. Tests use a fixed initial
		// clock plus real elapsed duration, rather than a way to bypass retention.
		entries := make([]catalog.Entry, 0, len(cfg.Sources)*len(cfg.Tenants))
		slog.Info("archive window selected", "start_ms", start, "end_ms", end, "sources", len(cfg.Sources), "tenants", len(cfg.Tenants))
		for _, tn := range cfg.Tenants {
			tenant, _ := pack.ParseTenant(tn)
			for _, source := range cfg.Sources {
				if err = ctx.Err(); err != nil {
					return out, err
				}
				idx, data, samples, e := verifiedExport(ctx, cfg, source, tenant, start, end)
				if e != nil {
					return out, e
				}
				ref, e := uploadPack(ctx, blobs, cfg, idx, data)
				if e != nil {
					return out, e
				}
				entry := catalog.Entry{Tenant: tenant.String(), Start: start, End: end, SourceID: source.ID, IndexKey: ref.IndexKey, DataKey: ref.DataKey, IndexSHA256: ref.IndexSHA256, SourceFingerprint: pack.Sum([]byte(strings.TrimRight(source.URL, "/")))}
				entries = append(entries, entry)
				out.Samples += samples
				out.Bytes += ref.DataBytes + ref.IndexBytes
			}
		}
		if start <= now.Add(time.Since(runStart)).UnixMilli()-cfg.Retention.Milliseconds()+cfg.SafetyMargin.Milliseconds() {
			return out, errors.New("retention safety margin elapsed before commit")
		}
		if err = hook(cfg, "verified"); err != nil {
			return out, err
		}
		m.Entries = append(m.Entries, entries...)
		generation, err = catalog.Commit(ctx, catalogs, m, generation)
		if err != nil {
			return out, fmt.Errorf("catalog commit: %w", err)
		}
		out.CommittedWindows++
		out.Entries += len(entries)
		slog.Info("archive window committed", "start_ms", start, "end_ms", end, "generation", m.Generation)
		if err = hook(cfg, "committed"); err != nil {
			return out, err
		}
		start = end
	}
	return out, nil
}

func hook(c Config, stage string) error {
	if c.Hook != nil {
		return c.Hook(stage)
	}
	return nil
}

func archiveContract(c Config) catalog.Contract {
	contract := catalog.Contract{CoverageStart: c.CoverageStart, WindowMS: c.Window.Milliseconds(), DedupMS: 1, AgeMS: c.Age.Milliseconds(), LatenessMS: c.Lateness.Milliseconds()}
	for _, s := range c.Tenants {
		t, _ := pack.ParseTenant(s)
		contract.Tenants = append(contract.Tenants, t.String())
	}
	for _, s := range c.Sources {
		contract.Sources = append(contract.Sources, catalog.Source{ID: s.ID, Fingerprint: pack.Sum([]byte(strings.TrimRight(s.URL, "/")))})
	}
	sort.Strings(contract.Tenants)
	sort.Slice(contract.Sources, func(i, j int) bool { return contract.Sources[i].ID < contract.Sources[j].ID })
	return contract
}

func validateTopology(m *catalog.Manifest, c Config) error {
	contract := archiveContract(c)
	if m.Generation == 0 {
		if len(m.Entries) != 0 {
			return errors.New("unpublished catalog contains entries")
		}
		m.Contract = contract
		return nil
	}
	if !catalog.EqualContract(m.Contract, contract) {
		return errors.New("catalog contract differs: coverage origin/window/dedup/age/lateness/tenant/source topology changes require explicit migration")
	}
	return nil
}

func existingWindow(m *catalog.Manifest, c Config, start, end int64) (bool, error) {
	tenants := map[string]bool{}
	for _, s := range c.Tenants {
		t, _ := pack.ParseTenant(s)
		tenants[t.String()] = true
	}
	seen := map[string]bool{}
	for _, e := range m.Entries {
		if !tenants[e.Tenant] || e.End <= start || e.Start >= end {
			continue
		}
		if e.Start != start || e.End != end {
			return false, errors.New("archive window overlaps committed coverage; changing window alignment or backfill is unsupported")
		}
		key := e.Tenant + "\x00" + e.SourceID
		if seen[key] {
			return false, errors.New("duplicate catalog entry")
		}
		seen[key] = true
	}
	if len(seen) == 0 {
		return false, nil
	}
	if len(seen) != len(c.Tenants)*len(c.Sources) {
		return false, errors.New("incomplete committed source/tenant window; refuse coverage advance")
	}
	return true, nil
}

func endpoint(source Source, t pack.Tenant, path string, q url.Values) string {
	return strings.TrimRight(source.URL, "/") + "/select/" + t.String() + "/prometheus/api/v1/" + path + "?" + q.Encode()
}
func seconds(ms int64) string {
	return strconv.FormatInt(ms/1000, 10) + "." + fmt.Sprintf("%03d", ms%1000)
}
func request(ctx context.Context, c Config, address string) (*http.Response, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if e != nil {
		return nil, e
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, e := c.HTTPClient.Do(req)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("source HTTP status %d", resp.StatusCode)
	}
	return resp, nil
}
func export(ctx context.Context, c Config, s Source, t pack.Tenant, start, end int64) (*pack.Index, []byte, error) {
	q := url.Values{"match[]": {`{__name__!=""}`}, "start": {seconds(start)}, "end": {seconds(end - 1)}, "deny_partial_response": {"1"}, "nocache": {"1"}}
	resp, e := request(ctx, c, endpoint(s, t, "export/native", q))
	if e != nil {
		return nil, nil, e
	}
	defer resp.Body.Close()
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "VictoriaMetrics/native") {
		return nil, nil, errors.New("unexpected native export content type")
	}
	return pack.Build(resp.Body, t, start, end, c.PackLimits)
}

type count struct{ normal, stale int }

type sample struct {
	timestamp int64
	value     float64
}

func extractSamples(idx *pack.Index, data []byte, limit int) (map[string][]sample, error) {
	out := map[string][]sample{}
	total := 0
	for si, s := range idx.Series {
		var rows []sample
		for bi, b := range s.Blocks {
			mb, e := pack.LoadBlock(idx, si, bi, data[b.Offset:b.Offset+b.Length])
			if e != nil {
				return nil, e
			}
			if mb == nil {
				continue
			}
			if e = mb.Block.UnmarshalData(); e != nil {
				return nil, e
			}
			ts, vs := mb.Block.AppendRowsWithTimeRangeFilter(nil, nil, storage.TimeRange{MinTimestamp: idx.Start, MaxTimestamp: idx.End - 1})
			total += len(ts)
			if total > limit {
				return nil, errors.New("archive sample verification limit exceeded; choose a smaller window before catalog initialization; changing a bound window requires migration to a new catalog")
			}
			for i, t := range ts {
				if math.IsNaN(vs[i]) && !decimal.IsStaleNaN(vs[i]) {
					return nil, errors.New("ordinary NaN is unsupported by the pinned VM native decimal codec")
				}
				rows = append(rows, sample{t, vs[i]})
			}
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].timestamp < rows[j].timestamp })
		ts := make([]int64, len(rows))
		vs := make([]float64, len(rows))
		for i, r := range rows {
			ts[i] = r.timestamp
			vs[i] = r.value
		}
		ts, vs = storage.DeduplicateSamples(ts, vs, 1)
		rows = rows[:len(ts)]
		for i := range rows {
			rows[i] = sample{ts[i], vs[i]}
		}
		key, _ := json.Marshal(s.Labels)
		out[string(key)] = rows
	}
	return out, nil
}
func countsOf(samples map[string][]sample) map[string]count {
	out := map[string]count{}
	for key, rows := range samples {
		n := count{}
		for _, r := range rows {
			if decimal.IsStaleNaN(r.value) {
				n.stale++
			} else {
				n.normal++
			}
		}
		out[key] = n
	}
	return out
}
func sampleCounts(idx *pack.Index, data []byte, limit int) (map[string]count, error) {
	rows, e := extractSamples(idx, data, limit)
	if e != nil {
		return nil, e
	}
	return countsOf(rows), nil
}

// rawTimestamp parses the fixed decimal seconds emitted by the pinned VM
// quicktemplate writer. It avoids float multiplication rounding timestamps.
func rawTimestamp(b []byte) (int64, error) {
	p := strings.Split(string(b), ".")
	if len(p) > 2 || len(p[0]) == 0 {
		return 0, errors.New("invalid raw timestamp")
	}
	whole, e := strconv.ParseInt(p[0], 10, 64)
	if e != nil || whole < 0 || whole > math.MaxInt64/1000 {
		return 0, errors.New("raw timestamp out of range")
	}
	var frac int64
	if len(p) == 2 {
		f := strings.TrimRight(p[1], "0")
		if len(f) > 3 {
			return 0, errors.New("raw timestamp has sub-millisecond precision")
		}
		if f != "" {
			frac, e = strconv.ParseInt(f+strings.Repeat("0", 3-len(f)), 10, 64)
			if e != nil {
				return 0, e
			}
		}
	}
	if whole*1000 > math.MaxInt64-frac {
		return 0, errors.New("raw timestamp overflow")
	}
	return whole*1000 + frac, nil
}

// verifyRaw uses QueryHandler's selector-with-range fast path. It returns the
// exact deduplicated raw series as a single matrix JSON document whose closing
// footer is emitted only after every worker succeeded. Native framing alone has
// no equivalent end marker. The comparison is exact (no numeric tolerance).
func verifyRaw(ctx context.Context, c Config, s Source, t pack.Tenant, start, end int64, want map[string][]sample) error {
	expr := fmt.Sprintf(`{__name__!=""}[%dms]`, end-start)
	q := url.Values{"query": {expr}, "time": {seconds(end - 1)}, "deny_partial_response": {"1"}, "nocache": {"1"}}
	resp, e := request(ctx, c, endpoint(s, t, "query", q))
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	return verifyRawResponse(resp.Body, maxOracleBytes, c.MaxSamples, start, end, want)
}

func verifyCounts(ctx context.Context, c Config, s Source, t pack.Tenant, start, end int64, want map[string]count) error {
	for _, mode := range []string{"count_over_time", "stale_samples_over_time"} {
		expr := fmt.Sprintf(`%s({__name__!=""}[%dms]) keep_metric_names`, mode, end-start)
		q := url.Values{"query": {expr}, "time": {seconds(end - 1)}, "deny_partial_response": {"1"}, "nocache": {"1"}}
		resp, e := request(ctx, c, endpoint(s, t, "query", q))
		if e != nil {
			return e
		}
		b, e := io.ReadAll(io.LimitReader(resp.Body, 16<<20+1))
		resp.Body.Close()
		if e != nil {
			return e
		}
		if len(b) > 16<<20 {
			return errors.New("verification response too large")
		}
		var v struct {
			Status    string   `json:"status"`
			IsPartial bool     `json:"isPartial"`
			Warnings  []string `json:"warnings"`
			Data      struct {
				ResultType string `json:"resultType"`
				Result     []struct {
					Metric map[string]string `json:"metric"`
					Value  []json.RawMessage `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if e = json.Unmarshal(b, &v); e != nil {
			return fmt.Errorf("incomplete verification JSON: %w", e)
		}
		if v.Status != "success" || v.IsPartial || len(v.Warnings) > 0 || v.Data.ResultType != "vector" {
			return errors.New("verification query did not return complete success vector")
		}
		got := map[string]int{}
		for _, r := range v.Data.Result {
			if len(r.Value) != 2 {
				return errors.New("invalid verification sample")
			}
			var value string
			if e = json.Unmarshal(r.Value[1], &value); e != nil {
				return e
			}
			n, e := strconv.ParseFloat(value, 64)
			if e != nil || n < 0 || n > float64(c.MaxSamples) || n != float64(int(n)) {
				return errors.New("invalid verification count")
			}
			key, _ := json.Marshal(r.Metric)
			if _, ok := got[string(key)]; ok {
				return errors.New("duplicate verification series")
			}
			got[string(key)] = int(n)
		}
		for key, n := range want {
			expected := n.normal
			if mode == "stale_samples_over_time" {
				expected = n.stale
			}
			if got[key] != expected {
				return fmt.Errorf("source completeness mismatch for %s: native=%d oracle=%d", mode, expected, got[key])
			}
			delete(got, key)
		}
		for _, n := range got {
			if n != 0 {
				return fmt.Errorf("source completeness mismatch: oracle includes series absent from native %s", mode)
			}
		}
	}
	return nil
}
