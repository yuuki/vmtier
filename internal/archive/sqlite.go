package archive

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

// Archiver advances each configured stream independently. The owner calls
// RunPass serially and supplies the same blob store used by the read gateway.
// Retry deadlines are disposable; persistent coverage is the restart checkpoint.
type Archiver struct {
	Store         objectstore.BlobStore
	Catalog       catalog.Repository
	Config        Config
	RetryInterval time.Duration
	WindowTimeout time.Duration
	// OnWindowFailure observes an attempted window, including cancellation.
	// It must return promptly; no callback is made for a skipped retry.
	OnWindowFailure func(tenant, source string, err error)
	retryAfter      map[string]time.Time
}

// RunPass archives at most one oldest eligible window per tenant/source. A failed
// stream receives a retry delay while healthy streams can continue. StartMS and
// EndMS in Config are ignored: coverage and the current archive-age boundary own
// scheduling. A successful empty window is published without payload objects.
func (a *Archiver) RunPass(ctx context.Context, now time.Time) (*Result, error) {
	out := &Result{}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if a.Store == nil || a.Catalog == nil || a.RetryInterval < 0 || a.WindowTimeout < 0 {
		return out, errors.New("archiver requires a catalog, blob store, and nonnegative retry/timeout durations")
	}
	cfg := a.Config
	w := cfg.Window.Milliseconds()
	if w <= 0 || cfg.CoverageStart > math.MaxInt64-w {
		return out, errors.New("invalid archive coverage origin/window")
	}
	cfg.StartMS, cfg.EndMS = cfg.CoverageStart, cfg.CoverageStart+w
	if err := cfg.validateConfig(); err != nil {
		return out, err
	}
	if cfg.Retention-cfg.Age-cfg.SafetyMargin <= cfg.Window {
		return out, errors.New("require R-H > safety+window")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 2 * time.Minute}
	}
	if cfg.MaxSamples == 0 {
		cfg.MaxSamples = 5000000
	}
	state, err := a.Catalog.Status(ctx)
	if err != nil {
		return out, err
	}
	if !catalog.EqualContract(state.Contract, archiveContract(cfg)) {
		return out, errors.New("catalog contract differs from archive configuration")
	}
	eligibleEnd := (now.Add(-cfg.Age).UnixMilli() / w) * w
	if eligibleEnd <= cfg.CoverageStart {
		return out, nil
	}
	retryInterval := a.RetryInterval
	if retryInterval == 0 {
		retryInterval = 30 * time.Second
	}
	windowTimeout := a.WindowTimeout
	if windowTimeout == 0 {
		windowTimeout = 2 * time.Minute
	}
	if a.retryAfter == nil {
		a.retryAfter = map[string]time.Time{}
	}
	sources := make(map[string]Source, len(cfg.Sources))
	for _, source := range cfg.Sources {
		sources[source.ID] = source
	}
	streams := append([]catalog.StreamCoverage(nil), state.Coverage...)
	sort.Slice(streams, func(i, j int) bool {
		if streams[i].End != streams[j].End {
			return streams[i].End < streams[j].End
		}
		if streams[i].Tenant != streams[j].Tenant {
			return streams[i].Tenant < streams[j].Tenant
		}
		return streams[i].SourceID < streams[j].SourceID
	})
	runStart := time.Now()
	clock := func() time.Time { return now.Add(time.Since(runStart)) }
	generation := state.Generation
	var failures []error
	for _, stream := range streams {
		if err := ctx.Err(); err != nil {
			return out, errors.Join(append(failures, err)...)
		}
		key := stream.Tenant + "\x00" + stream.SourceID
		current := clock()
		if stream.End >= eligibleEnd || current.Before(a.retryAfter[key]) {
			continue
		}
		start, end := stream.End, stream.End+w
		if end < start {
			return out, errors.New("archive window overflows timestamp range")
		}
		fail := func(err error) {
			if a.OnWindowFailure != nil {
				a.OnWindowFailure(stream.Tenant, stream.SourceID, err)
			}
			a.retryAfter[key] = clock().Add(retryInterval)
			failures = append(failures, fmt.Errorf("archive tenant %s source %s window [%d,%d): %w", stream.Tenant, stream.SourceID, start, end, err))
			slog.Error("archive stream failed", "tenant", stream.Tenant, "source", stream.SourceID, "start_ms", start, "end_ms", end, "error", err)
		}
		// A timeout belongs to one stream, so a stalled source cannot repeatedly
		// cancel the pass before unrelated streams get a chance to progress.
		windowCtx, cancel := context.WithTimeout(ctx, windowTimeout)
		next, result, err := a.publishStream(windowCtx, cfg, sources[stream.SourceID], stream, generation, clock)
		cancel()
		if result.CommittedWindows > 0 {
			generation = next
			delete(a.retryAfter, key)
			out.CommittedWindows += result.CommittedWindows
			out.Entries += result.Entries
			out.Samples += result.Samples
			out.Bytes += result.Bytes
			slog.Info("archive stream committed", "tenant", stream.Tenant, "source", stream.SourceID, "start_ms", start, "end_ms", end, "generation", generation)
		}
		if err != nil {
			fail(err)
			if errors.Is(err, catalog.ErrLeaseLost) {
				return out, errors.Join(failures...)
			}
		}
	}
	return out, errors.Join(failures...)
}

func (a *Archiver) publishStream(ctx context.Context, cfg Config, source Source, stream catalog.StreamCoverage, generation uint64, clock func() time.Time) (uint64, *Result, error) {
	out := &Result{}
	start, end := stream.End, stream.End+cfg.Window.Milliseconds()
	if err := cfg.validateTime(start, end, clock()); err != nil {
		return generation, out, err
	}
	tenant, err := pack.ParseTenant(stream.Tenant)
	if err != nil {
		return generation, out, err
	}
	refs, samples, err := stageVerifiedPacks(ctx, a.Store, cfg, source, tenant, start, end)
	if err != nil {
		return generation, out, err
	}
	publication := catalog.Publication{Tenant: stream.Tenant, SourceID: stream.SourceID, Start: start, End: end, Packs: refs}
	var uploaded int64
	for _, ref := range refs {
		uploaded += ref.DataBytes + ref.IndexBytes
	}
	if err := cfg.validateTime(start, end, clock()); err != nil {
		return generation, out, err
	}
	if err := hook(cfg, "verified"); err != nil {
		return generation, out, err
	}
	next, err := a.Catalog.PublishWindow(ctx, generation, publication)
	if err != nil {
		return generation, out, err
	}
	out.CommittedWindows, out.Entries, out.Samples, out.Bytes = 1, 1, samples, uploaded
	return next, out, hook(cfg, "committed")
}
