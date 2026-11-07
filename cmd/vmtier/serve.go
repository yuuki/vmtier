package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/vmselectapi"
	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/cold"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/gateway"
	"github.com/yuuki/vmtier/internal/objectstore"
)

func runServeCommand(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	file := fs.String("config", "", "SQLite or PostgreSQL service configuration")
	listen := fs.String("listen", "127.0.0.1:8489", "trusted vmselect RPC listener")
	httpAddr := fs.String("http", "127.0.0.1:8490", "health and metrics listener")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *file == "" {
		return errors.New("serve requires -config FILE and no positional arguments")
	}
	c, err := config.Read(*file)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runServe(ctx, c, *listen, *httpAddr)
}

func openConfiguredSQLite(ctx context.Context, c *config.Config) (*catalog.SQLite, error) {
	if c.Catalog.Type != "sqlite" {
		return nil, errors.New("serve requires catalog.type=sqlite and explicitly initialized state")
	}
	db, err := catalog.OpenSQLite(ctx, c.Catalog.Path, c.Catalog.ID)
	if err != nil {
		return nil, err
	}
	if err = validateConfiguredRepository(ctx, c, db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func openConfiguredRepository(ctx context.Context, c *config.Config) (catalog.Repository, error) {
	if c.Catalog.Type == "sqlite" {
		return openConfiguredSQLite(ctx, c)
	}
	if c.Catalog.Type != "postgres" {
		return nil, errors.New("serve requires catalog.type=sqlite or postgres and explicitly initialized state")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	dsn, err := c.PostgresDSN()
	if err != nil {
		return nil, err
	}
	db, err := catalog.OpenPostgres(ctx, dsn, c.Catalog.ID)
	if err != nil {
		return nil, err
	}
	if err = validateConfiguredRepository(ctx, c, db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func validateConfiguredRepository(ctx context.Context, c *config.Config, db catalog.Repository) error {
	status, err := db.Status(ctx)
	if err == nil && !catalog.EqualContract(status.Contract, c.ArchiveContract()) {
		err = errors.New("catalog contract differs from service configuration")
	}
	if err == nil {
		var blob catalog.BlobLocator
		blob, err = c.BlobLocator()
		if err == nil && blob != status.Blob {
			err = errors.New("catalog blob locator differs from service configuration")
		}
	}
	return err
}

// The process owns one catalog and one BlobStore instance. Archive, queries, and
// their SDK retries therefore share the same S3 request budget.
func runServe(parent context.Context, c *config.Config, rpcAddr, httpAddr string) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	db, err := openConfiguredRepository(ctx, c)
	if err != nil {
		return err
	}
	defer db.Close()
	blobs, err := c.OpenBlobs()
	if err != nil {
		return err
	}
	if closer, ok := blobs.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	reader, err := c.RepositoryReader(blobs, db)
	if err != nil {
		return err
	}
	api := gateway.NewWithContext(ctx, serviceBackend{ctx, reader}, c.GatewayLimits())
	rpc, err := vmselectapi.NewServer(rpcAddr, api, vmselectapi.Limits{
		MaxConcurrentRequests: 8, MaxQueueDuration: 0,
		MaxQueueDurationFlagName: "vmtier queue timeout", MaxConcurrentRequestsFlagName: "vmtier concurrency",
	}, false)
	if err != nil {
		return err
	}
	defer func() { cancel(); rpc.MustStop() }()
	ln, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return err
	}
	var stats serviceStats
	stats.windowFailures.init(c)
	web := &http.Server{Handler: serveMux(c, reader, api, blobs, &stats), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	defer func() {
		cancel()
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := web.Shutdown(shutdown); err != nil {
			web.Close()
		}
	}()
	webDone := make(chan error, 1)
	go func() { webDone <- web.Serve(ln) }()
	newArchiver := func(repo catalog.Repository) archivePassRunner {
		cfg := c.Archive(0, 0)
		cfg.Memory = &stats.archiveMemory
		return &archive.Archiver{Store: blobs, Catalog: repo, Config: cfg, RetryInterval: c.ArchiveInterval(), WindowTimeout: c.ArchiveTimeout(), OnWindowFailure: stats.windowFailures.record}
	}
	archiveDone := make(chan struct{})
	go func() {
		defer close(archiveDone)
		if pg, ok := db.(*catalog.Postgres); ok {
			runPostgresArchiveLoop(ctx, pg, newArchiver, c.ArchiveInterval(), c.ArchiveLeaseTTL(), &stats)
		} else {
			stats.leader.Store(true)
			defer stats.leader.Store(false)
			runArchiveLoop(ctx, newArchiver(db), c.ArchiveInterval(), &stats)
		}
	}()
	defer func() { cancel(); <-archiveDone }()
	slog.Info("service started", "catalog_type", c.Catalog.Type, "catalog_id", c.Catalog.ID, "rpc", rpcAddr, "http", ln.Addr().String())
	select {
	case <-ctx.Done():
		return nil
	case err := <-webDone:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

type serviceBackend struct {
	ctx    context.Context
	reader *cold.Reader
}

func (b serviceBackend) Search(ctx context.Context, q *storage.SearchQuery) ([]storage.MetricBlock, error) {
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()
	defer cancel()
	return b.reader.Search(child, q)
}

type serviceStats struct {
	windowFailures                            windowFailureMetrics
	archiveMemory                             archive.MemoryStats
	passes, failures, windows, samples, bytes atomic.Uint64
	lastCommit                                atomic.Int64
	leader                                    atomic.Bool
}

type archivePassRunner interface {
	RunPass(context.Context, time.Time) (*archive.Result, error)
}

func runArchiveLoop(ctx context.Context, worker archivePassRunner, interval time.Duration, stats *serviceStats) {
	for ctx.Err() == nil {
		result, err := worker.RunPass(ctx, time.Now())
		stats.passes.Add(1)
		if err != nil && ctx.Err() == nil {
			stats.failures.Add(1)
			slog.Error("archive pass failed", "error", err)
			if errors.Is(err, catalog.ErrLeaseLost) {
				return
			}
		}
		if result != nil && result.CommittedWindows > 0 {
			stats.windows.Add(uint64(result.CommittedWindows))
			stats.samples.Add(result.Samples)
			stats.bytes.Add(uint64(result.Bytes))
			stats.lastCommit.Store(time.Now().Unix())
			continue
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func serveMux(c *config.Config, reader *cold.Reader, api *gateway.API, blobs objectstore.BlobStore, stats *serviceStats) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, err := reader.Ready(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "vmtier_query_requests_total %d\nvmtier_query_errors_total %d\nvmtier_blocks_total %d\nvmtier_cache_hits_total %d\nvmtier_cache_misses_total %d\n", api.Requests.Load(), api.Errors.Load(), api.Blocks.Load(), reader.CacheHits.Load(), reader.CacheMisses.Load())
		fmt.Fprintf(w, "vmtier_archive_passes_total %d\nvmtier_archive_failed_passes_total %d\nvmtier_archive_committed_windows_total %d\nvmtier_archive_samples_total %d\nvmtier_archive_bytes_total %d\nvmtier_archive_last_commit_timestamp_seconds %d\n", stats.passes.Load(), stats.failures.Load(), stats.windows.Load(), stats.samples.Load(), stats.bytes.Load(), stats.lastCommit.Load())
		leader := 0
		if stats.leader.Load() {
			leader = 1
		}
		fmt.Fprintf(w, "vmtier_archive_leader %d\n", leader)
		if ioStats, ok := blobs.(interface{ Stats() objectstore.Snapshot }); ok {
			s := ioStats.Stats()
			fmt.Fprintf(w, "vmtier_object_get_requests_total %d\nvmtier_object_range_requests_total %d\nvmtier_object_download_bytes_total %d\nvmtier_object_put_requests_total %d\n", s.Gets, s.RangeGets, s.DownloadedBytes, s.Puts)
		}
		if budget, ok := blobs.(interface {
			BudgetStats() objectstore.BudgetSnapshot
		}); ok {
			s := budget.BudgetStats()
			fmt.Fprintf(w, "vmtier_s3_attempts_total %d\nvmtier_s3_waited_requests_total %d\nvmtier_s3_wait_seconds_total %.9f\nvmtier_s3_rejected_requests_total %d\nvmtier_s3_canceled_requests_total %d\nvmtier_s3_in_flight_requests %d\nvmtier_s3_queued_requests %d\n", s.Attempts, s.WaitedRequests, float64(s.WaitNanoseconds)/1e9, s.RejectedRequests, s.CanceledRequests, s.InFlightRequests, s.QueuedRequests)
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		writeCapacityMetrics(ctx, w, reader, api, stats)
		state, err := reader.CatalogStatus(ctx)
		ready := 0
		if err == nil {
			ready = 1
			coverage := make(map[string]int64)
			for _, stream := range state.Coverage {
				if end, ok := coverage[stream.Tenant]; !ok || stream.End < end {
					coverage[stream.Tenant] = stream.End
				}
				lag := float64(reader.RequiredEnd(time.Now())-stream.End) / 1000
				if lag < 0 {
					lag = 0
				}
				fmt.Fprintf(w, "vmtier_stream_coverage_end_ms{tenant=%q,source=%q} %d\nvmtier_stream_archive_lag_seconds{tenant=%q,source=%q} %.3f\nvmtier_stream_retention_headroom_seconds{tenant=%q,source=%q} %.3f\n", stream.Tenant, stream.SourceID, stream.End, stream.Tenant, stream.SourceID, lag, stream.Tenant, stream.SourceID, float64(stream.End+c.Retention().Milliseconds()-time.Now().UnixMilli())/1000)
			}
			for tenant, end := range coverage {
				lag := float64(reader.RequiredEnd(time.Now())-end) / 1000
				if lag > 0 {
					ready = 0
				} else {
					lag = 0
				}
				fmt.Fprintf(w, "vmtier_coverage_end_ms{tenant=%q} %d\nvmtier_archive_lag_seconds{tenant=%q} %.3f\nvmtier_retention_headroom_seconds{tenant=%q} %.3f\n", tenant, end, tenant, lag, tenant, float64(end+c.Retention().Milliseconds()-time.Now().UnixMilli())/1000)
			}
			fmt.Fprintf(w, "vmtier_catalog_generation %d\n", state.Generation)
		}
		fmt.Fprintf(w, "vmtier_ready %d\n", ready)
	})
	return mux
}
