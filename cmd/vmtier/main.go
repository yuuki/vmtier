package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/vmselectapi"
	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/cold"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/gateway"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(os.Args[1:]); err != nil {
		slog.Error("vmtier failed", "error", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: vmtier serve|archive|preflight|gateway|inspect|verify -config FILE; catalog-init|catalog|catalog-inspect|catalog-import|catalog-backup|catalog-restore; version")
	}
	switch args[0] {
	case "preflight":
		return runPreflightCommand(args)
	case "serve":
		return runServeCommand(args)
	case "catalog-inspect", "catalog-import", "catalog-backup", "catalog-restore":
		return runCatalogMaintenance(args)
	case "catalog", "catalog-init":
		return runCatalog(args)
	case "version":
		fmt.Printf("vmtier; JSON catalog v%d; SQLite schema %d; VM %s %s\n", catalog.Version, catalog.SQLiteSchemaVersion, pack.VMVersion, pack.VMRevision)
		return nil
	case "archive", "gateway", "inspect", "verify":
		return runConfigured(args)
	default:
		return fmt.Errorf("unsupported operation %q; writes/deletes/backfill/GC are not implemented", args[0])
	}
}

// runConfigured runs the operations configured by -config FILE. A SQLite
// catalog supports only offline inspect and verify.
func runConfigured(args []string) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	file := fs.String("config", "", "JSON configuration")
	start := fs.Int64("start", 0, "inclusive epoch milliseconds")
	end := fs.Int64("end", 0, "exclusive epoch milliseconds")
	listen := fs.String("listen", "127.0.0.1:8489", "trusted vmselect RPC listener")
	httpAddr := fs.String("http", "127.0.0.1:8490", "health and metrics listener")
	if e := fs.Parse(args[1:]); e != nil {
		return e
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	c, e := config.Read(*file)
	if e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if c.Catalog.Type == "sqlite" {
		return runSQLiteOffline(ctx, args[0], c)
	}
	cs, e := c.OpenCatalog()
	if e != nil {
		return e
	}
	if closer, ok := cs.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	if args[0] == "inspect" {
		m, _, err := catalog.Load(ctx, cs)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(m)
	}
	s, e := c.OpenBlobs()
	if e != nil {
		return e
	}
	if closer, ok := s.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	switch args[0] {
	case "archive":
		return runArchive(ctx, c, s, cs, *start, *end)
	case "verify":
		return runVerify(ctx, s, cs)
	case "gateway":
		return runGateway(ctx, c, s, cs, *listen, *httpAddr)
	}
	return nil
}

// runArchive archives [start,end), or with neither bound, the windows from
// the minimum tenant coverage up to the end currently required.
func runArchive(ctx context.Context, c *config.Config, s objectstore.BlobStore, cs catalog.Store, start, end int64) error {
	now := time.Now()
	if start == 0 && end == 0 {
		var err error
		if start, end, err = pendingArchiveRange(ctx, c, s, cs, now); err != nil {
			return err
		}
		if start >= end {
			return json.NewEncoder(os.Stdout).Encode(archive.Result{})
		}
	}
	if e := c.ValidateArchiveRange(start, end); e != nil {
		return e
	}
	r, e := archive.Run(ctx, s, cs, c.Archive(start, end), now)
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}

// pendingArchiveRange returns the range from the minimum coverage of the
// configured tenants to the end required at now.
func pendingArchiveRange(ctx context.Context, c *config.Config, s objectstore.BlobStore, cs catalog.Store, now time.Time) (int64, int64, error) {
	m, generation, err := catalog.Load(ctx, cs)
	if err != nil {
		return 0, 0, err
	}
	r := c.Reader(s, cs)
	if generation > 0 {
		if _, err = r.Catalog(ctx); err != nil {
			return 0, 0, err
		}
	}
	end := r.RequiredEnd(now)
	start := end
	for _, tenant := range c.Tenants {
		covered := catalog.Coverage(m, tenant, r.Sources, c.CoverageStart)
		if covered < start {
			start = covered
		}
	}
	return start, end, nil
}

// runVerify checks every committed entry's index and pack and prints the catalog.
func runVerify(ctx context.Context, s objectstore.BlobStore, cs catalog.Store) error {
	m, generation, e := catalog.Load(ctx, cs)
	if e != nil {
		return e
	}
	if generation == 0 {
		return errors.New("no committed catalog")
	}
	for _, entry := range m.Entries {
		if e := verifyEntry(ctx, s, entry); e != nil {
			return e
		}
	}
	return json.NewEncoder(os.Stdout).Encode(m)
}

func verifyEntry(ctx context.Context, s objectstore.BlobStore, entry catalog.Entry) error {
	b, e := s.Get(ctx, entry.IndexKey)
	if e != nil {
		return e
	}
	if pack.Sum(b) != entry.IndexSHA256 {
		return errors.New("index checksum mismatch")
	}
	idx, e := pack.ParseIndex(b, pack.Limits{})
	if e != nil {
		return e
	}
	if idx.Tenant.String() != entry.Tenant || idx.Start != entry.Start || idx.End != entry.End {
		return errors.New("index/catalog mismatch")
	}
	data, e := s.Get(ctx, entry.DataKey)
	if e != nil {
		return e
	}
	if int64(len(data)) != idx.Size || pack.Sum(data) != idx.SHA256 {
		return errors.New("pack size/checksum mismatch")
	}
	return loadPackBlocks(idx, data)
}

// loadPackBlocks decodes every block of a verified pack, which checks each
// block's checksum and payload.
func loadPackBlocks(idx *pack.Index, data []byte) error {
	for i, series := range idx.Series {
		for j, b := range series.Blocks {
			if _, e := pack.LoadBlock(idx, i, j, data[b.Offset:b.Offset+b.Length]); e != nil {
				return e
			}
		}
	}
	return nil
}

// runGateway serves the vmselect RPC and the health, readiness and metrics
// endpoints until ctx is done.
func runGateway(ctx context.Context, c *config.Config, s objectstore.BlobStore, cs catalog.Store, listen, httpAddr string) error {
	reader := c.Reader(s, cs)
	api := gateway.New(reader, c.GatewayLimits())
	srv, e := vmselectapi.NewServer(listen, api, vmselectapi.Limits{MaxConcurrentRequests: 8, MaxQueueDuration: 0, MaxQueueDurationFlagName: "vmtier queue timeout", MaxConcurrentRequestsFlagName: "vmtier concurrency"}, false)
	if e != nil {
		return e
	}
	defer srv.MustStop()
	mux := gatewayMux(c, s, reader, api)
	ln, e := net.Listen("tcp", httpAddr)
	if e != nil {
		return e
	}
	web := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second}
	defer web.Close()
	errCh := make(chan error, 1)
	go func() { errCh <- web.Serve(ln) }()
	slog.Info("gateway started", "vm", pack.VMVersion, "rpc", listen, "http", httpAddr)
	select {
	case <-ctx.Done():
		return nil
	case e := <-errCh:
		return e
	}
}

func gatewayMux(c *config.Config, s objectstore.BlobStore, reader *cold.Reader, api *gateway.API) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		coverage, e := reader.Ready(ctx)
		if e != nil {
			http.Error(w, e.Error(), http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(coverage)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "vmtier_query_requests_total %d\nvmtier_query_errors_total %d\nvmtier_blocks_total %d\nvmtier_cache_hits_total %d\nvmtier_cache_misses_total %d\n", api.Requests.Load(), api.Errors.Load(), api.Blocks.Load(), reader.CacheHits.Load(), reader.CacheMisses.Load())
		if st, ok := s.(interface{ Stats() objectstore.Snapshot }); ok {
			v := st.Stats()
			fmt.Fprintf(w, "vmtier_object_get_requests_total %d\nvmtier_object_range_requests_total %d\nvmtier_object_download_bytes_total %d\nvmtier_object_put_requests_total %d\n", v.Gets, v.RangeGets, v.DownloadedBytes, v.Puts)
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		writeCapacityMetrics(ctx, w, reader, api, nil)
		coverage, e := reader.Ready(ctx)
		ready := 1
		if e != nil {
			ready = 0
		}
		fmt.Fprintf(w, "vmtier_ready %d\n", ready)
		for t, end := range coverage {
			fmt.Fprintf(w, "vmtier_coverage_end_ms{tenant=%q} %d\nvmtier_archive_lag_seconds{tenant=%q} %.3f\nvmtier_retention_headroom_seconds{tenant=%q} %.3f\n", t, end, t, float64(reader.RequiredEnd(time.Now())-end)/1000, t, time.UnixMilli(end).Add(c.Retention()).Sub(time.Now()).Seconds())
		}
	})
	return mux
}

// Catalog state is initialized explicitly and never recreated on service restart.
func runCatalog(args []string) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	path := fs.String("path", "", "persistent local catalog directory")
	id := fs.String("id", "", "pinned catalog identity")
	backend := fs.String("backend", "json", "catalog-init backend: json or sqlite")
	configuration := fs.String("config", "", "validated policy/blob configuration for SQLite initialization")
	addr := fs.String("http", "127.0.0.1:8491", "catalog API listener (trusted network)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *path == "" || *id == "" {
		return errors.New("catalog requires -path and -id, with no positional arguments")
	}
	if args[0] == "catalog-init" && *backend == "sqlite" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return initializeSQLite(ctx, *configuration, *path, *id)
	}
	if *backend != "json" || *configuration != "" {
		return errors.New("only catalog-init supports -backend sqlite -config FILE; the catalog HTTP service uses JSON v2")
	}
	local, err := catalog.NewLocal(*path, *id, args[0] == "catalog-init")
	if err != nil {
		return err
	}
	defer local.Close()
	if args[0] == "catalog-init" {
		slog.Info("catalog initialized", "id", *id, "path", *path)
		return nil
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: catalog.Handler(local, os.Getenv("VMTIER_CATALOG_TOKEN")), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	defer server.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- server.Serve(ln) }()
	slog.Info("catalog authority started", "id", *id, "http", ln.Addr().String())
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	case err := <-errorsCh:
		return err
	}
}
