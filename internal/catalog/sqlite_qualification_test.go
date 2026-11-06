package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yuuki/vmtier/internal/querylimit"
)

// This opt-in fixture measures catalog descriptors, not archived payloads. It
// deliberately uses PublishWindow rather than bulk SQL insertion so that the
// real transaction, validation, indexes, and coverage update path are exercised.
func TestSQLiteCatalogCapacityQualification(t *testing.T) {
	output := os.Getenv("VMTIER_CATALOG_CAPACITY_OUTPUT")
	if output == "" {
		t.Skip("run scripts/catalog_capacity.py to qualify a disposable catalog")
	}
	intEnv := func(key string, fallback, min, max int) int {
		t.Helper()
		raw := os.Getenv(key)
		if raw == "" {
			return fallback
		}
		v, err := strconv.Atoi(raw)
		if err != nil || v < min || v > max {
			t.Fatalf("%s must be in %d..%d", key, min, max)
		}
		return v
	}
	hours := intEnv("VMTIER_CATALOG_CAPACITY_HOURS", 8760, 24, 8760)
	tenants := intEnv("VMTIER_CATALOG_CAPACITY_TENANTS", 10, 1, 100)
	repeats := intEnv("VMTIER_CATALOG_CAPACITY_REPEATS", 10, 1, 100)
	ctx := context.Background()
	const hourMS int64 = 3600000
	const startMS int64 = 1735689600000 // 2025-01-01 UTC, aligned to one hour.
	c := Contract{CoverageStart: startMS, WindowMS: hourMS, AgeMS: 2 * hourMS, LatenessMS: hourMS, DedupMS: 1,
		Sources: []Source{{ID: "hot", Fingerprint: strings.Repeat("a", 64)}}}
	for i := 0; i < tenants; i++ {
		c.Tenants = append(c.Tenants, fmt.Sprintf("%d:0", i+1))
	}
	result := map[string]any{
		"started_at": time.Now().UTC().Format(time.RFC3339Nano),
		"fixture": map[string]any{"tenants": tenants, "sources": 1, "hours_per_tenant": hours, "window_ms": hourMS,
			"packs_per_window": 1, "blob_objects_written": 0, "descriptor_data_bytes": 1048576, "descriptor_index_bytes": 4096,
			"selection_repeats_per_tenant_position_duration": repeats, "publication_path": "SQLite.PublishWindow, one FULL-synchronous WAL transaction per window"},
		"runtime": map[string]any{"go_version": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "num_cpu": runtime.NumCPU(), "goos": runtime.GOOS, "goarch": runtime.GOARCH},
	}
	defer func() {
		result["finished_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["passed"] = !t.Failed()
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err == nil {
			result["process_usage"] = map[string]any{"peak_rss_bytes_linux": usage.Maxrss * 1024,
				"user_cpu_seconds":   float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6,
				"system_cpu_seconds": float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6}
		}
		b, err := json.MarshalIndent(result, "", "  ")
		if err == nil {
			err = os.WriteFile(output, append(b, '\n'), 0600)
		}
		if err != nil {
			t.Errorf("write qualification evidence: %v", err)
		}
	}()
	s, err := CreateSQLite(ctx, filepath.Join(filepath.Dir(output), "state"), "catalog-capacity", c, BlobLocator{Type: "local", Path: filepath.Join(filepath.Dir(output), "unused-blobs")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var sqliteVersion, journal string
	var synchronous int
	if err := s.db.QueryRow(`SELECT sqlite_version()`).Scan(&sqliteVersion); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	result["sqlite"] = map[string]any{"version": sqliteVersion, "journal_mode": journal, "synchronous": synchronous, "max_open_connections": 1}
	publication := func(tenantIndex, h int) Publication {
		tenant := c.Tenants[tenantIndex]
		prefix := fmt.Sprintf("tenants/%s/attempts/capacity-%06d", tenant, h)
		return Publication{Tenant: tenant, SourceID: "hot", Start: startMS + int64(h)*hourMS, End: startMS + int64(h+1)*hourMS,
			Packs: []PackRef{{DataKey: prefix + ".pack", IndexKey: prefix + ".json", DataSHA256: strings.Repeat("a", 64), IndexSHA256: strings.Repeat("b", 64), DataBytes: 1048576, IndexBytes: 4096}}}
	}
	verify := func(plan QueryPlan, tenantIndex, first, count, publishedHours int) error {
		if plan.ID != "catalog-capacity" || plan.Generation != uint64(publishedHours*tenants) || plan.CoverageEnd != startMS+int64(publishedHours)*hourMS || len(plan.Windows) != count {
			return fmt.Errorf("incorrect selected plan metadata: tenant=%d first=%d count=%d got=%+v", tenantIndex, first, count, plan)
		}
		for i, got := range plan.Windows {
			h := first + i
			want := Window{Publication: publication(tenantIndex, h), Generation: uint64(h*tenants + tenantIndex + 1)}
			if !reflect.DeepEqual(got, want) {
				return fmt.Errorf("selection leaked, omitted, or changed a descriptor: got=%+v want=%+v", got, want)
			}
		}
		return nil
	}
	checkpoints := map[int]bool{hours: true}
	if hours/10 >= 24 {
		checkpoints[hours/10] = true
	}
	var stages []map[string]any
	var publicationMS []float64
	publicationStart := time.Now()
	for h := 0; h < hours; h++ {
		for tenant := range c.Tenants {
			before := time.Now()
			generation, err := s.PublishWindow(ctx, uint64(h*tenants+tenant), publication(tenant, h))
			publicationMS = append(publicationMS, elapsedMS(before))
			if err != nil || generation != uint64(h*tenants+tenant+1) {
				t.Fatalf("publication at hour=%d tenant=%d generation=%d: %v", h, tenant, generation, err)
			}
		}
		if !checkpoints[h+1] {
			continue
		}
		publishedHours := h + 1
		stage := map[string]any{"logical_windows": publishedHours * tenants}
		var selects []map[string]any
		for _, count := range []int{1, 24} {
			var durations []float64
			for _, first := range []int{0, (publishedHours - count) / 2, publishedHours - count} {
				for tenant, name := range c.Tenants {
					for repeat := 0; repeat < repeats; repeat++ {
						selection := Selection{Tenant: name, Start: startMS + int64(first)*hourMS, End: startMS + int64(first+count)*hourMS, MaxWindows: 24, MaxPacks: 24, MaxPlanBytes: 32768}
						before := time.Now()
						plan, err := s.Select(ctx, selection)
						durations = append(durations, elapsedMS(before))
						if err != nil {
							t.Fatal(err)
						}
						if err := verify(plan, tenant, first, count, publishedHours); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			selects = append(selects, map[string]any{"selected_windows": count, "selected_packs": count, "positions": []string{"oldest", "middle", "newest"}, "latency_ms": capacityDistribution(durations)})
		}
		stage["selections"] = selects
		var capacityMS []float64
		for i := 0; i < 20; i++ {
			before := time.Now()
			capacity, err := s.Capacity(ctx)
			capacityMS = append(capacityMS, elapsedMS(before))
			if err != nil || capacity.Windows != int64(publishedHours*tenants) || capacity.Packs != capacity.Windows || capacity.CommittedDataBytes != capacity.Windows*1048576 || capacity.CommittedIndexBytes != capacity.Windows*4096 {
				t.Fatalf("capacity count mismatch: %+v, %v", capacity, err)
			}
			stage["capacity"] = capacity
		}
		stage["capacity_latency_ms"] = capacityDistribution(capacityMS)
		stages = append(stages, stage)
		t.Logf("qualified %d logical windows", publishedHours*tenants)
	}
	result["publication_latency_ms"] = capacityDistribution(publicationMS)
	var publicationTotalMS float64
	for _, ms := range publicationMS {
		publicationTotalMS += ms
	}
	result["publication_call_seconds"] = publicationTotalMS / 1000
	result["publication_calls_per_second"] = float64(len(publicationMS)) * 1000 / publicationTotalMS
	result["publication_and_checkpoint_wall_seconds"] = time.Since(publicationStart).Seconds()
	result["stages"] = stages
	result["selection_limits"] = map[string]int{"max_windows": 24, "max_packs": 24, "max_plan_bytes": 32768}

	// Check actual query planning, including both bounds and the tenant/source
	// prefix. This statement mirrors Select; selection correctness above still
	// runs through the production method rather than this diagnostic SQL.
	plans := map[string][]string{}
	for name, query := range map[string]string{
		"window_selection": `EXPLAIN QUERY PLAN SELECT id,start_ms,end_ms,generation,pack_count FROM windows WHERE tenant='1:0' AND source_id='hot' AND start_ms>=1735689600000 AND start_ms<=1735772400000 ORDER BY start_ms LIMIT 25`,
		"pack_selection":   `EXPLAIN QUERY PLAN SELECT ordinal,data_key,index_key,data_sha256,index_sha256,data_bytes,index_bytes FROM packs WHERE window_id=1 ORDER BY ordinal`,
		"capacity_sum":     `EXPLAIN QUERY PLAN SELECT count(*),coalesce(sum(data_bytes),0),coalesce(sum(index_bytes),0) FROM packs`,
	} {
		rows, err := s.db.QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plans[name] = append(plans[name], detail)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if details := strings.Join(plans["window_selection"], " "); !strings.Contains(details, "SEARCH windows USING INDEX") || !strings.Contains(details, "tenant=? AND source_id=? AND start_ms>?") || strings.Contains(details, "TEMP B-TREE") {
		t.Fatalf("window selection does not use bounded index search: %s", details)
	}
	if details := strings.Join(plans["pack_selection"], " "); !strings.Contains(details, "SEARCH packs USING PRIMARY KEY") || strings.Contains(details, "TEMP B-TREE") {
		t.Fatalf("pack selection does not use primary key: %s", details)
	}
	result["query_plans"] = plans
	var refusals []map[string]any
	for _, limits := range []Selection{
		{Tenant: c.Tenants[0], Start: startMS, End: startMS + 24*hourMS, MaxWindows: 23},
		{Tenant: c.Tenants[0], Start: startMS, End: startMS + 24*hourMS, MaxPacks: 23},
		{Tenant: c.Tenants[0], Start: startMS, End: startMS + hourMS, MaxPlanBytes: 128},
	} {
		before := time.Now()
		_, err := s.Select(ctx, limits)
		if querylimit.Reason(err) != querylimit.CatalogPlan {
			t.Fatalf("expected bounded catalog refusal: %v", err)
		}
		refusals = append(refusals, map[string]any{"limits": limits, "reason": querylimit.Reason(err), "error": err.Error(), "latency_ms": elapsedMS(before)})
	}
	result["refusals"] = refusals
	// One connection also serializes metric aggregation. Measure it competing
	// with selection; do not infer constant-time metrics from indexed reads.
	var wg sync.WaitGroup
	var concurrentSelectMS, concurrentCapacityMS []float64
	errors := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			before := time.Now()
			plan, err := s.Select(ctx, Selection{Tenant: c.Tenants[0], Start: startMS + int64(hours-24)*hourMS, End: startMS + int64(hours)*hourMS, MaxWindows: 24, MaxPacks: 24, MaxPlanBytes: 32768})
			concurrentSelectMS = append(concurrentSelectMS, elapsedMS(before))
			if err == nil {
				err = verify(plan, 0, hours-24, 24, hours)
			}
			if err != nil {
				errors <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			before := time.Now()
			_, err := s.Capacity(ctx)
			concurrentCapacityMS = append(concurrentCapacityMS, elapsedMS(before))
			if err != nil {
				errors <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	result["concurrent_selection_and_capacity"] = map[string]any{"select_24h_latency_ms": capacityDistribution(concurrentSelectMS), "capacity_latency_ms": capacityDistribution(concurrentCapacityMS), "schedule": "one sequential selection worker and one sequential capacity worker, started together without sleeps"}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	reopened, err := OpenSQLite(ctx, s.dir, s.id)
	result["reopen_including_quick_check_ms"] = elapsedMS(before)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for tenant, name := range c.Tenants {
		plan, err := reopened.Select(ctx, Selection{Tenant: name, Start: startMS + int64(hours-24)*hourMS, End: startMS + int64(hours)*hourMS})
		if err != nil {
			t.Fatal(err)
		}
		if err := verify(plan, tenant, hours-24, 24, hours); err != nil {
			t.Fatal(err)
		}
	}
	result["reopen_exact_checks"] = tenants
}

func elapsedMS(start time.Time) float64 {
	return float64(time.Since(start)) / float64(time.Millisecond)
}

func capacityDistribution(values []float64) map[string]any {
	values = append([]float64(nil), values...)
	sort.Float64s(values)
	var sum float64
	for _, v := range values {
		sum += v
	}
	quantile := func(p float64) float64 { return values[int(math.Ceil(p*float64(len(values))))-1] }
	return map[string]any{"count": len(values), "mean": sum / float64(len(values)), "p50": quantile(.5), "p95": quantile(.95), "p99": quantile(.99), "max": values[len(values)-1]}
}
