package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/yuuki/vmtier/internal/cold"
	"github.com/yuuki/vmtier/internal/config"
	"github.com/yuuki/vmtier/internal/gateway"
)

type streamMetricKey struct{ tenant, source string }

// Only configured stream pairs are registered. Neither timestamps nor error
// strings can add a label value, so retries cannot grow this map.
type windowFailureMetrics struct {
	mu     sync.Mutex
	counts map[streamMetricKey][3]uint64
}

var windowFailureReasons = [3]string{"error", "timeout", "canceled"}

func (m *windowFailureMetrics) init(c *config.Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts = make(map[streamMetricKey][3]uint64, len(c.Tenants)*len(c.Sources))
	for _, tenant := range c.Tenants {
		for _, source := range c.Sources {
			m.counts[streamMetricKey{tenant, source.ID}] = [3]uint64{}
		}
	}
}

func (m *windowFailureMetrics) record(tenant, source string, err error) {
	if err == nil {
		return
	}
	reason := 0
	if errors.Is(err, context.DeadlineExceeded) {
		reason = 1
	} else if errors.Is(err, context.Canceled) {
		reason = 2
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := streamMetricKey{tenant, source}
	if count, ok := m.counts[key]; ok {
		count[reason]++
		m.counts[key] = count
	}
}

func (m *windowFailureMetrics) write(w io.Writer) {
	m.mu.Lock()
	keys := make([]streamMetricKey, 0, len(m.counts))
	counts := make(map[streamMetricKey][3]uint64, len(m.counts))
	for key, count := range m.counts {
		keys = append(keys, key)
		counts[key] = count
	}
	m.mu.Unlock()
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].tenant < keys[j].tenant || (keys[i].tenant == keys[j].tenant && keys[i].source < keys[j].source)
	})
	for _, key := range keys {
		for reason, count := range counts[key] {
			fmt.Fprintf(w, "vmtier_archive_failed_windows_total{tenant=%q,source=%q,reason=%q} %d\n", key.tenant, key.source, windowFailureReasons[reason], count)
		}
	}
}

func writeCapacityMetrics(ctx context.Context, w io.Writer, reader *cold.Reader, api *gateway.API, stats *serviceStats) {
	admission := api.AdmissionStats()
	fmt.Fprintf(w, "vmtier_query_memory_limit_bytes %d\nvmtier_query_work_bytes %d\nvmtier_query_memory_reserved_bytes %d\n", admission.LimitBytes, admission.WorkBytes, admission.ReservedBytes)
	cache := reader.CacheStats()
	fmt.Fprintf(w, "vmtier_cache_used_bytes %d\nvmtier_cache_limit_bytes %d\nvmtier_cache_entries %d\nvmtier_cache_inflight_reads %d\n", cache.UsedBytes, cache.LimitBytes, cache.Entries, cache.Inflight)
	for reason, count := range api.LimitRefusals() {
		fmt.Fprintf(w, "vmtier_query_limit_refusals_total{reason=%q} %d\n", reason, count)
	}
	if stats != nil {
		stats.windowFailures.write(w)
	}
	if reader.SQLiteCatalog == nil {
		return
	}
	capacity, err := reader.SQLiteCatalog.Capacity(ctx)
	if err != nil {
		// Omit unknown gauges rather than presenting stale values or false zeroes.
		fmt.Fprintln(w, "vmtier_catalog_capacity_available 0")
		return
	}
	fmt.Fprintln(w, "vmtier_catalog_capacity_available 1")
	fmt.Fprintf(w, "vmtier_catalog_file_bytes{file=\"database\"} %d\nvmtier_catalog_file_bytes{file=\"wal\"} %d\nvmtier_catalog_file_bytes{file=\"shared_memory\"} %d\n", capacity.DatabaseBytes, capacity.WALBytes, capacity.SharedMemoryBytes)
	fmt.Fprintf(w, "vmtier_catalog_committed_object_bytes{kind=\"data\"} %d\nvmtier_catalog_committed_object_bytes{kind=\"index\"} %d\nvmtier_catalog_windows %d\nvmtier_catalog_packs %d\n", capacity.CommittedDataBytes, capacity.CommittedIndexBytes, capacity.Windows, capacity.Packs)
}
