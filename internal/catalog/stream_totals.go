package catalog

import (
	"context"
	"database/sql"
	"fmt"
)

// Each coverage row keeps the committed pack count and lengths of its stream:
// publication adds a window's packs and Cold retention subtracts them, in the
// transaction that changes the windows. A capacity observation then reads one
// row per stream instead of scanning the packs. The window count needs no
// column: a stream's windows are exactly the range [start_ms, end_ms).
//
// The totals are derived from the packs and never decide anything: no
// constraint on them can stop a publication or a retirement, restore and
// imports recompute them instead of trusting a copy, and catalog-upgrade
// recomputes them in place. A negative total, which only a drift can
// produce, makes the capacity observation fail rather than report it.
type streamTotals struct {
	packs, dataBytes, indexBytes int64
}

func (t *streamTotals) add(o streamTotals) {
	t.packs += o.packs
	t.dataBytes += o.dataBytes
	t.indexBytes += o.indexBytes
}

// publicationTotals is what a publication adds to its stream.
func publicationTotals(packs []PackRef) streamTotals {
	out := streamTotals{packs: int64(len(packs))}
	for _, ref := range packs {
		out.dataBytes += ref.DataBytes
		out.indexBytes += ref.IndexBytes
	}
	return out
}

// streamTotalsOfPacks computes the totals of coverage row c from the packs of
// its windows, as one row of three values.
func streamTotalsOfPacks(windows, packs string) string {
	return `(SELECT count(*), coalesce(sum(p.data_bytes),0), coalesce(sum(p.index_bytes),0) FROM ` + windows + ` w JOIN ` + packs + ` p ON p.window_id=w.id WHERE w.tenant=c.tenant AND w.source_id=c.source_id)`
}

// streamTotalsBackfillSQL sets every stream's totals from its packs. Schema
// upgrades, catalog-upgrade of a current catalog, restore and the PostgreSQL
// import run it; it scans the whole windows and packs tables.
func streamTotalsBackfillSQL(coverage, windows, packs string) string {
	return `UPDATE ` + coverage + ` AS c SET (committed_packs, committed_data_bytes, committed_index_bytes) = ` + streamTotalsOfPacks(windows, packs)
}

// streamTotalsSQL reads every stream's totals.
const streamTotalsSQL = `SELECT tenant, source_id, committed_packs, committed_data_bytes, committed_index_bytes FROM `

// readStreamTotals reads the rows of streamTotalsSQL by stream.
func readStreamTotals(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, coverage string) (map[[2]string]streamTotals, error) {
	rows, err := q.QueryContext(ctx, streamTotalsSQL+coverage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[[2]string]streamTotals{}
	for rows.Next() {
		var tenant, source string
		var t streamTotals
		if err := rows.Scan(&tenant, &source, &t.packs, &t.dataBytes, &t.indexBytes); err != nil {
			return nil, err
		}
		out[[2]string{tenant, source}] = t
	}
	return out, rows.Err()
}

// capacityFromCoverage builds a capacity observation from the status and the
// stream totals read in the same transaction.
func capacityFromCoverage(state Status, totals map[[2]string]streamTotals) (Capacity, error) {
	out := Capacity{Streams: make([]StreamCapacity, 0, len(state.Coverage))}
	for _, stream := range state.Coverage {
		t, ok := totals[[2]string{stream.Tenant, stream.SourceID}]
		span := stream.End - stream.Start
		if !ok || span < 0 || span%state.Contract.WindowMS != 0 {
			return Capacity{}, fmt.Errorf("catalog stream %s/%s has no valid totals", stream.Tenant, stream.SourceID)
		}
		if t.packs < 0 || t.dataBytes < 0 || t.indexBytes < 0 {
			return Capacity{}, fmt.Errorf("catalog stream %s/%s has negative totals; catalog-upgrade recomputes them from the packs", stream.Tenant, stream.SourceID)
		}
		s := StreamCapacity{Tenant: stream.Tenant, SourceID: stream.SourceID, Windows: span / state.Contract.WindowMS,
			Packs: t.packs, CommittedDataBytes: t.dataBytes, CommittedIndexBytes: t.indexBytes}
		out.Streams = append(out.Streams, s)
		out.Windows += s.Windows
		out.Packs += s.Packs
		out.CommittedDataBytes += s.CommittedDataBytes
		out.CommittedIndexBytes += s.CommittedIndexBytes
	}
	return out, nil
}
