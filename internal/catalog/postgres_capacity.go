package catalog

import (
	"context"
	"database/sql"
)

var _ CapacityReader = (*Postgres)(nil)

// Capacity reads the stream totals in one read-only snapshot that also checks
// the catalog state. It takes no lock and needs no lease.
func (p *Postgres) Capacity(ctx context.Context) (Capacity, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return Capacity{}, err
	}
	defer tx.Rollback()
	state, err := p.readStatus(ctx, tx, false)
	if err != nil {
		return Capacity{}, err
	}
	totals, err := readStreamTotals(ctx, tx, p.table("coverage"))
	if err != nil {
		return Capacity{}, err
	}
	out, err := capacityFromCoverage(state, totals)
	if err != nil {
		return Capacity{}, err
	}
	return out, tx.Commit()
}
