package catalog

import (
	"context"
	"database/sql"
)

var _ CapacityReader = (*Postgres)(nil)

// Capacity reads committed counts in one read-only snapshot that also checks
// the catalog state. It takes no lock and needs no lease. Every replica that
// is scraped runs it against the shared database.
func (p *Postgres) Capacity(ctx context.Context) (Capacity, error) {
	var out Capacity
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if _, err = p.readStatus(ctx, tx, false); err != nil {
		return out, err
	}
	// sum(bigint) is numeric in PostgreSQL; the bounded descriptor lengths
	// keep every total far below the int64 range.
	if err = tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(data_bytes),0)::bigint,coalesce(sum(index_bytes),0)::bigint FROM `+p.table("packs")).Scan(&out.Packs, &out.CommittedDataBytes, &out.CommittedIndexBytes); err != nil {
		return Capacity{}, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM `+p.table("windows")).Scan(&out.Windows); err != nil {
		return Capacity{}, err
	}
	return out, tx.Commit()
}
