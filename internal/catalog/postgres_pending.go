package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

var _ ObjectLedger = (*Postgres)(nil)

// postgresNowMS is the database clock in Unix milliseconds. Every client of a
// shared catalog ages pending keys by this one clock.
const postgresNowMS = `(extract(epoch FROM clock_timestamp()) * 1000)::bigint`

// RecordPendingObjects records keys before their upload. It needs no lease:
// recording grants nothing, and publication still checks the lease.
func (p *Postgres) RecordPendingObjects(ctx context.Context, tenant string, keys []string) error {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = on`); err != nil {
		return err
	}
	state, err := p.readStatus(ctx, tx, false)
	if err != nil {
		return err
	}
	if err = validatePendingKeys(state.Contract, tenant, keys); err != nil {
		return err
	}
	for _, key := range keys {
		var published bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+p.table("object_keys")+` WHERE object_key=$1)`, key).Scan(&published); err != nil {
			return err
		}
		if published {
			return fmt.Errorf("%w: object key is already recorded", ErrInvalidPublication)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+p.table("pending_objects")+`(object_key,created_ms,state) VALUES($1,`+postgresNowMS+`,$2)`, key, pendingState); err != nil {
			var pgErr *pq.Error
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return fmt.Errorf("%w: object key is already recorded", ErrInvalidPublication)
			}
			return err
		}
	}
	return tx.Commit()
}

// ClaimUnpublishedObjects claims old pending keys. Rows that a concurrent
// publication has locked are skipped; a publication that waits for a claimed
// row finds it no longer pending and fails.
func (p *Postgres) ClaimUnpublishedObjects(ctx context.Context, minAge time.Duration, limit int) ([]string, error) {
	if err := validateClaim(minAge, limit); err != nil {
		return nil, err
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = on`); err != nil {
		return nil, err
	}
	state, err := p.readStatus(ctx, tx, false)
	if err != nil {
		return nil, err
	}
	keys, err := postgresLedgerKeys(ctx, tx, `SELECT object_key FROM `+p.table("pending_objects")+` WHERE state=$1 ORDER BY object_key LIMIT $2`, deletingState, limit)
	if err != nil {
		return nil, err
	}
	if len(keys) < limit {
		claimed, err := postgresLedgerKeys(ctx, tx, `WITH old AS (
 SELECT object_key FROM `+p.table("pending_objects")+` WHERE state IN ($1,$5) AND created_ms <= `+postgresNowMS+` - $2
 ORDER BY created_ms, object_key LIMIT $3 FOR UPDATE SKIP LOCKED)
UPDATE `+p.table("pending_objects")+` AS ledger SET state=$4 FROM old WHERE ledger.object_key = old.object_key RETURNING ledger.object_key`,
			pendingState, minAge.Milliseconds(), limit-len(keys), deletingState, retiredState)
		if err != nil {
			return nil, err
		}
		keys = append(keys, claimed...)
	}
	for _, key := range keys {
		if err = checkClaimedKey(state.Contract, key); err != nil {
			return nil, err
		}
		// Publication consumes pending records, so a published key here means
		// a damaged ledger. Refuse to direct any deletion at a published object.
		var published bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+p.table("object_keys")+` WHERE object_key=$1)`, key).Scan(&published); err != nil {
			return nil, err
		}
		if published {
			return nil, errors.New("catalog object ledger claims a published object key")
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return keys, nil
}

func postgresLedgerKeys(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// ForgetDeletedObjects removes claimed records after their objects are deleted.
func (p *Postgres) ForgetDeletedObjects(ctx context.Context, keys []string) error {
	keys, err := validateForget(keys)
	if err != nil || len(keys) == 0 {
		return err
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = p.readStatus(ctx, tx, false); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM `+p.table("pending_objects")+` WHERE object_key = ANY($1) AND state=$2`, pq.Array(keys), deletingState); err != nil {
		return err
	}
	return tx.Commit()
}

// UnpublishedObjects counts the ledger's records in each state.
func (p *Postgres) UnpublishedObjects(ctx context.Context) (UnpublishedObjectCounts, error) {
	var out UnpublishedObjectCounts
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if _, err = p.readStatus(ctx, tx, false); err != nil {
		return out, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE state='pending'), count(*) FILTER (WHERE state='retired'), count(*) FILTER (WHERE state='deleting') FROM `+p.table("pending_objects")).Scan(&out.Pending, &out.Retired, &out.Deleting); err != nil {
		return UnpublishedObjectCounts{}, err
	}
	return out, tx.Commit()
}

var _ Retainer = (*Postgres)(nil)

// RetireWindows removes the oldest windows below before in one transaction.
// It takes the catalog state row lock first, as publication and lease changes
// do, so it is serialized with them. It needs no lease: it never publishes,
// and repeating it after another process retired the same windows is a no-op.
func (p *Postgres) RetireWindows(ctx context.Context, before int64, limit int) (int, error) {
	if err := validateRetirement(before, limit); err != nil {
		return 0, err
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = on`); err != nil {
		return 0, err
	}
	state, err := p.readStatus(ctx, tx, true)
	if err != nil {
		return 0, err
	}
	retired := 0
	for _, stream := range state.Coverage {
		if retired == limit {
			break
		}
		windows, err := p.retiredWindows(ctx, tx, stream, min(before, stream.End), limit-retired)
		if err != nil {
			return 0, err
		}
		start, err := nextRetirements(stream.Start, windows)
		if err != nil {
			return 0, err
		}
		for _, w := range windows {
			if err = p.retireWindow(ctx, tx, w.id); err != nil {
				return 0, err
			}
		}
		if start != stream.Start {
			if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("coverage")+` SET start_ms=$1 WHERE tenant=$2 AND source_id=$3`, start, stream.Tenant, stream.SourceID); err != nil {
				return 0, err
			}
		}
		retired += len(windows)
	}
	if retired == 0 {
		return 0, nil
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("catalog_state")+` SET retired_windows=retired_windows+$1 WHERE singleton=1`, retired); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return retired, nil
}

func (p *Postgres) retiredWindows(ctx context.Context, tx *sql.Tx, stream StreamCoverage, cut int64, limit int) ([]retiredWindow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,start_ms,end_ms FROM `+p.table("windows")+` WHERE tenant=$1 AND source_id=$2 AND start_ms>=$3 AND end_ms<=$4 ORDER BY start_ms LIMIT $5`, stream.Tenant, stream.SourceID, stream.Start, cut, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []retiredWindow
	for rows.Next() {
		var w retiredWindow
		if err = rows.Scan(&w.id, &w.start, &w.end); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// retireWindow moves a window's object keys to the ledger and removes the
// window with its packs and key reservations.
func (p *Postgres) retireWindow(ctx context.Context, tx *sql.Tx, id int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+p.table("pending_objects")+`(object_key,created_ms,state) SELECT object_key, `+postgresNowMS+`, $2 FROM `+p.table("object_keys")+` WHERE window_id=$1`, id, retiredState); err != nil {
		return fmt.Errorf("record retired objects: %w", err)
	}
	for _, table := range []string{"packs", "object_keys"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+p.table(table)+` WHERE window_id=$1`, id); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM `+p.table("windows")+` WHERE id=$1`, id)
	return err
}
