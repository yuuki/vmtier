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
 SELECT object_key FROM `+p.table("pending_objects")+` WHERE state=$1 AND created_ms <= `+postgresNowMS+` - $2
 ORDER BY created_ms, object_key LIMIT $3 FOR UPDATE SKIP LOCKED)
UPDATE `+p.table("pending_objects")+` AS ledger SET state=$4 FROM old WHERE ledger.object_key = old.object_key RETURNING ledger.object_key`,
			pendingState, minAge.Milliseconds(), limit-len(keys), deletingState)
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
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE state='pending'), count(*) FILTER (WHERE state='deleting') FROM `+p.table("pending_objects")).Scan(&out.Pending, &out.Deleting); err != nil {
		return UnpublishedObjectCounts{}, err
	}
	return out, tx.Commit()
}
