package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

var _ ObjectLedger = (*Postgres)(nil)

// postgresNowMS is the database clock in Unix milliseconds. Every client of a
// shared catalog ages pending keys by this one clock.
const postgresNowMS = `(extract(epoch FROM clock_timestamp()) * 1000)::bigint`

// postgresIncarnation identifies the database lineage a ledger record was
// written in: the cluster's system identifier, the current WAL timeline, and
// the database and schema OIDs. A point-in-time recovery or standby promotion
// starts a new timeline, and a dump restored elsewhere gets other OIDs or
// another system identifier. A catalog claims only pending keys of its own
// incarnation, so a restored or copied catalog sharing the bucket never deletes
// the objects of an attempt that the original may still have published.
const postgresIncarnation = `SELECT s.system_identifier::text || '/' || substr(pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_lsn()), 1, 8) || '/' ||
 (SELECT d.oid::text FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()) || '/' ||
 (SELECT n.oid::text FROM pg_catalog.pg_namespace n WHERE n.nspname = $1)
 FROM pg_catalog.pg_control_system() s`

// incarnation reads the current database incarnation in tx.
func (p *Postgres) incarnation(ctx context.Context, tx *sql.Tx) (string, error) {
	var out sql.NullString
	if err := tx.QueryRowContext(ctx, postgresIncarnation, p.schemaName).Scan(&out); err != nil {
		return "", fmt.Errorf("identify the PostgreSQL catalog incarnation (needs pg_control_system and pg_walfile_name): %w", err)
	}
	if !out.Valid || out.String == "" || len(out.String) > 256 {
		return "", errors.New("cannot identify the PostgreSQL catalog incarnation")
	}
	return out.String, nil
}

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
	incarnation, err := p.incarnation(ctx, tx)
	if err != nil {
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
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+p.table("pending_objects")+`(object_key,created_ms,state,incarnation) VALUES($1,`+postgresNowMS+`,$2,$3)`, key, pendingState, incarnation); err != nil {
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
	incarnation, err := p.incarnation(ctx, tx)
	if err != nil {
		return nil, err
	}
	// Claimed keys order by their last claim or deferral, unclaimed keys by
	// their record time: a key whose deletion keeps failing moves behind the
	// keys recorded before its last attempt and cannot starve them. Pending
	// keys of another database incarnation are never claimed.
	keys, err := postgresLedgerKeys(ctx, tx, `WITH old AS (
 SELECT object_key FROM `+p.table("pending_objects")+`
 WHERE state=$4 OR (state=$1 AND incarnation=$5 AND created_ms <= `+postgresNowMS+` - $2) OR (state=$6 AND created_ms <= `+postgresNowMS+` - $7)
 ORDER BY created_ms, object_key LIMIT $3 FOR UPDATE SKIP LOCKED)
UPDATE `+p.table("pending_objects")+` AS ledger
 SET state=$4, created_ms=CASE WHEN ledger.state=$4 THEN ledger.created_ms ELSE `+postgresNowMS+` END
 FROM old WHERE ledger.object_key = old.object_key RETURNING ledger.object_key`,
		pendingState, minAge.Milliseconds(), limit, deletingState, incarnation, retiredState, retiredAge(minAge).Milliseconds())
	if err != nil {
		return nil, err
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

// DeferClaimedObjects moves claimed keys behind the others.
func (p *Postgres) DeferClaimedObjects(ctx context.Context, keys []string) error {
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
	if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("pending_objects")+` SET created_ms=greatest(created_ms, `+postgresNowMS+`) WHERE object_key = ANY($1) AND state=$2`, pq.Array(keys), deletingState); err != nil {
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
	incarnation, err := p.incarnation(ctx, tx)
	if err != nil {
		return out, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE state='pending' AND incarnation=$1), count(*) FILTER (WHERE state='retired'), count(*) FILTER (WHERE state='deleting'), count(*) FILTER (WHERE state='pending' AND incarnation<>$1) FROM `+p.table("pending_objects"), incarnation).Scan(&out.Pending, &out.Retired, &out.Deleting, &out.Foreign); err != nil {
		return UnpublishedObjectCounts{}, err
	}
	return out, tx.Commit()
}

var _ Retainer = (*Postgres)(nil)

// RetireWindows removes the oldest windows below the retention boundary of
// the database clock in one transaction. It takes the catalog state row lock
// first, as publication and lease changes do, so it is serialized with them.
// It needs no lease: it never publishes, and repeating it after another
// process retired the same windows is a no-op.
func (p *Postgres) RetireWindows(ctx context.Context, retention time.Duration, limit int) (int, error) {
	if err := validateRetirement(retention, limit); err != nil {
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
	var now int64
	if err = tx.QueryRowContext(ctx, `SELECT `+postgresNowMS).Scan(&now); err != nil {
		return 0, err
	}
	last, ok := retirementBound(state.Contract, now, retention)
	if !ok {
		return 0, nil
	}
	windows, err := p.retiredWindows(ctx, tx, last, limit)
	if err != nil {
		return 0, err
	}
	starts, err := streamRetirements(state.Coverage, windows)
	if err != nil {
		return 0, err
	}
	incarnation, err := p.incarnation(ctx, tx)
	if err != nil {
		return 0, err
	}
	for _, w := range windows {
		if err = p.retireWindow(ctx, tx, w.id, incarnation); err != nil {
			return 0, err
		}
	}
	for key, start := range starts {
		tenant, source, _ := strings.Cut(key, "\x00")
		if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("coverage")+` SET start_ms=$1 WHERE tenant=$2 AND source_id=$3`, start, tenant, source); err != nil {
			return 0, err
		}
	}
	if len(windows) == 0 {
		return 0, nil
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("catalog_state")+` SET retired_windows=retired_windows+$1 WHERE singleton=1`, len(windows)); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(windows), nil
}

// retiredWindows reads up to limit windows that start at or before last,
// oldest first, through the start index.
func (p *Postgres) retiredWindows(ctx context.Context, tx *sql.Tx, last int64, limit int) ([]retiredWindow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,tenant,source_id,start_ms,end_ms FROM `+p.table("windows")+` WHERE start_ms<=$1 ORDER BY start_ms,tenant,source_id LIMIT $2`, last, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []retiredWindow
	for rows.Next() {
		var w retiredWindow
		if err = rows.Scan(&w.id, &w.tenant, &w.sourceID, &w.start, &w.end); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// retireWindow moves a window's object keys to the ledger and removes the
// window with its packs and key reservations.
func (p *Postgres) retireWindow(ctx context.Context, tx *sql.Tx, id int64, incarnation string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+p.table("pending_objects")+`(object_key,created_ms,state,incarnation) SELECT object_key, `+postgresNowMS+`, $2, ($3)::text FROM `+p.table("object_keys")+` WHERE window_id=$1`, id, retiredState, incarnation); err != nil {
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

var _ StreamAdder = (*Postgres)(nil)

// AddStreams adds the streams of new tenants or sources in one transaction
// under the catalog state row lock, which orders it with publications. It
// needs no lease: a running archiver with the earlier configuration keeps
// archiving the streams it knows, whose contract the new one covers.
func (p *Postgres) AddStreams(ctx context.Context, next Contract) ([]StreamCoverage, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = on`); err != nil {
		return nil, err
	}
	state, err := p.readStatus(ctx, tx, true)
	if err != nil {
		return nil, err
	}
	var now int64
	if err = tx.QueryRowContext(ctx, `SELECT `+postgresNowMS).Scan(&now); err != nil {
		return nil, err
	}
	added, err := newStreams(state.Contract, next, streamOrigin(state.Contract, now))
	if err != nil {
		return nil, err
	}
	for _, stream := range added {
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+p.table("coverage")+`(tenant,source_id,end_ms,start_ms) VALUES($1,$2,$3,$4)`, stream.Tenant, stream.SourceID, stream.End, stream.Start); err != nil {
			return nil, err
		}
	}
	encoded, err := json.Marshal(canonicalContract(next))
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("catalog_state")+` SET contract_json=$1 WHERE singleton=1`, string(encoded)); err != nil {
		return nil, err
	}
	if _, err = p.readStatus(ctx, tx, false); err != nil {
		return nil, err
	}
	return added, tx.Commit()
}
