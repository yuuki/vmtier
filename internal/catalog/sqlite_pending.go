package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var _ ObjectLedger = (*SQLite)(nil)

// RecordPendingObjects records keys before their upload in a short transaction.
func (s *SQLite) RecordPendingObjects(ctx context.Context, tenant string, keys []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state, err := readSQLiteStatus(ctx, tx, s.id)
	if err != nil {
		return err
	}
	if err = validatePendingKeys(state.Contract, tenant, keys); err != nil {
		return err
	}
	created := s.now().UnixMilli()
	if created < 0 {
		return errors.New("catalog clock is before the Unix epoch")
	}
	for _, key := range keys {
		var known bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM packs WHERE data_key=? OR index_key=?) OR EXISTS(SELECT 1 FROM pending_objects WHERE object_key=?)`, key, key, key).Scan(&known); err != nil {
			return err
		}
		if known {
			return fmt.Errorf("%w: object key is already recorded", ErrInvalidPublication)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO pending_objects(object_key,created_ms,state) VALUES(?,?,?)`, key, created, pendingState); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ClaimUnpublishedObjects claims old pending keys in one transaction. The
// single connection serializes it with publication.
func (s *SQLite) ClaimUnpublishedObjects(ctx context.Context, minAge time.Duration, limit int) ([]string, error) {
	if err := validateClaim(minAge, limit); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	state, err := readSQLiteStatus(ctx, tx, s.id)
	if err != nil {
		return nil, err
	}
	keys, err := sqliteLedgerKeys(ctx, tx, `SELECT object_key FROM pending_objects WHERE state=? ORDER BY object_key LIMIT ?`, deletingState, limit)
	if err != nil {
		return nil, err
	}
	cutoff := s.now().Add(-minAge).UnixMilli()
	if len(keys) < limit && cutoff >= 0 {
		claimed, err := sqliteLedgerKeys(ctx, tx, `SELECT object_key FROM pending_objects WHERE state IN (?,?) AND created_ms<=? ORDER BY created_ms,object_key LIMIT ?`, pendingState, retiredState, cutoff, limit-len(keys))
		if err != nil {
			return nil, err
		}
		for _, key := range claimed {
			if _, err = tx.ExecContext(ctx, `UPDATE pending_objects SET state=? WHERE object_key=? AND state IN (?,?)`, deletingState, key, pendingState, retiredState); err != nil {
				return nil, err
			}
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
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM packs WHERE data_key=? OR index_key=?)`, key, key).Scan(&published); err != nil {
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

func sqliteLedgerKeys(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
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
func (s *SQLite) ForgetDeletedObjects(ctx context.Context, keys []string) error {
	keys, err := validateForget(keys)
	if err != nil || len(keys) == 0 {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = readSQLiteStatus(ctx, tx, s.id); err != nil {
		return err
	}
	for _, key := range keys {
		if _, err = tx.ExecContext(ctx, `DELETE FROM pending_objects WHERE object_key=? AND state=?`, key, deletingState); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UnpublishedObjects counts the ledger's records in each state.
func (s *SQLite) UnpublishedObjects(ctx context.Context) (UnpublishedObjectCounts, error) {
	var out UnpublishedObjectCounts
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if _, err = readSQLiteStatus(ctx, tx, s.id); err != nil {
		return out, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE state='pending'), count(*) FILTER (WHERE state='retired'), count(*) FILTER (WHERE state='deleting') FROM pending_objects`).Scan(&out.Pending, &out.Retired, &out.Deleting); err != nil {
		return UnpublishedObjectCounts{}, err
	}
	return out, tx.Commit()
}

// UpgradeSQLite explicitly upgrades an offline schema 1 catalog to schema 2 by
// adding the object ledger and the retention columns. It takes the directory lock, so a live service
// refuses it; take a backup first. Repeating a completed upgrade only verifies
// the current state.
func UpgradeSQLite(ctx context.Context, dir, id string) error {
	s, err := openSQLiteFile(ctx, dir, id)
	if err != nil {
		return err
	}
	defer s.Close()
	if err = s.upgrade(ctx); err != nil {
		return err
	}
	if _, err = s.Status(ctx); err != nil {
		return err
	}
	if err = s.quickCheck(ctx); err != nil {
		return err
	}
	return s.Close()
}

// upgrade applies the schema 1 to 2 change in one transaction.
func (s *SQLite) upgrade(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT schema_version FROM catalog_state WHERE singleton=1`).Scan(&version); err != nil {
		return fmt.Errorf("read initialized SQLite catalog: %w", err)
	}
	switch version {
	case SQLiteSchemaVersion:
		return nil
	case 1:
	default:
		return errors.New("unsupported SQLite catalog upgrade source schema")
	}
	old, err := readSQLiteStatusVersion(ctx, tx, s.id, 1)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, sqlitePendingSchema+sqliteRetentionUpgrade); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE coverage SET start_ms=?`, old.Contract.CoverageStart); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE catalog_state SET schema_version=? WHERE singleton=1`, SQLiteSchemaVersion); err != nil {
		return err
	}
	return tx.Commit()
}

var _ Retainer = (*SQLite)(nil)

// RetireWindows removes the oldest windows below before in one transaction.
func (s *SQLite) RetireWindows(ctx context.Context, before int64, limit int) (int, error) {
	if err := validateRetirement(before, limit); err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	state, err := readSQLiteStatus(ctx, tx, s.id)
	if err != nil {
		return 0, err
	}
	created := s.now().UnixMilli()
	if created < 0 {
		return 0, errors.New("catalog clock is before the Unix epoch")
	}
	retired := 0
	for _, stream := range state.Coverage {
		if retired == limit {
			break
		}
		windows, err := sqliteRetiredWindows(ctx, tx, stream, min(before, stream.End), limit-retired)
		if err != nil {
			return 0, err
		}
		start, err := nextRetirements(stream.Start, windows)
		if err != nil {
			return 0, err
		}
		for _, w := range windows {
			if err = s.retireWindow(ctx, tx, w.id, created); err != nil {
				return 0, err
			}
		}
		if start != stream.Start {
			if _, err = tx.ExecContext(ctx, `UPDATE coverage SET start_ms=? WHERE tenant=? AND source_id=?`, start, stream.Tenant, stream.SourceID); err != nil {
				return 0, err
			}
		}
		retired += len(windows)
	}
	if retired == 0 {
		return 0, nil
	}
	if _, err = tx.ExecContext(ctx, `UPDATE catalog_state SET retired_windows=retired_windows+? WHERE singleton=1`, retired); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return retired, nil
}

// sqliteRetiredWindows reads up to limit windows of a stream, in order from its
// start, that end no later than cut.
func sqliteRetiredWindows(ctx context.Context, tx *sql.Tx, stream StreamCoverage, cut int64, limit int) ([]retiredWindow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id,start_ms,end_ms FROM windows WHERE tenant=? AND source_id=? AND start_ms>=? AND end_ms<=? ORDER BY start_ms LIMIT ?`, stream.Tenant, stream.SourceID, stream.Start, cut, limit)
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

// retireWindow moves a window's object keys to the ledger and removes it.
func (s *SQLite) retireWindow(ctx context.Context, tx *sql.Tx, id, created int64) error {
	keys, err := sqliteLedgerKeys(ctx, tx, `SELECT data_key FROM packs WHERE window_id=? UNION ALL SELECT index_key FROM packs WHERE window_id=?`, id, id)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if _, err = tx.ExecContext(ctx, `INSERT INTO pending_objects(object_key,created_ms,state) VALUES(?,?,?)`, key, created, retiredState); err != nil {
			return fmt.Errorf("record retired object: %w", err)
		}
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM packs WHERE window_id=?`, id); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM windows WHERE id=?`, id)
	return err
}
