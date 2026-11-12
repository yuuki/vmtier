package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	ErrLeaseRequired = errors.New("PostgreSQL publication requires an archiver lease")
	ErrLeaseHeld     = errors.New("archiver lease is held by another owner")
	ErrLeaseLost     = errors.New("archiver lease is expired or no longer owned")
)

// Lease identifies one acquisition of the catalog's archiver ownership. A
// process must use a unique Owner for its lifetime. ExpiresAt is informational;
// publication checks expiry against database time, never the caller's clock.
type Lease struct {
	CatalogID string    `json:"catalog_id"`
	Owner     string    `json:"owner"`
	Token     uint64    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

const postgresLeaseSchema = `
CREATE TABLE %[1]s.archive_lease (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 owner TEXT NOT NULL CHECK (octet_length(owner) <= 128),
 token BIGINT NOT NULL CHECK (token >= 0),
 expires_at TIMESTAMPTZ NOT NULL,
 CHECK (owner = '' OR token > 0)
);
INSERT INTO %[1]s.archive_lease VALUES (1, '', 0, 'epoch');
`

func validateLeaseTTL(ttl time.Duration) error {
	if ttl < 10*time.Millisecond || ttl > 24*time.Hour || ttl%time.Millisecond != 0 {
		return errors.New("archiver lease TTL must be whole milliseconds from 10ms through 24h")
	}
	return nil
}

// beginLeaseChange imposes the same lock order as publication: catalog_state
// first, then lease metadata. No database lock is retained during archive I/O.
func (p *Postgres) beginLeaseChange(ctx context.Context) (*sql.Tx, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = on`); err == nil {
		_, err = p.readStatus(ctx, tx, true)
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func (p *Postgres) readLease(ctx context.Context, tx *sql.Tx) (Lease, bool, error) {
	out := Lease{CatalogID: p.id}
	var token int64
	var active bool
	err := tx.QueryRowContext(ctx, `SELECT owner,token,expires_at,expires_at>clock_timestamp() FROM `+p.table("archive_lease")+` WHERE singleton=1`).Scan(&out.Owner, &token, &out.ExpiresAt, &active)
	if err != nil {
		return out, false, fmt.Errorf("read initialized archiver lease: %w", err)
	}
	if token < 0 || (out.Owner != "" && (!validID(out.Owner) || token == 0)) {
		return out, false, errors.New("invalid stored archiver lease")
	}
	out.Token = uint64(token)
	return out, active && out.Owner != "", nil
}

// AcquireLease acquires an unowned or expired lease with a fresh fencing token.
// Retrying a lost acquisition response with the same live owner returns the
// existing token without extending its deadline. Distinct processes must never
// share an owner identifier.
func (p *Postgres) AcquireLease(ctx context.Context, owner string, ttl time.Duration) (Lease, error) {
	if !validID(owner) {
		return Lease{}, errors.New("invalid archiver lease owner")
	}
	if err := validateLeaseTTL(ttl); err != nil {
		return Lease{}, err
	}
	tx, err := p.beginLeaseChange(ctx)
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback()
	current, active, err := p.readLease(ctx, tx)
	if err != nil {
		return Lease{}, err
	}
	if active {
		if current.Owner != owner {
			return Lease{}, ErrLeaseHeld
		}
		return current, tx.Commit()
	}
	if current.Token == math.MaxInt64 {
		return Lease{}, errors.New("archiver fencing token exhausted")
	}
	out := Lease{CatalogID: p.id, Owner: owner, Token: current.Token + 1}
	err = tx.QueryRowContext(ctx, `UPDATE `+p.table("archive_lease")+` SET owner=$1,token=$2,expires_at=clock_timestamp()+($3 * interval '1 millisecond') WHERE singleton=1 RETURNING expires_at`, out.Owner, out.Token, ttl.Milliseconds()).Scan(&out.ExpiresAt)
	if err != nil {
		return Lease{}, err
	}
	return out, tx.Commit()
}

// RenewLease extends only a currently live acquisition. Expiry cannot be undone
// by renewal; a new acquisition must receive a greater token.
func (p *Postgres) RenewLease(ctx context.Context, lease Lease, ttl time.Duration) (Lease, error) {
	if err := validateLeaseTTL(ttl); err != nil {
		return Lease{}, err
	}
	if !p.validLeaseIdentity(lease) {
		return Lease{}, ErrLeaseLost
	}
	tx, err := p.beginLeaseChange(ctx)
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback()
	out := lease
	err = tx.QueryRowContext(ctx, `UPDATE `+p.table("archive_lease")+` SET expires_at=clock_timestamp()+($1 * interval '1 millisecond') WHERE singleton=1 AND owner=$2 AND token=$3 AND expires_at>clock_timestamp() RETURNING expires_at`, ttl.Milliseconds(), lease.Owner, lease.Token).Scan(&out.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Lease{}, ErrLeaseLost
	}
	if err != nil {
		return Lease{}, err
	}
	return out, tx.Commit()
}

// ReleaseLease relinquishes this acquisition without resetting the fencing
// counter. Retrying release is harmless until another owner acquires the lease.
func (p *Postgres) ReleaseLease(ctx context.Context, lease Lease) error {
	if !p.validLeaseIdentity(lease) {
		return ErrLeaseLost
	}
	tx, err := p.beginLeaseChange(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, _, err := p.readLease(ctx, tx)
	if err != nil {
		return err
	}
	if current.Token != lease.Token || (current.Owner != "" && current.Owner != lease.Owner) {
		return ErrLeaseLost
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("archive_lease")+` SET owner='',expires_at='epoch' WHERE singleton=1`); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *Postgres) validLeaseIdentity(lease Lease) bool {
	return lease.CatalogID == p.id && validID(lease.Owner) && lease.Token > 0 && lease.Token <= math.MaxInt64
}

func (p *Postgres) validateLease(ctx context.Context, tx *sql.Tx, lease Lease) error {
	if !p.validLeaseIdentity(lease) {
		return ErrLeaseLost
	}
	current, active, err := p.readLease(ctx, tx)
	if err != nil {
		return err
	}
	if !active || current.Owner != lease.Owner || current.Token != lease.Token {
		return ErrLeaseLost
	}
	return nil
}

type fencedPostgres struct {
	*Postgres
	lease Lease
}

// Fenced returns a view that publishes only while the supplied acquisition is
// live. Renewal retains its token, so existing views observe renewed DB expiry.
// Closing a view is a no-op; the runtime owns and closes the underlying handle.
func (p *Postgres) Fenced(lease Lease) Repository {
	return &fencedPostgres{Postgres: p, lease: lease}
}

func (p *fencedPostgres) PublishWindow(ctx context.Context, expected uint64, publication Publication) (uint64, error) {
	return p.Postgres.publishWindow(ctx, expected, publication, p.lease)
}

func (p *fencedPostgres) Close() error { return nil }

// UpgradePostgres explicitly upgrades schema 1 or 2 to schema 3. Stop legacy
// writers first. The state lock waits for an in-flight publication, then
// atomically adds ownership metadata (schema 2), the object ledger and the
// retention columns (schema 3) and changes the version so legacy writers
// reject new work. Repeating a
// completed upgrade verifies the current state without changing it.
func UpgradePostgres(ctx context.Context, dsn, id string) (err error) {
	p, err := connectPostgres(ctx, dsn, id)
	if err != nil {
		return err
	}
	defer p.Close()
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = on`); err != nil {
		return err
	}
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT schema_version FROM `+p.table("catalog_state")+` WHERE singleton=1 FOR UPDATE`).Scan(&version); err != nil {
		return err
	}
	if version == PostgreSQLSchemaVersion {
		if _, err = p.readStatus(ctx, tx, false); err != nil {
			return err
		}
		return tx.Commit()
	}
	if version != 1 && version != 2 {
		return errors.New("unsupported PostgreSQL catalog upgrade source schema")
	}
	old, err := p.readStatusVersion(ctx, tx, false, version)
	if err != nil {
		return err
	}
	if version == 1 {
		if _, err = tx.ExecContext(ctx, fmt.Sprintf(postgresLeaseSchema, p.schema)); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(postgresPendingSchema+postgresRetentionUpgrade, p.schema)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("coverage")+` SET start_ms=$1`, old.Contract.CoverageStart); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+p.table("catalog_state")+` SET schema_version=$1 WHERE singleton=1`, PostgreSQLSchemaVersion); err != nil {
		return err
	}
	if _, err = p.readStatus(ctx, tx, false); err != nil {
		return err
	}
	return tx.Commit()
}
