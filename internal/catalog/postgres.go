package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/lib/pq"
	"github.com/yuuki/vmtier/internal/pack"
	"github.com/yuuki/vmtier/internal/querylimit"
)

const PostgreSQLSchemaVersion = 3

// Postgres stores catalog metadata in an explicitly initialized PostgreSQL
// schema. It permits independent clients; publication is serialized by a row
// lock, not by process-local ownership. Publications additionally require a
// live database lease and fencing token; readers do not acquire ownership.
type Postgres struct {
	db     *sql.DB
	id     string
	schema string // quoted identifier, fixed when the repository is opened
	// schemaName is the unquoted schema, for catalog functions.
	schemaName string
}

var _ Repository = (*Postgres)(nil)

const postgresSchema = `
CREATE TABLE %[1]s.catalog_state (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 schema_version INTEGER NOT NULL,
 catalog_id TEXT NOT NULL,
 vm_version TEXT NOT NULL,
 generation BIGINT NOT NULL CHECK (generation >= 0),
 contract_json TEXT NOT NULL CHECK (octet_length(contract_json) <= 1048576),
 blob_json TEXT NOT NULL CHECK (octet_length(blob_json) <= 16384),
 retired_windows BIGINT NOT NULL DEFAULT 0 CHECK (retired_windows >= 0 AND retired_windows <= generation)
);
CREATE TABLE %[1]s.coverage (
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 end_ms BIGINT NOT NULL CHECK (end_ms >= 0),
 start_ms BIGINT NOT NULL DEFAULT 0 CHECK (start_ms >= 0 AND start_ms <= end_ms),
 PRIMARY KEY (tenant, source_id)
);
CREATE TABLE %[1]s.windows (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 start_ms BIGINT NOT NULL CHECK (start_ms >= 0),
 end_ms BIGINT NOT NULL CHECK (end_ms > start_ms),
 generation BIGINT NOT NULL UNIQUE CHECK (generation > 0),
 pack_count INTEGER NOT NULL CHECK (pack_count >= 0 AND pack_count <= 1024),
 digest TEXT NOT NULL CHECK (octet_length(digest) = 64),
 UNIQUE (tenant, source_id, start_ms),
 FOREIGN KEY (tenant, source_id) REFERENCES %[1]s.coverage (tenant, source_id)
);
CREATE TABLE %[1]s.object_keys (
 object_key TEXT PRIMARY KEY CHECK (octet_length(object_key) BETWEEN 1 AND 1024),
 window_id BIGINT NOT NULL REFERENCES %[1]s.windows(id),
 UNIQUE (object_key, window_id)
);
CREATE TABLE %[1]s.packs (
 window_id BIGINT NOT NULL REFERENCES %[1]s.windows(id),
 ordinal INTEGER NOT NULL CHECK (ordinal >= 0 AND ordinal < 1024),
 data_key TEXT NOT NULL UNIQUE,
 index_key TEXT NOT NULL UNIQUE,
 data_sha256 TEXT NOT NULL CHECK (octet_length(data_sha256) = 64),
 index_sha256 TEXT NOT NULL CHECK (octet_length(index_sha256) = 64),
 data_bytes BIGINT NOT NULL CHECK (data_bytes >= 0 AND data_bytes <= 67108864),
 index_bytes BIGINT NOT NULL CHECK (index_bytes > 0 AND index_bytes <= 16777216),
 PRIMARY KEY (window_id, ordinal),
 CHECK (data_key <> index_key),
 FOREIGN KEY (data_key, window_id) REFERENCES %[1]s.object_keys(object_key, window_id),
 FOREIGN KEY (index_key, window_id) REFERENCES %[1]s.object_keys(object_key, window_id)
);
` + postgresLeaseSchema + postgresPendingSchema

// postgresPendingSchema is the object ledger added by schema 3.
const postgresPendingSchema = `
CREATE TABLE %[1]s.pending_objects (
 object_key TEXT PRIMARY KEY CHECK (octet_length(object_key) BETWEEN 1 AND 1024),
 created_ms BIGINT NOT NULL CHECK (created_ms >= 0),
 state TEXT NOT NULL CHECK (state IN ('pending', 'retired', 'deleting')),
 incarnation TEXT NOT NULL DEFAULT '' CHECK (octet_length(incarnation) <= 256)
);
CREATE INDEX pending_objects_age ON %[1]s.pending_objects (state, created_ms);
CREATE INDEX windows_start ON %[1]s.windows (start_ms);
`

// postgresRetentionUpgrade adds the retention columns of schema 3 to an older
// catalog: every stream starts at the coverage origin and nothing is retired.
const postgresRetentionUpgrade = `
ALTER TABLE %[1]s.catalog_state ADD COLUMN retired_windows BIGINT NOT NULL DEFAULT 0 CHECK (retired_windows >= 0 AND retired_windows <= generation);
ALTER TABLE %[1]s.coverage ADD COLUMN start_ms BIGINT NOT NULL DEFAULT 0 CHECK (start_ms >= 0 AND start_ms <= end_ms);
`

// CreatePostgres initializes new tables and binds the immutable archive
// contract in one transaction. Provision a dedicated database or schema first;
// the DSN's search_path selects the schema. Existing tables are never replaced.
func CreatePostgres(ctx context.Context, dsn, id string, c Contract, blob BlobLocator) (_ *Postgres, err error) {
	if err = validateContract(c); err != nil {
		return nil, err
	}
	c = canonicalContract(c)
	if blob, err = NormalizeBlobLocator(blob); err != nil {
		return nil, err
	}
	p, err := connectPostgres(ctx, dsn, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = p.Close()
		}
	}()
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Do not inherit an asynchronous-commit default for authoritative metadata.
	// Configured synchronous standbys and their failover policy remain the
	// deployment's responsibility.
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = on`); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(postgresSchema, p.schema)); err != nil {
		return nil, fmt.Errorf("initialize PostgreSQL catalog (existing tables are not overwritten): %w", err)
	}
	cb, _ := json.Marshal(c)
	bb, _ := json.Marshal(blob)
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+p.table("catalog_state")+`(singleton,schema_version,catalog_id,vm_version,generation,contract_json,blob_json,retired_windows) VALUES(1,$1,$2,$3,0,$4,$5,0)`, PostgreSQLSchemaVersion, id, pack.VMVersion, string(cb), string(bb)); err != nil {
		return nil, err
	}
	for _, tenant := range c.Tenants {
		for _, source := range c.Sources {
			if _, err = tx.ExecContext(ctx, `INSERT INTO `+p.table("coverage")+`(tenant,source_id,end_ms,start_ms) VALUES($1,$2,$3,$3)`, tenant, source.ID, c.CoverageStart); err != nil {
				return nil, err
			}
		}
	}
	if err = tx.Commit(); err != nil {
		// A lost commit response is ambiguous. Do not drop tables or retry
		// initialization automatically; open the pinned identity to reconcile.
		return nil, err
	}
	return p, nil
}

// OpenPostgres opens initialized state without creating tables or changing its
// identity. Each subsequent operation checks the pinned identity and schema.
func OpenPostgres(ctx context.Context, dsn, id string) (_ *Postgres, err error) {
	p, err := connectPostgres(ctx, dsn, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = p.Close()
		}
	}()
	if _, err = p.Status(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func connectPostgres(ctx context.Context, dsn, id string) (_ *Postgres, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if !validID(id) {
		return nil, errors.New("invalid catalog ID")
	}
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("PostgreSQL catalog requires an explicit DSN")
	}
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		// Driver parse errors may contain the supplied DSN or credential.
		return nil, errors.New("cannot configure PostgreSQL catalog connection")
	}
	connector.Dialer(postgresDialer{timeout: PostgreSQLIdleTimeout})
	db := sql.OpenDB(connector)
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	// A bounded pool permits snapshot reads alongside the short publication
	// transaction. Export and object I/O never occupy these connections.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	var schema sql.NullString
	if err = db.QueryRowContext(ctx, `SELECT pg_catalog.current_schema()`).Scan(&schema); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("cannot connect to PostgreSQL catalog; verify connection settings and server availability")
	}
	if !schema.Valid || schema.String == "" {
		return nil, errors.New("PostgreSQL catalog requires an existing schema in search_path")
	}
	return &Postgres{db: db, id: id, schema: pq.QuoteIdentifier(schema.String), schemaName: schema.String}, nil
}

// table only accepts fixed names from this file; the schema is quoted once and
// qualified explicitly so a missing relation cannot fall through search_path.
func (p *Postgres) table(name string) string { return p.schema + "." + name }

func (p *Postgres) Close() error { return p.db.Close() }

func (p *Postgres) readStatus(ctx context.Context, tx *sql.Tx, lock bool) (Status, error) {
	out, err := p.readStatusVersion(ctx, tx, lock, PostgreSQLSchemaVersion)
	if err == nil {
		_, _, err = p.readLease(ctx, tx)
	}
	return out, err
}

func (p *Postgres) readStatusVersion(ctx context.Context, tx *sql.Tx, lock bool, version int) (Status, error) {
	var out Status
	var standby bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_catalog.pg_is_in_recovery()`).Scan(&standby); err != nil {
		return out, err
	}
	if standby {
		return out, errors.New("PostgreSQL catalog requires the primary; standby reads are not supported")
	}
	var generation, retired int64
	var cb, bb []byte
	// Schemas before 3 have no retention columns: nothing is retired and every
	// stream starts at the coverage origin.
	retiredColumn, startColumn := "retired_windows", "start_ms"
	if version < 3 {
		retiredColumn, startColumn = "0", "-1"
	}
	// Check the version first: another schema may lack the columns read below.
	var stored int
	if err := tx.QueryRowContext(ctx, `SELECT schema_version FROM `+p.table("catalog_state")+` WHERE singleton=1`).Scan(&stored); err != nil {
		var pgErr *pq.Error
		if errors.Is(err, sql.ErrNoRows) || (errors.As(err, &pgErr) && pgErr.Code == "42P01") {
			return out, ErrNotInitialized
		}
		return out, fmt.Errorf("read initialized PostgreSQL catalog: %w", err)
	}
	if stored != version {
		return out, schemaVersionError(stored, version)
	}
	query := `SELECT schema_version,catalog_id,vm_version,generation,contract_json,blob_json,` + retiredColumn + ` FROM ` + p.table("catalog_state") + ` WHERE singleton=1 AND octet_length(contract_json)<=1048576 AND octet_length(blob_json)<=16384`
	if lock {
		query += ` FOR UPDATE`
	}
	err := tx.QueryRowContext(ctx, query).Scan(&out.SchemaVersion, &out.ID, &out.VMVersion, &generation, &cb, &bb, &retired)
	if err != nil {
		var pgErr *pq.Error
		if errors.Is(err, sql.ErrNoRows) || (errors.As(err, &pgErr) && pgErr.Code == "42P01") {
			return out, ErrNotInitialized
		}
		return out, fmt.Errorf("read initialized PostgreSQL catalog: %w", err)
	}
	if out.SchemaVersion != version || out.VMVersion != pack.VMVersion {
		return out, fmt.Errorf("unsupported catalog schema %d or VM version %s; explicit catalog-upgrade or migration required", out.SchemaVersion, out.VMVersion)
	}
	if out.ID != p.id || !validID(out.ID) || generation < 0 || retired < 0 || retired > generation {
		return out, errors.New("invalid catalog identity or generation")
	}
	out.Generation, out.RetiredWindows = uint64(generation), uint64(retired)
	if err = strictSQLiteJSON(cb, &out.Contract); err != nil {
		return out, err
	}
	if err = validateContract(out.Contract); err != nil {
		return out, err
	}
	if err = strictSQLiteJSON(bb, &out.Blob); err != nil {
		return out, err
	}
	normalized, err := NormalizeBlobLocator(out.Blob)
	if err != nil || normalized != out.Blob {
		return out, errors.New("invalid stored blob locator")
	}
	expected := make(map[string]bool, len(out.Contract.Tenants)*len(out.Contract.Sources))
	for _, tenant := range out.Contract.Tenants {
		for _, source := range out.Contract.Sources {
			expected[tenant+"\x00"+source.ID] = true
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT tenant,source_id,end_ms,`+startColumn+` FROM `+p.table("coverage")+` ORDER BY tenant,source_id LIMIT $1`, len(expected)+1)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var c StreamCoverage
		if err = rows.Scan(&c.Tenant, &c.SourceID, &c.End, &c.Start); err != nil {
			return out, err
		}
		if version < 3 {
			c.Start = out.Contract.CoverageStart
		}
		key := c.Tenant + "\x00" + c.SourceID
		if !expected[key] || !validStreamCoverage(out.Contract, c) {
			return out, errors.New("invalid stream coverage")
		}
		delete(expected, key)
		out.Coverage = append(out.Coverage, c)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(expected) != 0 {
		return out, errors.New("missing configured stream coverage")
	}
	return out, nil
}

func (p *Postgres) Status(ctx context.Context) (Status, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return Status{}, err
	}
	defer tx.Rollback()
	out, err := p.readStatus(ctx, tx, false)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

// PublishWindow refuses unfenced writes. Use Fenced with a live archiver lease;
// keeping this method explicit prevents generic Repository callers from
// accidentally bypassing ownership when given the raw PostgreSQL handle.
func (p *Postgres) PublishWindow(ctx context.Context, expected uint64, publication Publication) (uint64, error) {
	return 0, ErrLeaseRequired
}

// publishWindow holds the state row lock from ownership validation through
// commit. All lease mutations take this lock first, so a successor cannot
// acquire ownership while this transaction publishes.
func (p *Postgres) publishWindow(ctx context.Context, expected uint64, publication Publication, lease Lease) (uint64, error) {
	if expected > math.MaxInt64 {
		return 0, errors.New("expected generation exceeds PostgreSQL integer range")
	}
	publication.Packs = append([]PackRef{}, publication.Packs...)
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
	if err = p.validateLease(ctx, tx, lease); err != nil {
		return 0, err
	}
	digest, err := validatePublication(state.Contract, publication)
	if err != nil {
		return 0, err
	}
	var oldDigest string
	err = tx.QueryRowContext(ctx, `SELECT digest FROM `+p.table("windows")+` WHERE tenant=$1 AND source_id=$2 AND start_ms=$3`, publication.Tenant, publication.SourceID, publication.Start).Scan(&oldDigest)
	if err == nil {
		if oldDigest != digest {
			return 0, fmt.Errorf("%w: committed window differs", ErrInvalidPublication)
		}
		if err = p.validateLease(ctx, tx, lease); err != nil {
			return 0, err
		}
		return state.Generation, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if state.Generation != expected {
		return 0, ErrConflict
	}
	if state.Generation == math.MaxInt64 {
		return 0, errors.New("catalog generation exhausted")
	}
	var end int64
	if err = tx.QueryRowContext(ctx, `SELECT end_ms FROM `+p.table("coverage")+` WHERE tenant=$1 AND source_id=$2`, publication.Tenant, publication.SourceID).Scan(&end); err != nil {
		return 0, err
	}
	if end != publication.Start {
		return 0, fmt.Errorf("%w: window must start at stream coverage end %d", ErrInvalidPublication, end)
	}
	if err = p.consumePending(ctx, tx, publication); err != nil {
		return 0, err
	}
	next := state.Generation + 1
	if err = p.writePublication(ctx, tx, publication, next, digest); err != nil {
		return 0, err
	}
	// Even a transaction delayed by a database lock must still have a live
	// lease at the final publication boundary. Use database wall time, not
	// transaction_timestamp(), which is fixed when the transaction starts.
	if err = p.validateLease(ctx, tx, lease); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// consumePending deletes the pending record of every referenced key. A key
// that garbage collection claimed is no longer pending: its row lock orders the
// claim and this deletion, so exactly one of them succeeds.
func (p *Postgres) consumePending(ctx context.Context, tx *sql.Tx, publication Publication) error {
	for _, ref := range publication.Packs {
		for _, key := range []string{ref.DataKey, ref.IndexKey} {
			result, err := tx.ExecContext(ctx, `DELETE FROM `+p.table("pending_objects")+` WHERE object_key=$1 AND state=$2`, key, pendingState)
			if err != nil {
				return err
			}
			if n, err := result.RowsAffected(); err != nil {
				return err
			} else if n != 1 {
				return fmt.Errorf("%w: %w", ErrInvalidPublication, ErrUnrecordedObject)
			}
		}
	}
	return nil
}

// writePublication inserts a new window with its object keys and packs, then
// advances the stream coverage and the catalog generation to next.
func (p *Postgres) writePublication(ctx context.Context, tx *sql.Tx, publication Publication, next uint64, digest string) error {
	var windowID int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO `+p.table("windows")+`(tenant,source_id,start_ms,end_ms,generation,pack_count,digest) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id`, publication.Tenant, publication.SourceID, publication.Start, publication.End, next, len(publication.Packs), digest).Scan(&windowID); err != nil {
		return err
	}
	for ordinal, ref := range publication.Packs {
		// One shared key namespace rejects reuse across data and index roles,
		// including keys published by another client in an earlier window.
		for _, key := range []string{ref.DataKey, ref.IndexKey} {
			if err := p.reserveObjectKey(ctx, tx, key, windowID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+p.table("packs")+` VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, windowID, ordinal, ref.DataKey, ref.IndexKey, ref.DataSHA256, ref.IndexSHA256, ref.DataBytes, ref.IndexBytes); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE `+p.table("coverage")+` SET end_ms=$1 WHERE tenant=$2 AND source_id=$3`, publication.End, publication.Tenant, publication.SourceID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE `+p.table("catalog_state")+` SET generation=$1 WHERE singleton=1`, next)
	return err
}

// reserveObjectKey records a published object key, which must be new.
func (p *Postgres) reserveObjectKey(ctx context.Context, tx *sql.Tx, key string, windowID int64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+p.table("object_keys")+`(object_key,window_id) VALUES($1,$2)`, key, windowID); err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("%w: published object key cannot be reused", ErrInvalidPublication)
		}
		return err
	}
	return nil
}

// Select returns a bounded plan from one snapshot. No transaction remains open
// while the caller reads immutable objects; object deletion is not implemented.
func (p *Postgres) Select(ctx context.Context, sel Selection) (QueryPlan, error) {
	var out QueryPlan
	sel, err := normalizeSelection(sel)
	if err != nil {
		return out, err
	}
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	state, err := p.readStatus(ctx, tx, false)
	if err != nil {
		return out, err
	}
	out.ID, out.Generation = state.ID, state.Generation
	out.CoverageEnd = tenantCoverageEnd(state.Coverage, sel.Tenant)
	if out.CoverageEnd == math.MaxInt64 {
		return out, errors.New("tenant is not configured")
	}
	lower, upper := windowStarts(sel, state.Contract)
	plan := newPlanReader(tx, sel, state)
	for _, source := range state.Contract.Sources {
		if err = p.selectWindows(ctx, plan, source.ID, lower, upper); err != nil {
			return out, err
		}
	}
	for _, v := range plan.selected {
		if err = plan.reservePacks(v.count); err != nil {
			return out, err
		}
		if err = p.loadPacks(ctx, plan, &v); err != nil {
			return out, err
		}
		digest, err := plan.checkPacks(v)
		if err != nil {
			return out, err
		}
		if digest != v.digest {
			return out, errors.New("stored publication digest differs")
		}
		out.Windows = append(out.Windows, v.window)
	}
	if plan.planBytes > sel.MaxPlanBytes {
		return out, querylimit.Errorf(querylimit.CatalogPlan, "query catalog plan byte limit exceeded")
	}
	return out, tx.Commit()
}

// selectWindows adds the stored windows of one source that start in
// [lower,upper] to the plan, with their publication digests.
func (p *Postgres) selectWindows(ctx context.Context, plan *planReader, sourceID string, lower, upper int64) error {
	rows, err := plan.tx.QueryContext(ctx, `SELECT id,start_ms,end_ms,generation,pack_count,digest FROM `+p.table("windows")+` WHERE tenant=$1 AND source_id=$2 AND start_ms>=$3 AND start_ms<=$4 ORDER BY start_ms LIMIT $5`, plan.sel.Tenant, sourceID, lower, upper, plan.windowLimit())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		v := plan.newWindow(sourceID)
		var generation int64
		if err := rows.Scan(&v.id, &v.window.Start, &v.window.End, &generation, &v.count, &v.digest); err != nil {
			return err
		}
		if !validHash(v.digest) {
			return errors.New("invalid stored window")
		}
		if err := plan.addWindow(v, generation); err != nil {
			return err
		}
	}
	return rows.Err()
}

// loadPacks reads the pack references of a selected window in ordinal order.
func (p *Postgres) loadPacks(ctx context.Context, plan *planReader, v *selectedWindow) error {
	rows, err := plan.tx.QueryContext(ctx, `SELECT ordinal,data_key,index_key,data_sha256,index_sha256,data_bytes,index_bytes FROM `+p.table("packs")+` WHERE window_id=$1 ORDER BY ordinal LIMIT $2`, v.id, v.count+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	return plan.readPacks(rows, v)
}
