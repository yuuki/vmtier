package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yuuki/vmtier/internal/pack"
	"github.com/yuuki/vmtier/internal/querylimit"
)

// SQLite owns one local catalog for its lifetime. It deliberately does not
// implement the MVP's whole-manifest Store interface.
type SQLite struct {
	db       *sql.DB
	dir      string
	id       string
	lockFile *os.File
	close    sync.Once
	closeErr error
	// now is the clock of the object ledger; tests may replace it.
	now func() time.Time
}

const sqliteSchema = `
CREATE TABLE catalog_state (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 schema_version INTEGER NOT NULL,
 catalog_id TEXT NOT NULL,
 vm_version TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK (generation >= 0),
 contract_json TEXT NOT NULL,
 blob_json TEXT NOT NULL,
 retired_windows INTEGER NOT NULL DEFAULT 0 CHECK (retired_windows >= 0 AND retired_windows <= generation)
);
CREATE TABLE coverage (
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 end_ms INTEGER NOT NULL CHECK (end_ms >= 0),
 start_ms INTEGER NOT NULL DEFAULT 0 CHECK (start_ms >= 0 AND start_ms <= end_ms),
 PRIMARY KEY (tenant, source_id)
) WITHOUT ROWID;
CREATE TABLE windows (
 id INTEGER PRIMARY KEY,
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 start_ms INTEGER NOT NULL CHECK (start_ms >= 0),
 end_ms INTEGER NOT NULL CHECK (end_ms > start_ms),
 generation INTEGER NOT NULL UNIQUE CHECK (generation > 0),
 pack_count INTEGER NOT NULL CHECK (pack_count >= 0 AND pack_count <= 1024),
 digest TEXT NOT NULL,
 UNIQUE (tenant, source_id, start_ms),
 FOREIGN KEY (tenant, source_id) REFERENCES coverage (tenant, source_id)
);
CREATE TABLE packs (
 window_id INTEGER NOT NULL REFERENCES windows(id),
 ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
 data_key TEXT NOT NULL UNIQUE,
 index_key TEXT NOT NULL UNIQUE,
 data_sha256 TEXT NOT NULL,
 index_sha256 TEXT NOT NULL,
 data_bytes INTEGER NOT NULL CHECK (data_bytes >= 0 AND data_bytes <= 67108864),
 index_bytes INTEGER NOT NULL CHECK (index_bytes > 0 AND index_bytes <= 16777216),
 PRIMARY KEY (window_id, ordinal)
) WITHOUT ROWID;
` + sqlitePendingSchema

// sqlitePendingSchema is the object ledger added by schema 2.
const sqlitePendingSchema = `
CREATE TABLE pending_objects (
 object_key TEXT PRIMARY KEY CHECK (length(object_key) BETWEEN 1 AND 1024),
 created_ms INTEGER NOT NULL CHECK (created_ms >= 0),
 state TEXT NOT NULL CHECK (state IN ('pending', 'retired', 'deleting'))
) WITHOUT ROWID;
CREATE INDEX pending_objects_age ON pending_objects (state, created_ms);
`

// sqliteRetentionUpgrade adds the retention columns of schema 2 to a schema 1
// catalog: every stream starts at the coverage origin and nothing is retired.
const sqliteRetentionUpgrade = `
ALTER TABLE catalog_state ADD COLUMN retired_windows INTEGER NOT NULL DEFAULT 0 CHECK (retired_windows >= 0 AND retired_windows <= generation);
ALTER TABLE coverage ADD COLUMN start_ms INTEGER NOT NULL DEFAULT 0 CHECK (start_ms >= 0 AND start_ms <= end_ms);
`

// CreateSQLite explicitly binds a new catalog contract. Existing state is never
// overwritten, including after a failed earlier initialization.
func CreateSQLite(ctx context.Context, dir, id string, c Contract, blob BlobLocator) (_ *SQLite, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if !validID(id) {
		return nil, errors.New("invalid catalog ID")
	}
	if err = validateContract(c); err != nil {
		return nil, err
	}
	c = canonicalContract(c)
	if blob, err = NormalizeBlobLocator(blob); err != nil {
		return nil, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if blob.Type == "local" && filepath.Clean(dir) == blob.Path {
		return nil, errors.New("catalog and blob directories must be distinct")
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	s, err := lockSQLite(dir, id)
	if err != nil {
		return nil, err
	}
	created := false
	defer func() {
		if err != nil {
			if created {
				if s.db != nil {
					_ = s.db.Close()
				}
				// Only remove files belonging to this failed initialization.
				// Keep ownership until cleanup finishes.
				for _, suffix := range []string{"", "-wal", "-shm"} {
					_ = os.Remove(filepath.Join(dir, SQLiteFile+suffix))
				}
			}
			_ = s.Close()
		}
	}()
	f, err := os.OpenFile(filepath.Join(dir, SQLiteFile), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("create SQLite catalog (existing state is not overwritten): %w", err)
	}
	created = true
	if err = f.Close(); err != nil {
		return nil, err
	}
	if err = s.connect(ctx); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, sqliteSchema); err != nil {
		return nil, err
	}
	cb, _ := json.Marshal(c)
	bb, _ := json.Marshal(blob)
	if _, err = tx.ExecContext(ctx, `INSERT INTO catalog_state(singleton,schema_version,catalog_id,vm_version,generation,contract_json,blob_json,retired_windows) VALUES(1,?,?,?,?,?,?,0)`, SQLiteSchemaVersion, id, pack.VMVersion, 0, string(cb), string(bb)); err != nil {
		return nil, err
	}
	for _, tenant := range c.Tenants {
		for _, source := range c.Sources {
			if _, err = tx.ExecContext(ctx, `INSERT INTO coverage(tenant,source_id,end_ms,start_ms) VALUES(?,?,?,?)`, tenant, source.ID, c.CoverageStart, c.CoverageStart); err != nil {
				return nil, err
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if err = syncSQLiteDirectory(dir); err != nil {
		return nil, err
	}
	if err = syncSQLiteDirectory(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	return s, nil
}

// OpenSQLite opens an existing, explicitly initialized authority. The database
// connection uses mode=rw, never SQLite's implicit create-on-open behavior.
func OpenSQLite(ctx context.Context, dir, id string) (_ *SQLite, err error) {
	s, err := openSQLiteFile(ctx, dir, id)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = s.Close()
		}
	}()
	if _, err = s.Status(ctx); err != nil {
		return nil, err
	}
	if err = s.quickCheck(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// openSQLiteFile locks and connects to an existing catalog file without
// reading its state, which may still have an older schema.
func openSQLiteFile(ctx context.Context, dir, id string) (_ *SQLite, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if !validID(id) {
		return nil, errors.New("invalid catalog ID")
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(filepath.Join(dir, SQLiteFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotInitialized
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return nil, errors.New("invalid or uninitialized SQLite catalog file")
	}
	s, err := lockSQLite(dir, id)
	if err != nil {
		return nil, err
	}
	if err = s.connect(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) quickCheck(ctx context.Context) error {
	var check string
	if err := s.db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&check); err != nil {
		return err
	}
	if check != "ok" {
		return errors.New("SQLite catalog integrity check failed")
	}
	return nil
}

func lockSQLite(dir, id string) (*SQLite, error) {
	f, err := os.OpenFile(filepath.Join(dir, ".catalog.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("catalog state directory is already in use: %w", err)
	}
	return &SQLite{dir: dir, id: id, lockFile: f, now: time.Now}, nil
}

func (s *SQLite) connect(ctx context.Context) error {
	u := &url.URL{Scheme: "file", Path: filepath.Join(s.dir, SQLiteFile)}
	q := url.Values{"mode": {"rw"}, "_journal_mode": {"WAL"}, "_synchronous": {"FULL"}, "_foreign_keys": {"on"}, "_busy_timeout": {"5000"}}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return err
	}
	// Metadata transactions are short. A single connection serializes the one
	// writer and bounds connection memory without holding locks during S3 I/O.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s.db = db
	return db.PingContext(ctx)
}

func (s *SQLite) Close() error {
	s.close.Do(func() {
		if s.db != nil {
			s.closeErr = s.db.Close()
		}
		if s.lockFile != nil {
			unlockErr := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
			closeErr := s.lockFile.Close()
			s.closeErr = errors.Join(s.closeErr, unlockErr, closeErr)
		}
	})
	return s.closeErr
}

func syncSQLiteDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

type sqliteQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func strictSQLiteJSON(b []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("trailing catalog metadata")
	}
	return nil
}

func readSQLiteStatus(ctx context.Context, q sqliteQueryer, expectedID string) (SQLiteStatus, error) {
	return readSQLiteStatusVersion(ctx, q, expectedID, SQLiteSchemaVersion)
}

// readSQLiteStatusVersion reads the catalog state of an exact schema version.
func readSQLiteStatusVersion(ctx context.Context, q sqliteQueryer, expectedID string, version int) (SQLiteStatus, error) {
	var out SQLiteStatus
	var generation, retired int64
	var cb, bb []byte
	// Schema 1 has no retention columns: nothing is retired and every stream
	// starts at the coverage origin.
	retiredColumn, startColumn := "retired_windows", "start_ms"
	if version == 1 {
		retiredColumn, startColumn = "0", "-1"
	}
	// Check the version first: another schema may lack the columns read below.
	var stored int
	if err := q.QueryRowContext(ctx, `SELECT schema_version FROM catalog_state WHERE singleton=1`).Scan(&stored); err != nil {
		return out, fmt.Errorf("read initialized SQLite catalog: %w", err)
	}
	if stored != version {
		return out, fmt.Errorf("unsupported catalog schema %d; explicit offline catalog-upgrade or migration required", stored)
	}
	err := q.QueryRowContext(ctx, `SELECT schema_version,catalog_id,vm_version,generation,contract_json,blob_json,`+retiredColumn+` FROM catalog_state WHERE singleton=1 AND length(contract_json)<=1048576 AND length(blob_json)<=16384`).Scan(&out.SchemaVersion, &out.ID, &out.VMVersion, &generation, &cb, &bb, &retired)
	if err != nil {
		return out, fmt.Errorf("read initialized SQLite catalog: %w", err)
	}
	if out.SchemaVersion != version || out.VMVersion != pack.VMVersion {
		return out, fmt.Errorf("unsupported catalog schema %d or VM version %s; explicit offline catalog-upgrade or migration required", out.SchemaVersion, out.VMVersion)
	}
	if out.ID != expectedID || !validID(out.ID) || generation < 0 || retired < 0 || retired > generation {
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
	rows, err := q.QueryContext(ctx, `SELECT tenant,source_id,end_ms,`+startColumn+` FROM coverage ORDER BY tenant,source_id`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var c StreamCoverage
		if err = rows.Scan(&c.Tenant, &c.SourceID, &c.End, &c.Start); err != nil {
			return out, err
		}
		if version == 1 {
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

func (s *SQLite) Status(ctx context.Context) (SQLiteStatus, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SQLiteStatus{}, err
	}
	defer tx.Rollback()
	out, err := readSQLiteStatus(ctx, tx, s.id)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

// PublishWindow commits a complete stream window. No object-store operation is
// performed here; callers must finish source and upload verification first.
// Every referenced key must have been recorded with RecordPendingObjects and
// not claimed by garbage collection; publication consumes those records.
func (s *SQLite) PublishWindow(ctx context.Context, expected uint64, p Publication) (uint64, error) {
	return s.publish(ctx, expected, p, true)
}

// publish commits a window. Imports of objects that existing catalogs already
// reference do not consume pending records.
func (s *SQLite) publish(ctx context.Context, expected uint64, p Publication, consumePending bool) (uint64, error) {
	if expected > math.MaxInt64 {
		return 0, errors.New("expected generation exceeds SQLite integer range")
	}
	p.Packs = append([]PackRef{}, p.Packs...)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	state, err := readSQLiteStatus(ctx, tx, s.id)
	if err != nil {
		return 0, err
	}
	digest, err := validatePublication(state.Contract, p)
	if err != nil {
		return 0, err
	}
	var oldDigest string
	err = tx.QueryRowContext(ctx, `SELECT digest FROM windows WHERE tenant=? AND source_id=? AND start_ms=?`, p.Tenant, p.SourceID, p.Start).Scan(&oldDigest)
	if err == nil {
		if oldDigest != digest {
			return 0, fmt.Errorf("%w: committed window differs", ErrInvalidPublication)
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
	if err = tx.QueryRowContext(ctx, `SELECT end_ms FROM coverage WHERE tenant=? AND source_id=?`, p.Tenant, p.SourceID).Scan(&end); err != nil {
		return 0, err
	}
	if end != p.Start {
		return 0, fmt.Errorf("%w: window must start at stream coverage end %d", ErrInvalidPublication, end)
	}
	for _, ref := range p.Packs {
		for _, key := range []string{ref.DataKey, ref.IndexKey} {
			var used bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM packs WHERE data_key=? OR index_key=?)`, key, key).Scan(&used); err != nil {
				return 0, err
			}
			if used {
				return 0, fmt.Errorf("%w: published object key cannot be reused", ErrInvalidPublication)
			}
			if !consumePending {
				continue
			}
			result, err := tx.ExecContext(ctx, `DELETE FROM pending_objects WHERE object_key=? AND state=?`, key, pendingState)
			if err != nil {
				return 0, err
			}
			if n, err := result.RowsAffected(); err != nil {
				return 0, err
			} else if n != 1 {
				return 0, fmt.Errorf("%w: %w", ErrInvalidPublication, ErrUnrecordedObject)
			}
		}
	}
	next := state.Generation + 1
	result, err := tx.ExecContext(ctx, `INSERT INTO windows(tenant,source_id,start_ms,end_ms,generation,pack_count,digest) VALUES(?,?,?,?,?,?,?)`, p.Tenant, p.SourceID, p.Start, p.End, next, len(p.Packs), digest)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	for i, ref := range p.Packs {
		if _, err = tx.ExecContext(ctx, `INSERT INTO packs VALUES(?,?,?,?,?,?,?,?)`, id, i, ref.DataKey, ref.IndexKey, ref.DataSHA256, ref.IndexSHA256, ref.DataBytes, ref.IndexBytes); err != nil {
			return 0, err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE coverage SET end_ms=? WHERE tenant=? AND source_id=?`, p.End, p.Tenant, p.SourceID); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE catalog_state SET generation=? WHERE singleton=1`, next); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *SQLite) Select(ctx context.Context, sel Selection) (QueryPlan, error) {
	var out QueryPlan
	var err error
	if sel, err = normalizeSelection(sel); err != nil {
		return out, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	state, err := readSQLiteStatus(ctx, tx, s.id)
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
		if err = s.selectWindows(ctx, plan, source.ID, lower, upper); err != nil {
			return out, err
		}
	}
	for _, v := range plan.selected {
		if err = plan.reservePacks(v.count); err != nil {
			return out, err
		}
		if err = s.loadPacks(ctx, plan, &v); err != nil {
			return out, err
		}
		if _, err = plan.checkPacks(v); err != nil {
			return out, err
		}
		out.Windows = append(out.Windows, v.window)
	}
	if plan.planBytes > sel.MaxPlanBytes {
		return out, querylimit.Errorf(querylimit.CatalogPlan, "query catalog plan byte limit exceeded")
	}
	return out, tx.Commit()
}

// selectWindows adds the stored windows of one source that start in
// [lower,upper] to the plan.
func (s *SQLite) selectWindows(ctx context.Context, plan *planReader, sourceID string, lower, upper int64) error {
	rows, err := plan.tx.QueryContext(ctx, `SELECT id,start_ms,end_ms,generation,pack_count FROM windows WHERE tenant=? AND source_id=? AND start_ms>=? AND start_ms<=? ORDER BY start_ms LIMIT ?`, plan.sel.Tenant, sourceID, lower, upper, plan.windowLimit())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		v := plan.newWindow(sourceID)
		var generation int64
		if err := rows.Scan(&v.id, &v.window.Start, &v.window.End, &generation, &v.count); err != nil {
			return err
		}
		if err := plan.addWindow(v, generation); err != nil {
			return err
		}
	}
	return rows.Err()
}

// loadPacks reads the pack references of a selected window in ordinal order.
func (s *SQLite) loadPacks(ctx context.Context, plan *planReader, v *selectedWindow) error {
	rows, err := plan.tx.QueryContext(ctx, `SELECT ordinal,data_key,index_key,data_sha256,index_sha256,data_bytes,index_bytes FROM packs WHERE window_id=? ORDER BY ordinal`, v.id)
	if err != nil {
		return err
	}
	defer rows.Close()
	return plan.readPacks(rows, v)
}
