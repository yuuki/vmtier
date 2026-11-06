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

	_ "github.com/mattn/go-sqlite3"
	"github.com/yuuki/vmtier/internal/pack"
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
}

const sqliteSchema = `
CREATE TABLE catalog_state (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 schema_version INTEGER NOT NULL,
 catalog_id TEXT NOT NULL,
 vm_version TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK (generation >= 0),
 contract_json TEXT NOT NULL,
 blob_json TEXT NOT NULL
);
CREATE TABLE coverage (
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 end_ms INTEGER NOT NULL CHECK (end_ms >= 0),
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
	if _, err = tx.ExecContext(ctx, `INSERT INTO catalog_state VALUES(1,?,?,?,?,?,?)`, SQLiteSchemaVersion, id, pack.VMVersion, 0, string(cb), string(bb)); err != nil {
		return nil, err
	}
	for _, tenant := range c.Tenants {
		for _, source := range c.Sources {
			if _, err = tx.ExecContext(ctx, `INSERT INTO coverage VALUES(?,?,?)`, tenant, source.ID, c.CoverageStart); err != nil {
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
	defer func() {
		if err != nil {
			_ = s.Close()
		}
	}()
	if err = s.connect(ctx); err != nil {
		return nil, err
	}
	if _, err = s.Status(ctx); err != nil {
		return nil, err
	}
	var check string
	if err = s.db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&check); err != nil {
		return nil, err
	}
	if check != "ok" {
		return nil, errors.New("SQLite catalog integrity check failed")
	}
	return s, nil
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
	return &SQLite{dir: dir, id: id, lockFile: f}, nil
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
	var out SQLiteStatus
	var generation int64
	var cb, bb []byte
	err := q.QueryRowContext(ctx, `SELECT schema_version,catalog_id,vm_version,generation,contract_json,blob_json FROM catalog_state WHERE singleton=1 AND length(contract_json)<=1048576 AND length(blob_json)<=16384`).Scan(&out.SchemaVersion, &out.ID, &out.VMVersion, &generation, &cb, &bb)
	if err != nil {
		return out, fmt.Errorf("read initialized SQLite catalog: %w", err)
	}
	if out.SchemaVersion != SQLiteSchemaVersion || out.VMVersion != pack.VMVersion {
		return out, errors.New("unsupported catalog schema or VM version; explicit offline migration required")
	}
	if out.ID != expectedID || !validID(out.ID) || generation < 0 {
		return out, errors.New("invalid catalog identity or generation")
	}
	out.Generation = uint64(generation)
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
	rows, err := q.QueryContext(ctx, `SELECT tenant,source_id,end_ms FROM coverage ORDER BY tenant,source_id`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var c StreamCoverage
		if err = rows.Scan(&c.Tenant, &c.SourceID, &c.End); err != nil {
			return out, err
		}
		key := c.Tenant + "\x00" + c.SourceID
		if !expected[key] || c.End < out.Contract.CoverageStart || (c.End-out.Contract.CoverageStart)%out.Contract.WindowMS != 0 {
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
func (s *SQLite) PublishWindow(ctx context.Context, expected uint64, p Publication) (uint64, error) {
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
	out.CoverageEnd = math.MaxInt64
	for _, c := range state.Coverage {
		if c.Tenant == sel.Tenant && c.End < out.CoverageEnd {
			out.CoverageEnd = c.End
		}
	}
	if out.CoverageEnd == math.MaxInt64 {
		return out, errors.New("tenant is not configured")
	}
	lower := (sel.Start / state.Contract.WindowMS) * state.Contract.WindowMS
	upper := ((sel.End - 1) / state.Contract.WindowMS) * state.Contract.WindowMS
	if lower < state.Contract.CoverageStart {
		lower = state.Contract.CoverageStart
	}
	type selectedWindow struct {
		id     int64
		count  int
		window Window
	}
	var selected []selectedWindow
	var planBytes int64 = 128
	for _, source := range state.Contract.Sources {
		rows, err := tx.QueryContext(ctx, `SELECT id,start_ms,end_ms,generation,pack_count FROM windows WHERE tenant=? AND source_id=? AND start_ms>=? AND start_ms<=? ORDER BY start_ms LIMIT ?`, sel.Tenant, source.ID, lower, upper, sel.MaxWindows-len(selected)+1)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var v selectedWindow
			var generation int64
			v.window.Tenant, v.window.SourceID = sel.Tenant, source.ID
			if err = rows.Scan(&v.id, &v.window.Start, &v.window.End, &generation, &v.count); err != nil {
				rows.Close()
				return out, err
			}
			if generation < 1 || generation > int64(state.Generation) || v.count < 0 || v.count > SQLiteMaxPacksPerWindow || v.window.End-v.window.Start != state.Contract.WindowMS {
				rows.Close()
				return out, errors.New("invalid stored window")
			}
			v.window.Generation = uint64(generation)
			planBytes += 256 + int64(len(sel.Tenant)+len(source.ID))
			if len(selected) >= sel.MaxWindows || planBytes > sel.MaxPlanBytes {
				rows.Close()
				return out, errors.New("query catalog plan limit exceeded")
			}
			selected = append(selected, v)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
	}
	packCount := 0
	for _, v := range selected {
		if v.count > sel.MaxPacks-packCount {
			return out, errors.New("query catalog pack limit exceeded")
		}
		rows, err := tx.QueryContext(ctx, `SELECT ordinal,data_key,index_key,data_sha256,index_sha256,data_bytes,index_bytes FROM packs WHERE window_id=? ORDER BY ordinal`, v.id)
		if err != nil {
			return out, err
		}
		v.window.Packs = []PackRef{}
		for rows.Next() {
			var ordinal int
			var ref PackRef
			if err = rows.Scan(&ordinal, &ref.DataKey, &ref.IndexKey, &ref.DataSHA256, &ref.IndexSHA256, &ref.DataBytes, &ref.IndexBytes); err != nil {
				rows.Close()
				return out, err
			}
			if ordinal != len(v.window.Packs) || ordinal >= v.count {
				rows.Close()
				return out, errors.New("invalid stored pack order/count")
			}
			planBytes += 256 + int64(len(ref.DataKey)+len(ref.IndexKey)+len(ref.DataSHA256)+len(ref.IndexSHA256))
			if planBytes > sel.MaxPlanBytes {
				rows.Close()
				return out, errors.New("query catalog plan byte limit exceeded")
			}
			v.window.Packs = append(v.window.Packs, ref)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
		if len(v.window.Packs) != v.count {
			return out, errors.New("stored window is missing pack references")
		}
		if _, err = validatePublication(state.Contract, v.window.Publication); err != nil {
			return out, err
		}
		packCount += v.count
		out.Windows = append(out.Windows, v.window)
	}
	if planBytes > sel.MaxPlanBytes {
		return out, errors.New("query catalog plan byte limit exceeded")
	}
	return out, tx.Commit()
}
