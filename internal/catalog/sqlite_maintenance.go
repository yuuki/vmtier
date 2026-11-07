package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
	"golang.org/x/sys/unix"
)

const backupMetadataFile = "metadata.json"
const maxBackupMetadataBytes = 4 << 20

type sqliteBackupMetadata struct {
	Version        int          `json:"version"`
	DatabaseBytes  int64        `json:"database_bytes"`
	DatabaseSHA256 string       `json:"database_sha256"`
	Status         SQLiteStatus `json:"status"`
}

// ImportV2 imports a bound, contiguous v2 manifest into a new SQLite authority.
// It verifies indexes one at a time; payload verification remains on the read
// path and in maintenance. An unsuccessful import never publishes a partial DB.
func ImportV2(ctx context.Context, dir, newID string, manifest *Manifest, blob BlobLocator, blobs objectstore.BlobStore) (*SQLite, error) {
	if !validID(newID) || manifest == nil || newID == manifest.ID {
		return nil, errors.New("import requires a valid new catalog ID")
	}
	encoded, err := marshal(manifest)
	if err != nil {
		return nil, err
	}
	m, err := decode(encoded)
	if err != nil {
		return nil, err
	}
	if m.Generation == 0 {
		return nil, errors.New("unbound v2 catalog: initialize a new SQLite catalog instead")
	}
	if blobs == nil {
		return nil, errors.New("import requires the original blob store")
	}
	entries := append([]Entry(nil), m.Entries...)
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.Tenant != b.Tenant {
			return a.Tenant < b.Tenant
		}
		if a.SourceID != b.SourceID {
			return a.SourceID < b.SourceID
		}
		return a.Start < b.Start
	})
	ends := map[string]int64{}
	for _, e := range entries {
		key := e.Tenant + "\x00" + e.SourceID
		end, ok := ends[key]
		if !ok {
			end = m.Contract.CoverageStart
		}
		if e.Start != end {
			return nil, errors.New("v2 import requires contiguous coverage from coverage_start_ms; repair or rearchive gaps first")
		}
		ends[key] = e.End
	}
	stage, dest, err := stageMaintenanceDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stage)
	s, err := CreateSQLite(ctx, stage, newID, m.Contract, blob)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	var generation uint64
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := blobs.Get(ctx, e.IndexKey)
		if err != nil {
			return nil, fmt.Errorf("read import index %q: %w", e.IndexKey, err)
		}
		if pack.Sum(data) != e.IndexSHA256 {
			return nil, errors.New("import index checksum mismatch")
		}
		idx, err := pack.ParseIndex(data, pack.Limits{})
		if err != nil {
			return nil, fmt.Errorf("validate import index: %w", err)
		}
		if idx.Tenant.String() != e.Tenant || idx.Start != e.Start || idx.End != e.End {
			return nil, errors.New("import index disagrees with catalog tenant or logical window")
		}
		pub := Publication{Tenant: e.Tenant, SourceID: e.SourceID, Start: e.Start, End: e.End,
			Packs: []PackRef{{DataKey: e.DataKey, IndexKey: e.IndexKey, DataSHA256: idx.SHA256,
				IndexSHA256: e.IndexSHA256, DataBytes: idx.Size, IndexBytes: int64(len(data))}}}
		generation, err = s.PublishWindow(ctx, generation, pub)
		if err != nil {
			return nil, fmt.Errorf("import window: %w", err)
		}
	}
	if err := s.Close(); err != nil {
		return nil, err
	}
	if err := publishMaintenanceDirectory(stage, dest); err != nil {
		return nil, err
	}
	return OpenSQLite(ctx, dest, newID)
}

// Backup creates a new directory containing a consistent database image and its
// checksummed metadata. The caller owns the offline catalog lock. VACUUM INTO
// supplies a snapshot; metadata is read from that image, not the live connection.
func (s *SQLite) Backup(ctx context.Context, outDir string) error {
	stage, dest, err := stageMaintenanceDirectory(outDir)
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	image := filepath.Join(stage, SQLiteFile)
	if _, err := s.db.ExecContext(ctx, "VACUUM INTO ?", image); err != nil {
		return fmt.Errorf("create SQLite backup image: %w", err)
	}
	if err := os.Chmod(image, 0600); err != nil {
		return err
	}
	copyDB, err := OpenSQLite(ctx, stage, s.id)
	if err != nil {
		return err
	}
	status, err := copyDB.Status(ctx)
	closeErr := copyDB.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	checksum, size, err := hashDatabase(ctx, image)
	if err != nil {
		return err
	}
	meta := sqliteBackupMetadata{Version: 1, DatabaseBytes: size, DatabaseSHA256: checksum, Status: status}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > maxBackupMetadataBytes {
		return errors.New("backup metadata exceeds limit")
	}
	if err := writeMaintenanceFile(filepath.Join(stage, backupMetadataFile), append(data, '\n')); err != nil {
		return err
	}
	return publishMaintenanceDirectory(stage, dest)
}

// RestoreSQLite restores into a fresh directory and requires a new catalog ID.
// It validates local reference structure without reading every historical S3
// payload, leaving Hot headroom available for catch-up after recovery.
func RestoreSQLite(ctx context.Context, backupDir, destDir, newID string) (*SQLite, error) {
	meta, err := readBackupMetadata(filepath.Join(backupDir, backupMetadataFile))
	if err != nil {
		return nil, err
	}
	if !validID(newID) || newID == meta.Status.ID {
		return nil, errors.New("restore requires a valid new catalog ID")
	}
	stage, dest, err := stageMaintenanceDirectory(destDir)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stage)
	if err := copyDatabaseImage(ctx, filepath.Join(backupDir, SQLiteFile), filepath.Join(stage, SQLiteFile), meta); err != nil {
		return nil, err
	}
	s, err := OpenSQLite(ctx, stage, meta.Status.ID)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	status, err := s.Status(ctx)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(status, meta.Status) {
		return nil, errors.New("backup metadata disagrees with database state")
	}
	if err := validateRestoredReferences(ctx, s, status); err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE catalog_state SET catalog_id=? WHERE singleton=1", newID); err != nil {
		return nil, err
	}
	if err := s.Close(); err != nil {
		return nil, err
	}
	if err := publishMaintenanceDirectory(stage, dest); err != nil {
		return nil, err
	}
	return OpenSQLite(ctx, dest, newID)
}

func validateRestoredReferences(ctx context.Context, s *SQLite, status SQLiteStatus) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := checkRestoredForeignKeys(ctx, tx); err != nil {
		return err
	}
	if err := checkRestoredCoverage(ctx, tx, status); err != nil {
		return err
	}
	if err := checkRestoredWindows(ctx, tx, status); err != nil {
		return err
	}
	return tx.Commit()
}

func checkRestoredForeignKeys(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	bad := rows.Next()
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if bad {
		return errors.New("backup contains invalid foreign-key references")
	}
	return nil
}

// checkRestoredCoverage requires each stream's windows to form exactly the
// contiguous prefix of the coverage it records.
func checkRestoredCoverage(ctx context.Context, tx *sql.Tx, status SQLiteStatus) error {
	for _, stream := range status.Coverage {
		var count int64
		var first, last sql.NullInt64
		err := tx.QueryRowContext(ctx, "SELECT COUNT(*), MIN(start_ms), MAX(end_ms) FROM windows WHERE tenant=? AND source_id=?", stream.Tenant, stream.SourceID).Scan(&count, &first, &last)
		if err != nil {
			return err
		}
		want := (stream.End - status.Contract.CoverageStart) / status.Contract.WindowMS
		if count != want || (count > 0 && (!first.Valid || !last.Valid || first.Int64 != status.Contract.CoverageStart || last.Int64 != stream.End)) {
			return errors.New("backup coverage is not a contiguous prefix of its windows")
		}
	}
	return nil
}

// checkRestoredWindows validates every stored window and its packs, and
// requires one window per published generation.
func checkRestoredWindows(ctx context.Context, tx *sql.Tx, status SQLiteStatus) error {
	rows, err := tx.QueryContext(ctx, "SELECT id, tenant, source_id, start_ms, end_ms, generation, pack_count, digest FROM windows ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		if err := checkRestoredWindow(ctx, tx, rows, status); err != nil {
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != status.Generation {
		return errors.New("backup generation disagrees with published window count")
	}
	return nil
}

// checkRestoredWindow validates the current window row and recomputes the
// digest of its publication.
func checkRestoredWindow(ctx context.Context, tx *sql.Tx, rows *sql.Rows, status SQLiteStatus) error {
	var id int64
	var generation uint64
	var packCount int
	var digest string
	var pub Publication
	if err := rows.Scan(&id, &pub.Tenant, &pub.SourceID, &pub.Start, &pub.End, &generation, &packCount, &digest); err != nil {
		return err
	}
	if generation == 0 || generation > status.Generation || packCount < 0 || packCount > SQLiteMaxPacksPerWindow {
		return errors.New("backup contains invalid window metadata")
	}
	packs, err := readRestoredPacks(ctx, tx, id)
	if err != nil {
		return err
	}
	pub.Packs = packs
	if len(pub.Packs) != packCount {
		return errors.New("backup pack count disagrees with window")
	}
	actual, err := validatePublication(status.Contract, pub)
	if err != nil {
		return fmt.Errorf("invalid backup publication: %w", err)
	}
	if actual != digest {
		return errors.New("backup publication digest mismatch")
	}
	return nil
}

// readRestoredPacks returns the pack references of a window in ordinal order.
func readRestoredPacks(ctx context.Context, tx *sql.Tx, windowID int64) ([]PackRef, error) {
	rows, err := tx.QueryContext(ctx, "SELECT ordinal, data_key, index_key, data_sha256, index_sha256, data_bytes, index_bytes FROM packs WHERE window_id=? ORDER BY ordinal LIMIT ?", windowID, SQLiteMaxPacksPerWindow+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var packs []PackRef
	for rows.Next() {
		var ordinal int
		var p PackRef
		if err := rows.Scan(&ordinal, &p.DataKey, &p.IndexKey, &p.DataSHA256, &p.IndexSHA256, &p.DataBytes, &p.IndexBytes); err != nil {
			return nil, err
		}
		if ordinal != len(packs) {
			return nil, errors.New("backup contains invalid pack ordinals")
		}
		packs = append(packs, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return packs, nil
}

func stageMaintenanceDirectory(dest string) (string, string, error) {
	abs, err := filepath.Abs(dest)
	if err != nil {
		return "", "", err
	}
	if _, err := os.Lstat(abs); err == nil {
		return "", "", errors.New("destination already exists; refusing overwrite")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	stage, err := os.MkdirTemp(filepath.Dir(abs), ".vmtier-stage-")
	if err != nil {
		return "", "", fmt.Errorf("create staging directory in existing destination parent: %w", err)
	}
	return stage, abs, nil
}

func publishMaintenanceDirectory(stage, dest string) error {
	if err := syncMaintenancePath(filepath.Join(stage, SQLiteFile)); err != nil {
		return err
	}
	if err := syncMaintenancePath(stage); err != nil {
		return err
	}
	// Do not replace even an empty directory created by another operation.
	if err := unix.Renameat2(unix.AT_FDCWD, stage, unix.AT_FDCWD, dest, unix.RENAME_NOREPLACE); err != nil {
		return fmt.Errorf("publish catalog directory without overwrite: %w", err)
	}
	return syncMaintenancePath(filepath.Dir(dest))
}

func syncMaintenancePath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func writeMaintenanceFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func readBackupMetadata(path string) (sqliteBackupMetadata, error) {
	var meta sqliteBackupMetadata
	f, err := openRegularMaintenanceFile(path)
	if err != nil {
		return meta, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return meta, err
	}
	if info.Size() > maxBackupMetadataBytes {
		return meta, errors.New("backup metadata exceeds limit")
	}
	d := json.NewDecoder(io.LimitReader(f, maxBackupMetadataBytes+1))
	d.DisallowUnknownFields()
	if err := d.Decode(&meta); err != nil {
		return meta, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return meta, errors.New("trailing or oversized backup metadata")
	}
	if meta.Version != 1 || meta.DatabaseBytes <= 0 || !validHash(meta.DatabaseSHA256) || !validID(meta.Status.ID) {
		return meta, errors.New("invalid SQLite backup metadata")
	}
	return meta, nil
}

func openRegularMaintenanceFile(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("catalog backup files must be regular files")
	}
	return os.Open(path)
}

func hashDatabase(ctx context.Context, path string) (string, int64, error) {
	f, err := openRegularMaintenanceFile(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := copyWithContext(ctx, h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func copyDatabaseImage(ctx context.Context, src, dest string, meta sqliteBackupMetadata) error {
	f, err := openRegularMaintenanceFile(src)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != meta.DatabaseBytes {
		return errors.New("backup database length mismatch")
	}
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	h := sha256.New()
	n, err := copyWithContext(ctx, io.MultiWriter(out, h), f)
	if err != nil {
		return err
	}
	if n != meta.DatabaseBytes || hex.EncodeToString(h.Sum(nil)) != meta.DatabaseSHA256 {
		return errors.New("backup database checksum or length mismatch")
	}
	if err := out.Sync(); err != nil {
		return err
	}
	return out.Close()
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 64<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := src.Read(buf)
		if n > 0 {
			written, writeErr := dst.Write(buf[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}
