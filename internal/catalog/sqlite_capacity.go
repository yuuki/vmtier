package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SQLiteCapacity separates local file lengths from committed immutable object
// lengths. Object lengths come from publication descriptors, never bucket LIST;
// they exclude failed/unreferenced attempts and do not measure bucket usage.
type SQLiteCapacity struct {
	DatabaseBytes, WALBytes, SharedMemoryBytes int64
	CommittedDataBytes, CommittedIndexBytes    int64
	Windows, Packs                             int64
}

// Capacity reads committed counts in one transaction. File sizes are apparent
// lengths (including reusable/free pages), not allocated filesystem blocks.
// No checkpoint, catalog write, or object-store operation is performed.
func (s *SQLite) Capacity(ctx context.Context) (SQLiteCapacity, error) {
	var out SQLiteCapacity
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if _, err = readSQLiteStatus(ctx, tx, s.id); err != nil {
		return out, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(data_bytes),0),coalesce(sum(index_bytes),0) FROM packs`).Scan(&out.Packs, &out.CommittedDataBytes, &out.CommittedIndexBytes); err != nil {
		return SQLiteCapacity{}, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM windows`).Scan(&out.Windows); err != nil {
		return SQLiteCapacity{}, err
	}
	for suffix, target := range map[string]*int64{"": &out.DatabaseBytes, "-wal": &out.WALBytes, "-shm": &out.SharedMemoryBytes} {
		info, err := os.Stat(filepath.Join(s.dir, SQLiteFile+suffix))
		if errors.Is(err, os.ErrNotExist) && suffix != "" {
			continue
		}
		if err != nil {
			return SQLiteCapacity{}, err
		}
		if !info.Mode().IsRegular() {
			return SQLiteCapacity{}, fmt.Errorf("catalog capacity requires regular SQLite files")
		}
		*target = info.Size()
	}
	if err = tx.Commit(); err != nil {
		return SQLiteCapacity{}, err
	}
	return out, nil
}
