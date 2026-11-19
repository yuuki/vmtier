package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var _ CapacityReader = (*SQLite)(nil)

// Capacity reads committed counts in one transaction and adds the apparent
// lengths of the local SQLite files. No checkpoint, catalog write, or
// object-store operation is performed.
func (s *SQLite) Capacity(ctx context.Context) (Capacity, error) {
	var out Capacity
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if _, err = readSQLiteStatus(ctx, tx, s.id); err != nil {
		return out, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(data_bytes),0),coalesce(sum(index_bytes),0) FROM packs`).Scan(&out.Packs, &out.CommittedDataBytes, &out.CommittedIndexBytes); err != nil {
		return Capacity{}, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM windows`).Scan(&out.Windows); err != nil {
		return Capacity{}, err
	}
	files := &SQLiteFiles{}
	for suffix, target := range map[string]*int64{"": &files.DatabaseBytes, "-wal": &files.WALBytes, "-shm": &files.SharedMemoryBytes} {
		info, err := os.Stat(filepath.Join(s.dir, SQLiteFile+suffix))
		if errors.Is(err, os.ErrNotExist) && suffix != "" {
			continue
		}
		if err != nil {
			return Capacity{}, err
		}
		if !info.Mode().IsRegular() {
			return Capacity{}, fmt.Errorf("catalog capacity requires regular SQLite files")
		}
		*target = info.Size()
	}
	if err = tx.Commit(); err != nil {
		return Capacity{}, err
	}
	out.Files = files
	return out, nil
}
