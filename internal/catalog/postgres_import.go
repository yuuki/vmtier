package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// ImportSQLiteToPostgres copies a stopped SQLite authority into a fresh
// PostgreSQL schema under a new catalog identity. The caller must exclusively
// own source and keep it open until this function returns; OpenSQLite's lifetime
// lock prevents an external serve process from using the source directory.
//
// Only metadata is copied. Existing object locations, publication digests,
// generations, and empty windows are preserved. The destination is created and
// verified in one transaction, so a failed import never exposes partial state.
// A lost commit response is ambiguous: inspect the new identity before retrying.
func ImportSQLiteToPostgres(ctx context.Context, source *SQLite, dsn, newID string) (_ *Postgres, err error) {
	if source == nil || source.db == nil || source.lockFile == nil {
		return nil, errors.New("PostgreSQL import requires an exclusively owned SQLite catalog")
	}
	if !validID(newID) || newID == source.id {
		return nil, errors.New("PostgreSQL import requires a valid new catalog ID")
	}
	before, err := source.Status(ctx)
	if err != nil {
		return nil, err
	}
	if err = validateRestoredReferences(ctx, source, before); err != nil {
		return nil, fmt.Errorf("validate SQLite import source: %w", err)
	}
	// The SQLite repository has one connection. Holding this transaction also
	// prevents an accidental same-handle publication during metadata transfer.
	snapshot, err := source.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer snapshot.Rollback()
	state, err := readSQLiteStatus(ctx, snapshot, source.id)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(before, state) {
		return nil, errors.New("SQLite source changed during validation; stop its writer before import")
	}
	p, err := connectPostgres(ctx, dsn, newID)
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
	if err = p.initializeImport(ctx, tx, newID, state); err != nil {
		return nil, err
	}
	imported, err := p.copySQLiteWindows(ctx, tx, snapshot, state)
	if err != nil {
		return nil, err
	}
	// The source writer is stopped, so its pending keys belong to attempts that
	// can no longer publish. Copying them lets the new catalog collect them.
	if imported.ledger, err = p.copySQLiteLedger(ctx, tx, snapshot, state); err != nil {
		return nil, err
	}
	want := state
	want.ID, want.SchemaVersion = newID, PostgreSQLSchemaVersion
	actual, err := p.readStatus(ctx, tx, false)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(actual, want) {
		return nil, errors.New("PostgreSQL import status differs from SQLite source")
	}
	if err = verifyPostgresImport(ctx, tx, p, state.Contract, imported); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit PostgreSQL import; inspect destination identity before retrying: %w", err)
	}
	return p, nil
}

// initializeImport creates the destination tables and copies the catalog
// state and stream coverage of the SQLite snapshot under the new identity.
func (p *Postgres) initializeImport(ctx context.Context, tx *sql.Tx, newID string, state Status) error {
	if _, err := tx.ExecContext(ctx, `SET LOCAL synchronous_commit = on`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(postgresSchema, p.schema)); err != nil {
		return fmt.Errorf("initialize PostgreSQL import destination (existing tables are not overwritten): %w", err)
	}
	contractJSON, _ := json.Marshal(state.Contract)
	blobJSON, _ := json.Marshal(state.Blob)
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+p.table("catalog_state")+` VALUES(1,$1,$2,$3,$4,$5,$6)`, PostgreSQLSchemaVersion, newID, state.VMVersion, state.Generation, string(contractJSON), string(blobJSON)); err != nil {
		return err
	}
	for _, stream := range state.Coverage {
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+p.table("coverage")+` VALUES($1,$2,$3)`, stream.Tenant, stream.SourceID, stream.End); err != nil {
			return err
		}
	}
	return nil
}

// importedWindows counts the windows and packs that an import copied, and
// hashes their encoded references for comparison with the destination.
type importedWindows struct {
	windows, packs, ledger uint64
	hash                   []byte
}

// copySQLiteLedger copies the object ledger of the SQLite snapshot with its
// states and ages, after the windows, so that published keys are known.
func (p *Postgres) copySQLiteLedger(ctx context.Context, tx, snapshot *sql.Tx, state Status) (uint64, error) {
	rows, err := snapshot.QueryContext(ctx, `SELECT object_key,created_ms,state FROM pending_objects ORDER BY object_key`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		var key, recordState string
		var created int64
		if err = rows.Scan(&key, &created, &recordState); err != nil {
			return 0, err
		}
		if (recordState != pendingState && recordState != deletingState) || created < 0 || checkClaimedKey(state.Contract, key) != nil {
			return 0, errors.New("SQLite import source has an invalid object ledger record")
		}
		var published bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+p.table("object_keys")+` WHERE object_key=$1)`, key).Scan(&published); err != nil {
			return 0, err
		}
		if published {
			return 0, errors.New("SQLite import source object ledger records a published object key")
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+p.table("pending_objects")+` (object_key,created_ms,state) VALUES($1,$2,$3)`, key, created, recordState); err != nil {
			return 0, err
		}
		count++
	}
	return count, rows.Err()
}

// copySQLiteWindows copies the windows of the SQLite snapshot in generation
// order, requiring them to be contiguous and to reach each stream's coverage.
func (p *Postgres) copySQLiteWindows(ctx context.Context, tx, snapshot *sql.Tx, state Status) (importedWindows, error) {
	var out importedWindows
	ends := make(map[string]int64, len(state.Coverage))
	for _, stream := range state.Coverage {
		ends[stream.Tenant+"\x00"+stream.SourceID] = state.Contract.CoverageStart
	}
	rows, err := snapshot.QueryContext(ctx, `SELECT id,tenant,source_id,start_ms,end_ms,generation,pack_count,digest FROM windows ORDER BY generation`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	sourceHash := sha256.New()
	encoder := json.NewEncoder(sourceHash)
	for rows.Next() {
		window, digest, err := readImportWindow(ctx, snapshot, rows, "packs", "?", state.Contract)
		if err != nil {
			return out, fmt.Errorf("read SQLite import window: %w", err)
		}
		key := window.Tenant + "\x00" + window.SourceID
		if window.Generation != out.windows+1 || window.Start != ends[key] {
			return out, errors.New("SQLite import source has noncontiguous generations or stream coverage")
		}
		ends[key] = window.End
		if err = p.insertImportWindow(ctx, tx, window, digest); err != nil {
			return out, err
		}
		if err = encoder.Encode(window); err != nil {
			return out, err
		}
		out.windows++
		out.packs += uint64(len(window.Packs))
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	rows.Close()
	if out.windows != state.Generation {
		return out, errors.New("SQLite import window count differs from generation")
	}
	for _, stream := range state.Coverage {
		if ends[stream.Tenant+"\x00"+stream.SourceID] != stream.End {
			return out, errors.New("SQLite import windows differ from recorded coverage")
		}
	}
	out.hash = sourceHash.Sum(nil)
	return out, nil
}

// insertImportWindow inserts one copied window with its object keys and packs.
func (p *Postgres) insertImportWindow(ctx context.Context, tx *sql.Tx, window Window, digest string) error {
	var id int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO `+p.table("windows")+` (tenant,source_id,start_ms,end_ms,generation,pack_count,digest) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id`, window.Tenant, window.SourceID, window.Start, window.End, window.Generation, len(window.Packs), digest).Scan(&id); err != nil {
		return err
	}
	for ordinal, ref := range window.Packs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+p.table("object_keys")+` (object_key,window_id) VALUES($1,$3),($2,$3)`, ref.DataKey, ref.IndexKey, id); err != nil {
			return fmt.Errorf("import object key reservation: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+p.table("packs")+` (window_id,ordinal,data_key,index_key,data_sha256,index_sha256,data_bytes,index_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, id, ordinal, ref.DataKey, ref.IndexKey, ref.DataSHA256, ref.IndexSHA256, ref.DataBytes, ref.IndexBytes); err != nil {
			return err
		}
	}
	return nil
}

// readImportWindow bounds memory to a single publication and validates every
// descriptor. It is shared by the source transfer and destination read-back.
func readImportWindow(ctx context.Context, tx *sql.Tx, row interface{ Scan(...any) error }, packsTable, parameter string, contract Contract) (Window, string, error) {
	var window Window
	var id, generation int64
	var count int
	var digest string
	if err := row.Scan(&id, &window.Tenant, &window.SourceID, &window.Start, &window.End, &generation, &count, &digest); err != nil {
		return window, "", err
	}
	if generation < 1 || count < 0 || count > MaxPacksPerWindow {
		return window, "", errors.New("invalid import window generation or pack count")
	}
	window.Generation = uint64(generation)
	window.Packs = []PackRef{}
	packRows, err := tx.QueryContext(ctx, `SELECT ordinal,data_key,index_key,data_sha256,index_sha256,data_bytes,index_bytes FROM `+packsTable+` WHERE window_id=`+parameter+` ORDER BY ordinal LIMIT 1025`, id)
	if err != nil {
		return window, "", err
	}
	defer packRows.Close()
	for packRows.Next() {
		var ordinal int
		var ref PackRef
		if err := packRows.Scan(&ordinal, &ref.DataKey, &ref.IndexKey, &ref.DataSHA256, &ref.IndexSHA256, &ref.DataBytes, &ref.IndexBytes); err != nil {
			return window, "", err
		}
		if ordinal != len(window.Packs) || ordinal >= count {
			return window, "", errors.New("invalid import pack order or count")
		}
		window.Packs = append(window.Packs, ref)
	}
	if err := packRows.Err(); err != nil {
		return window, "", err
	}
	if len(window.Packs) != count {
		return window, "", errors.New("import window is missing pack references")
	}
	actual, err := validatePublication(contract, window.Publication)
	if err != nil {
		return window, "", err
	}
	if actual != digest {
		return window, "", errors.New("import publication digest mismatch")
	}
	return window, digest, nil
}

func verifyPostgresImport(ctx context.Context, tx *sql.Tx, p *Postgres, contract Contract, imported importedWindows) error {
	windows, expectedHash := imported.windows, imported.hash
	for _, item := range []struct {
		table string
		count uint64
	}{{"windows", windows}, {"packs", imported.packs}, {"object_keys", 2 * imported.packs}, {"pending_objects", imported.ledger}} {
		var count uint64
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+p.table(item.table)).Scan(&count); err != nil {
			return err
		}
		if count != item.count {
			return fmt.Errorf("PostgreSQL import %s count differs from SQLite source", item.table)
		}
	}
	// QueryRow consumes its result on Scan before pack reads use the same
	// PostgreSQL transaction. Memory remains bounded to a single window.
	destinationHash := sha256.New()
	encoder := json.NewEncoder(destinationHash)
	for generation := uint64(1); generation <= windows; generation++ {
		row := tx.QueryRowContext(ctx, `SELECT id,tenant,source_id,start_ms,end_ms,generation,pack_count,digest FROM `+p.table("windows")+` WHERE generation=$1`, generation)
		window, _, err := readImportWindow(ctx, tx, row, p.table("packs"), "$1", contract)
		if err != nil {
			return err
		}
		if err := encoder.Encode(window); err != nil {
			return err
		}
	}
	if !bytes.Equal(destinationHash.Sum(nil), expectedHash) {
		return errors.New("PostgreSQL import window references differ from SQLite source")
	}
	return nil
}
