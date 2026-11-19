package catalog

import "context"

// Capacity is one observation of a catalog's committed objects. Object lengths
// come from publication descriptors, never bucket LIST; they exclude
// failed/unreferenced attempts and do not measure bucket usage.
type Capacity struct {
	Windows, Packs                          int64
	CommittedDataBytes, CommittedIndexBytes int64
	// Files holds the apparent lengths of a SQLite catalog's local files. It
	// is nil for a backend without local files.
	Files *SQLiteFiles
}

// SQLiteFiles holds apparent file lengths (including reusable/free pages), not
// allocated filesystem blocks. WAL and shared-memory files may be absent.
type SQLiteFiles struct {
	DatabaseBytes, WALBytes, SharedMemoryBytes int64
}

// CapacityReader is implemented by the service backends. Each observation
// reads the catalog in one read-only transaction that also checks its state;
// it performs no catalog write and no object-store operation.
type CapacityReader interface {
	Capacity(ctx context.Context) (Capacity, error)
}
