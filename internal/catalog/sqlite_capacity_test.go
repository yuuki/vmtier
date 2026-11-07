package catalog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSQLiteCapacityCountsCommittedObjectsAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	s := sqliteTestStore(t, contract())
	initial, err := s.Capacity(ctx)
	if err != nil || initial.Packs != 0 || initial.Windows != 0 || initial.DatabaseBytes <= 0 || initial.CommittedDataBytes != 0 || initial.CommittedIndexBytes != 0 {
		t.Fatalf("initial capacity=%+v err=%v", initial, err)
	}
	if _, err := s.PublishWindow(ctx, 0, sqlitePublication("1:2", "a", 100, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishWindow(ctx, 1, sqlitePublication("1:2", "a", 200, 0)); err != nil {
		t.Fatal(err)
	}
	// Unrelated files and failed publication attempts are not committed objects.
	if err := os.WriteFile(filepath.Join(s.dir, "unreferenced-object"), make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PublishWindow(ctx, 0, sqlitePublication("1:2", "a", 300, 1)); err == nil {
		t.Fatal("stale generation unexpectedly published")
	}
	check := func(s *SQLite) {
		t.Helper()
		capacity, err := s.Capacity(ctx)
		if err != nil || capacity.Windows != 2 || capacity.Packs != 2 || capacity.CommittedDataBytes != 20 || capacity.CommittedIndexBytes != 40 {
			t.Fatalf("capacity=%+v err=%v", capacity, err)
		}
		for suffix, got := range map[string]int64{"": capacity.DatabaseBytes, "-wal": capacity.WALBytes, "-shm": capacity.SharedMemoryBytes} {
			info, err := os.Stat(filepath.Join(s.dir, SQLiteFile+suffix))
			if err != nil || got != info.Size() {
				t.Fatalf("file %q bytes=%d info=%v err=%v", suffix, got, info, err)
			}
		}
	}
	check(s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLite(ctx, s.dir, s.id)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	check(reopened)
}

func TestSQLiteCapacityRejectsUnavailableAuthority(t *testing.T) {
	s := sqliteTestStore(t, contract())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Capacity(ctx); err == nil {
		t.Fatal("canceled observation succeeded")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Capacity(context.Background()); err == nil {
		t.Fatal("closed authority returned capacity")
	}
}
