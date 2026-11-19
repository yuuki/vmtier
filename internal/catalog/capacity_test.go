package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCommittedCapacity(t *testing.T) {
	c := hourContract()
	c.Tenants = append(c.Tenants, "3:4")
	c.Sources = append(c.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	for name, fixture := range map[string]func(*testing.T, Contract) ledgerFixture{"sqlite": sqliteLedgerFixtureFor, "postgres": postgresLedgerFixtureFor} {
		t.Run(name, func(t *testing.T) { testCommittedCapacity(t, fixture(t, c)) })
	}
}

// streamCapacities lists every stream of the four-stream contract, with the
// values given for some of them.
func streamCapacities(values ...StreamCapacity) []StreamCapacity {
	out := []StreamCapacity{{Tenant: "1:2", SourceID: "a"}, {Tenant: "1:2", SourceID: "b"}, {Tenant: "3:4", SourceID: "a"}, {Tenant: "3:4", SourceID: "b"}}
	for _, v := range values {
		for i := range out {
			if out[i].Tenant == v.Tenant && out[i].SourceID == v.SourceID {
				out[i] = v
			}
		}
	}
	return out
}

func testCommittedCapacity(t *testing.T, f ledgerFixture) {
	ctx := context.Background()
	reader, ok := f.repo.(CapacityReader)
	if !ok {
		t.Fatal("service backend does not report capacity")
	}
	_, local := f.repo.(*SQLite)
	check := func(want Capacity) {
		t.Helper()
		got, err := reader.Capacity(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Only SQLite has local catalog files.
		if (got.Files != nil) != local || (local && got.Files.DatabaseBytes <= 0) {
			t.Fatalf("catalog files: %+v", got.Files)
		}
		got.Files = nil
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("capacity:\n got %+v\nwant %+v", got, want)
		}
	}
	// A stream without windows is a known zero, not an omitted stream.
	check(Capacity{Streams: streamCapacities()})
	generation := uint64(0)
	for _, p := range []Publication{hourPublication("1:2", "a", 1, 2), hourPublication("1:2", "a", 2, 0), hourPublication("3:4", "b", 1, 1)} {
		recordPublication(t, f.repo, p)
		var err error
		if generation, err = f.publish(generation, p); err != nil {
			t.Fatal(err)
		}
	}
	// Recorded keys and failed publications are not committed objects.
	stale := hourPublication("1:2", "a", 3, 1)
	recordPublication(t, f.repo, stale)
	if _, err := f.publish(0, stale); err == nil {
		t.Fatal("stale generation unexpectedly published")
	}
	check(Capacity{Windows: 3, Packs: 3, CommittedDataBytes: 30, CommittedIndexBytes: 60, Streams: streamCapacities(
		StreamCapacity{Tenant: "1:2", SourceID: "a", Windows: 2, Packs: 2, CommittedDataBytes: 20, CommittedIndexBytes: 40},
		StreamCapacity{Tenant: "3:4", SourceID: "b", Windows: 1, Packs: 1, CommittedDataBytes: 10, CommittedIndexBytes: 20},
	)})
	// Cold retention removes the windows and their packs from the stream and
	// the totals.
	if n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 2), 10); err != nil || n != 2 {
		t.Fatalf("retire first windows: %d %v", n, err)
	}
	check(Capacity{Windows: 1, Streams: streamCapacities(StreamCapacity{Tenant: "1:2", SourceID: "a", Windows: 1})})
}

func TestPostgresCapacitySurvivesReconnectAndRejectsUnavailableAuthority(t *testing.T) {
	ctx, dsn, db, s := postgresTestStore(t, contract())
	first := sqlitePublication("1:2", "a", 100, 2)
	recordPublication(t, s, first)
	if _, err := publishPostgresTest(ctx, s, 0, first); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPostgres(ctx, dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	want := Capacity{Windows: 1, Packs: 2, CommittedDataBytes: 20, CommittedIndexBytes: 40, Streams: []StreamCapacity{{Tenant: "1:2", SourceID: "a", Windows: 1, Packs: 2, CommittedDataBytes: 20, CommittedIndexBytes: 40}}}
	if got, err := reopened.Capacity(ctx); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("capacity after reconnect: %+v %v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := reopened.Capacity(canceled); err == nil {
		t.Fatal("canceled observation succeeded")
	}
	// A catalog of another schema is not observed, as by every other read.
	if _, err := db.ExecContext(ctx, `UPDATE catalog_state SET schema_version=schema_version+1`); err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Capacity(ctx); err == nil {
		t.Fatalf("unsupported schema observed: %+v", got)
	}
}

// Publications, replays and retirements in any order keep every stream's
// totals equal to its committed packs, on both backends.
func TestStreamTotalsFollowPublicationAndRetention(t *testing.T) {
	c := hourContract()
	c.Tenants = append(c.Tenants, "3:4")
	c.Sources = append(c.Sources, Source{ID: "b", Fingerprint: strings.Repeat("b", 64)})
	for name, fixture := range map[string]func(*testing.T, Contract) ledgerFixture{"sqlite": sqliteLedgerFixtureFor, "postgres": postgresLedgerFixtureFor} {
		for seed := uint64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("%s/seed-%d", name, seed), func(t *testing.T) {
				f := fixture(t, c)
				coverage, windows, packs := "coverage", "windows", "packs"
				count := `SELECT count(*) FROM windows WHERE tenant=? AND source_id=?`
				if p, ok := f.repo.(*Postgres); ok {
					coverage, windows, packs = p.table("coverage"), p.table("windows"), p.table("packs")
					count = `SELECT count(*) FROM ` + windows + ` WHERE tenant=$1 AND source_id=$2`
				}
				testStreamTotals(t, f, seed, streamTotalsMismatchSQL(coverage, windows, packs), count)
			})
		}
	}
}

// testStreamTotals publishes, replays and retires at random and checks after
// each step that every stream's totals equal its packs and that its derived
// window count equals its window rows. count reads one stream's window rows.
func testStreamTotals(t *testing.T, f ledgerFixture, seed uint64, mismatch, count string) {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, 2))
	streams := [][2]string{{"1:2", "a"}, {"1:2", "b"}, {"3:4", "a"}, {"3:4", "b"}}
	next := map[[2]string]int64{}
	var published []Publication
	generation := uint64(0)
	check := func(step string) {
		t.Helper()
		got, err := f.repo.(CapacityReader).Capacity(ctx)
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		for _, s := range got.Streams {
			var rows int64
			if err := f.queryRow(count, s.Tenant, s.SourceID).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != s.Windows {
				t.Fatalf("%s: stream %s/%s reports %d windows, has %d", step, s.Tenant, s.SourceID, s.Windows, rows)
			}
		}
		if err := checkStreamTotals(ctx, queryRower{f}, mismatch); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
	}
	retired := 0
	for step := 0; step < 40; step++ {
		stream := streams[rng.IntN(len(streams))]
		hour := 1 + next[stream]
		p := hourPublication(stream[0], stream[1], hour, rng.IntN(4))
		for i := range p.Packs {
			p.Packs[i].DataBytes, p.Packs[i].IndexBytes = rng.Int64N(1000), 1+rng.Int64N(1000)
		}
		recordPublication(t, f.repo, p)
		var err error
		if generation, err = f.publish(generation, p); err != nil {
			t.Fatal(err)
		}
		next[stream] = hour
		published = append(published, p)
		// A replay of a committed window that is still retained changes
		// nothing; a retired window cannot be replayed at all.
		if step%5 == 0 {
			if replay, ok := retainedPublication(t, f, rng, published); ok {
				if _, err := f.publish(generation, replay); err != nil {
					t.Fatal(err)
				}
			}
		}
		if step%10 == 9 {
			n, err := f.repo.RetireWindows(ctx, retentionEnding(f.now(), 1+next[stream]/2), 1+rng.IntN(5))
			if err != nil {
				t.Fatal(err)
			}
			retired += n
		}
		check(fmt.Sprintf("step %d", step))
	}
	if retired == 0 {
		t.Fatal("the sequence retired no window")
	}
}

// retainedPublication picks a published window that Cold retention has not
// retired.
func retainedPublication(t *testing.T, f ledgerFixture, rng *rand.Rand, published []Publication) (Publication, bool) {
	t.Helper()
	state, err := f.repo.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	starts := map[[2]string]int64{}
	for _, stream := range state.Coverage {
		starts[[2]string{stream.Tenant, stream.SourceID}] = stream.Start
	}
	var retained []Publication
	for _, p := range published {
		if p.Start >= starts[[2]string{p.Tenant, p.SourceID}] {
			retained = append(retained, p)
		}
	}
	if len(retained) == 0 {
		return Publication{}, false
	}
	return retained[rng.IntN(len(retained))], true
}

// streamTotalsMismatchSQL counts the streams whose totals differ from the
// packs of their windows.
func streamTotalsMismatchSQL(coverage, windows, packs string) string {
	return `SELECT count(*) FROM ` + coverage + ` c WHERE (c.committed_packs, c.committed_data_bytes, c.committed_index_bytes) <> ` + streamTotalsOfPacks(windows, packs)
}

// checkStreamTotals fails when a stream's totals differ from its packs.
func checkStreamTotals(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, query string) error {
	var mismatched int64
	if err := q.QueryRowContext(ctx, query).Scan(&mismatched); err != nil {
		return err
	}
	if mismatched != 0 {
		return fmt.Errorf("stream totals of %d streams differ from the committed packs", mismatched)
	}
	return nil
}

// queryRower runs a fixture's queries through its raw SQL connection.
type queryRower struct{ f ledgerFixture }

func (q queryRower) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return q.f.queryRow(query, args...)
}

// Drifted stream totals are derived data: they block neither Cold retention
// nor a restore, the capacity observation refuses negative ones instead of
// reporting them, and catalog-upgrade recomputes them in place.
func TestSQLiteDriftedStreamTotalsBlockNothing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := CreateSQLite(ctx, root+"/state", "test", twoTenantHourContract(), BlobLocator{Type: "s3", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for i, p := range driftFixturePublications() {
		recordPublication(t, s, p)
		if _, err := s.PublishWindow(ctx, uint64(i), p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE coverage SET committed_packs=0, committed_data_bytes=committed_data_bytes+1`); err != nil {
		t.Fatal(err)
	}
	if n, err := s.RetireWindows(ctx, retentionEnding(time.Now().UnixMilli(), 2), 10); err != nil || n != 2 {
		t.Fatalf("retirement with drifted totals: %d %v", n, err)
	}
	if got, err := s.Capacity(ctx); err == nil || !strings.Contains(err.Error(), "catalog-upgrade") {
		t.Fatalf("negative totals reported: %+v %v", got, err)
	}
	if err := s.Backup(ctx, root+"/backup"); err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSQLite(ctx, root+"/backup", root+"/restored", "restored")
	if err != nil {
		t.Fatal(err)
	}
	want := driftFixtureRemainder()
	if got, err := restored.Capacity(ctx); err != nil || !reflect.DeepEqual(got.Streams, want) {
		t.Fatalf("restored totals: %+v %v", got.Streams, err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := UpgradeSQLite(ctx, root+"/state", "test")
	if err != nil {
		t.Fatal(err)
	}
	repaired, err := OpenSQLite(ctx, root+"/state", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer repaired.Close()
	if after := sqliteStatus(t, repaired); !reflect.DeepEqual(after, before) || after.Generation != 4 {
		t.Fatalf("catalog-upgrade changed the state: %+v %+v", before, after)
	}
	if got, err := repaired.Capacity(ctx); err != nil || !reflect.DeepEqual(got.Streams, want) {
		t.Fatalf("repaired totals: %+v %v", got.Streams, err)
	}
}

func TestPostgresDriftedStreamTotalsBlockNothing(t *testing.T) {
	ctx, dsn, db, p := postgresTestStore(t, twoTenantHourContract())
	for i, publication := range driftFixturePublications() {
		recordPublication(t, p, publication)
		if _, err := publishPostgresTest(ctx, p, uint64(i), publication); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `UPDATE coverage SET committed_packs=0, committed_data_bytes=committed_data_bytes+1`); err != nil {
		t.Fatal(err)
	}
	var now int64
	if err := db.QueryRowContext(ctx, `SELECT `+postgresNowMS).Scan(&now); err != nil {
		t.Fatal(err)
	}
	if n, err := p.RetireWindows(ctx, retentionEnding(now, 2), 10); err != nil || n != 2 {
		t.Fatalf("retirement with drifted totals: %d %v", n, err)
	}
	if got, err := p.Capacity(ctx); err == nil || !strings.Contains(err.Error(), "catalog-upgrade") {
		t.Fatalf("negative totals reported: %+v %v", got, err)
	}
	before := postgresStatus(t, ctx, p)
	if err := UpgradePostgres(ctx, dsn, "test"); err != nil {
		t.Fatal(err)
	}
	if after := postgresStatus(t, ctx, p); !reflect.DeepEqual(after, before) {
		t.Fatalf("catalog-upgrade changed the state: %+v %+v", before, after)
	}
	if got, err := p.Capacity(ctx); err != nil || !reflect.DeepEqual(got.Streams, driftFixtureRemainder()) {
		t.Fatalf("repaired totals: %+v %v", got.Streams, err)
	}
}

// twoTenantHourContract is hourContract with a second tenant.
func twoTenantHourContract() Contract {
	c := hourContract()
	c.Tenants = append(c.Tenants, "3:4")
	return c
}

// driftFixturePublications publishes hours 1 and 2 of two streams with
// different pack counts, so that recomputed totals must group by stream.
func driftFixturePublications() []Publication {
	return []Publication{hourPublication("1:2", "a", 1, 2), hourPublication("3:4", "a", 1, 1),
		hourPublication("1:2", "a", 2, 1), hourPublication("3:4", "a", 2, 3)}
}

// driftFixtureRemainder is the correct totals after hour 1 is retired.
func driftFixtureRemainder() []StreamCapacity {
	return []StreamCapacity{{Tenant: "1:2", SourceID: "a", Windows: 1, Packs: 1, CommittedDataBytes: 10, CommittedIndexBytes: 20},
		{Tenant: "3:4", SourceID: "a", Windows: 1, Packs: 3, CommittedDataBytes: 30, CommittedIndexBytes: 60}}
}

// The PostgreSQL import computes the destination's totals from the copied
// packs, so a drifted source neither blocks the migration nor carries over.
func TestPostgresImportRecomputesStreamTotals(t *testing.T) {
	ctx, dsn, _ := postgresTestDatabase(t)
	source := sqliteTestStore(t, contract())
	first := sqlitePublication("1:2", "a", 100, 2)
	recordPublication(t, source, first)
	if _, err := source.PublishWindow(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	if _, err := source.db.ExecContext(ctx, `UPDATE coverage SET committed_index_bytes=7`); err != nil {
		t.Fatal(err)
	}
	destination, err := ImportSQLiteToPostgres(ctx, source, dsn, "imported")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	want := []StreamCapacity{{Tenant: "1:2", SourceID: "a", Windows: 1, Packs: 2, CommittedDataBytes: 20, CommittedIndexBytes: 40}}
	if got, err := destination.Capacity(ctx); err != nil || !reflect.DeepEqual(got.Streams, want) {
		t.Fatalf("imported totals: %+v %v", got.Streams, err)
	}
}
