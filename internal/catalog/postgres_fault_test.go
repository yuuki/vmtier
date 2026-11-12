package catalog

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This test is driven only by scripts/postgres_ha.py, which owns fresh primary,
// standby and replacement directories. It never stops a user-selected server.
func TestPostgresHAFaultQualification(t *testing.T) {
	primaryDSN := os.Getenv("VMTIER_HA_PRIMARY_DSN")
	if primaryDSN == "" {
		t.Skip("run scripts/postgres_ha.py for disposable PostgreSQL fault qualification")
	}
	work, pg := os.Getenv("VMTIER_HA_WORKDIR"), os.Getenv("VMTIER_HA_POSTGRES_BIN")
	standbyDSN := os.Getenv("VMTIER_HA_STANDBY_DSN")
	if work == "" || pg == "" || standbyDSN == "" || os.Getenv("VMTIER_HA_REPLACEMENT_PORT") == "" {
		t.Fatal("incomplete disposable HA fixture configuration")
	}
	if marker, err := os.ReadFile(filepath.Join(work, "owned-postgres-ha-fixture")); err != nil || string(marker) != "vmtier disposable PostgreSQL HA fixture\n" {
		t.Fatal("refusing process control outside an owned HA fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	report := map[string]any{
		"passed":    false,
		"scope":     "disposable PostgreSQL catalog fault qualification",
		"promotion": "explicit pg_ctl promotion after confirmed old-primary process stop",
		"limitations": []string{
			"No automatic election controller or provider failure is exercised.",
			"Measurements are local observations, not an availability or recovery-time SLO.",
			"Lease deadlines use database time; cross-host clock skew is not injected.",
		},
	}
	t.Cleanup(func() {
		b, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(work, "results.json"), append(b, '\n'), 0600)
		}
		if err != nil {
			t.Error("write HA report:", err)
		}
	})
	admin, err := sql.Open("postgres", primaryDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var version string
	if err := admin.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	report["postgres_version"] = version
	s, err := CreatePostgres(ctx, primaryDSN, "ha-fault-catalog", contract(), BlobLocator{Type: "s3", Bucket: "ha-fixture"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	standbyAdmin, err := sql.Open("postgres", standbyDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer standbyAdmin.Close()
	var initializedLSN string
	if err := admin.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&initializedLSN); err != nil {
		t.Fatal(err)
	}
	replayDeadline := time.Now().Add(10 * time.Second)
	for {
		var replayed bool
		err := standbyAdmin.QueryRowContext(ctx, "SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn", initializedLSN).Scan(&replayed)
		if err == nil && replayed {
			break
		}
		if time.Now().After(replayDeadline) {
			t.Fatalf("standby did not replay initialized catalog: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if replica, err := OpenPostgres(ctx, standbyDSN, "ha-fault-catalog"); err == nil {
		_ = replica.Close()
		t.Fatal("unpromoted standby accepted as the catalog authority")
	}
	report["unpromoted_standby_rejected"] = true
	primaryURL, err := url.Parse(primaryDSN)
	if err != nil {
		t.Fatal(err)
	}
	proxy := newPostgresFaultProxy(t, primaryURL.Host)
	defer proxy.Close()
	proxyURL := *primaryURL
	proxyURL.Host = proxy.listener.Addr().String()
	s, err = OpenPostgres(ctx, proxyURL.String(), "ha-fault-catalog")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lease, err := s.AcquireLease(ctx, "archiver-a", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	first := sqlitePublication("1:2", "a", 100, 2)
	recordPublication(t, s, first)
	if generation, err := s.Fenced(lease).PublishWindow(ctx, 0, first); err != nil || generation != 1 {
		t.Fatalf("initial synchronous publication: generation=%d err=%v", generation, err)
	}
	report["acknowledged_initial_publication"] = true

	// The proxy observes the real server's COMMIT CommandComplete after its WAL
	// acknowledgement, then closes the socket before forwarding that response.
	second := Publication{Tenant: "1:2", SourceID: "a", Start: 200, End: 300}
	proxy.dropCommit.Store(true)
	_, ambiguousErr := s.Fenced(lease).PublishWindow(ctx, 1, second)
	if ambiguousErr == nil {
		t.Fatal("lost commit acknowledgement unexpectedly returned success")
	}
	select {
	case <-proxy.commitDropped:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not observe a committed transaction before disconnect")
	}
	state, err := s.Status(ctx)
	if err != nil || state.Generation != 2 {
		t.Fatalf("recover ambiguous commit: generation=%d err=%v", state.Generation, err)
	}
	if generation, err := s.Fenced(lease).PublishWindow(ctx, 1, second); err != nil || generation != 2 {
		t.Fatalf("replay ambiguous commit: generation=%d err=%v", generation, err)
	}
	report["ambiguous_commit_recovered_by_status_and_replay"] = true

	if err := s.ReleaseLease(ctx, lease); err != nil {
		t.Fatal(err)
	}
	stale, err := s.AcquireLease(ctx, "old-archiver", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var successor Lease
	deadline := time.Now().Add(5 * time.Second)
	for {
		successor, err = s.AcquireLease(ctx, "new-archiver", 30*time.Second)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrLeaseHeld) || time.Now().After(deadline) {
			t.Fatalf("lease takeover: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if successor.Token <= stale.Token {
		t.Fatalf("lease token did not advance: old=%d new=%d", stale.Token, successor.Token)
	}
	third := sqlitePublication("1:2", "a", 300, 1)
	recordPublication(t, s, third)
	if _, err := s.Fenced(stale).PublishWindow(ctx, 2, third); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale writer was not fenced: %v", err)
	}
	if generation, err := s.Fenced(successor).PublishWindow(ctx, 2, third); err != nil || generation != 3 {
		t.Fatalf("successor publication: generation=%d err=%v", generation, err)
	}
	report["expired_archiver_rejected_after_token_takeover"] = true
	report["fencing_tokens"] = []uint64{stale.Token, successor.Token}

	before, err := s.Select(ctx, Selection{Tenant: "1:2", Start: 100, End: 400})
	if err != nil {
		t.Fatal(err)
	}
	proxy.Partition(true)
	started := time.Now()
	queryCtx, stopQuery := context.WithTimeout(ctx, time.Second)
	_, partitionErr := s.Select(queryCtx, Selection{Tenant: "1:2", Start: 100, End: 400})
	stopQuery()
	partitionDuration := time.Since(started)
	proxy.Partition(false)
	if partitionErr == nil || partitionDuration > 2*time.Second {
		t.Fatalf("partition did not fail closed within bound: duration=%s err=%v", partitionDuration, partitionErr)
	}
	report["partition_failed_closed"] = true
	report["partition_error_ms"] = float64(partitionDuration.Microseconds()) / 1000
	// Drop all client/server bytes while leaving connections open, including
	// separate cancellation connections. Start after BEGIN has completed on a
	// warm connection so this exercises an in-flight query, not a dial timeout.
	if _, err := s.Status(ctx); err != nil {
		t.Fatal("warm connection before blackhole:", err)
	}
	proxy.blackholeAfterBegin.Store(true)
	started = time.Now()
	queryCtx, stopQuery = context.WithTimeout(ctx, 300*time.Millisecond)
	_, blackholeErr := s.Select(queryCtx, Selection{Tenant: "1:2", Start: 100, End: 400})
	stopQuery()
	blackholeDuration := time.Since(started)
	proxy.blackhole.Store(false)
	proxy.Route(primaryURL.Host)
	select {
	case <-proxy.blackholeStarted:
	default:
		t.Fatal("blackhole did not start after an established transaction began")
	}
	if blackholeErr == nil || blackholeDuration > 12*time.Second {
		t.Fatalf("blackholed data/cancel sockets did not fail closed within bound: duration=%s err=%v", blackholeDuration, blackholeErr)
	}
	report["silent_blackhole_failed_closed"] = true
	report["silent_blackhole_error_ms"] = float64(blackholeDuration.Microseconds()) / 1000
	if err := s.ReleaseLease(ctx, successor); err != nil {
		t.Fatal(err)
	}

	control := func(args ...string) {
		t.Helper()
		command := exec.CommandContext(ctx, filepath.Join(pg, args[0]), args[1:]...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("PostgreSQL fixture command %s: %v\n%s", args[0], err, output)
		}
	}
	failoverStart := time.Now()
	control("pg_ctl", "-D", filepath.Join(work, "primary"), "-m", "immediate", "-w", "stop")
	if _, err := os.Stat(filepath.Join(work, "primary", "postmaster.pid")); !os.IsNotExist(err) {
		t.Fatal("old primary was not fenced before promotion")
	}
	control("pg_ctl", "-D", filepath.Join(work, "standby"), "-w", "promote")
	standbyURL, err := url.Parse(standbyDSN)
	if err != nil {
		t.Fatal(err)
	}
	proxy.Route(standbyURL.Host)
	// Keep the same repository and stable proxy DSN across failover. Existing
	// sockets are invalidated; database/sql must reconnect to the promoted node.
	after, err := s.Select(ctx, Selection{Tenant: "1:2", Start: 100, End: 400})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("acknowledged state changed across failover: before=%+v after=%+v err=%v", before, after, err)
	}
	report["same_client_read_recovery_ms"] = float64(time.Since(failoverStart).Microseconds()) / 1000
	report["acknowledged_catalog_preserved_after_primary_stop"] = true
	report["catalog_id_preserved"] = after.ID
	report["generation_after_promotion"] = after.Generation

	// Restore a synchronous replica before allowing new commits. Promotion alone
	// does not justify weakening synchronous durability to make the test pass.
	replacement := filepath.Join(work, "replacement")
	control("pg_basebackup", "-d", standbyDSN+"&application_name=ha_standby", "-D", replacement, "-R", "-X", "stream", "-c", "fast")
	config, err := os.OpenFile(filepath.Join(replacement, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := fmt.Fprintf(config, "\nport=%s\n", os.Getenv("VMTIER_HA_REPLACEMENT_PORT"))
	closeErr := config.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("configure replacement standby: %v %v", writeErr, closeErr)
	}
	control("pg_ctl", "-D", replacement, "-l", filepath.Join(work, "replacement.log"), "-w", "start")
	promoted, err := sql.Open("postgres", standbyDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer promoted.Close()
	deadline = time.Now().Add(15 * time.Second)
	for {
		var synchronous bool
		err = promoted.QueryRowContext(ctx, `SELECT NOT pg_is_in_recovery() AND EXISTS (SELECT 1 FROM pg_stat_replication WHERE application_name='ha_standby' AND state='streaming' AND sync_state='sync')`).Scan(&synchronous)
		if err == nil && synchronous {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement synchronous standby not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	recoveredLease, err := s.AcquireLease(ctx, "recovered-archiver", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredLease.Token <= successor.Token {
		t.Fatalf("fencing token did not survive promotion: before=%d recovered=%d", successor.Token, recoveredLease.Token)
	}
	fourth := Publication{Tenant: "1:2", SourceID: "a", Start: 400, End: 500}
	if _, err := s.Fenced(successor).PublishWindow(ctx, 3, fourth); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("pre-promotion writer was not fenced after recovery: %v", err)
	}
	report["fencing_token_monotonic_across_promotion"] = true
	report["pre_promotion_archiver_rejected_after_recovery"] = true
	report["recovered_fencing_token"] = recoveredLease.Token
	if generation, err := s.Fenced(recoveredLease).PublishWindow(ctx, 3, fourth); err != nil || generation != 4 {
		t.Fatalf("publication after synchronous recovery: generation=%d err=%v", generation, err)
	}
	report["synchronous_write_recovery_ms"] = float64(time.Since(failoverStart).Microseconds()) / 1000
	report["publication_after_replacement_sync_standby"] = true
	report["passed"] = true
}

// postgresFaultProxy forwards plaintext PostgreSQL protocol only in the owned
// localhost fixture. It never parses credentials or accepts production DSNs.
type postgresFaultProxy struct {
	listener            net.Listener
	mu                  sync.Mutex
	target              string
	partition           bool
	connections         map[net.Conn]struct{}
	dropCommit          atomic.Bool
	blackhole           atomic.Bool
	blackholeAfterBegin atomic.Bool
	commitDropped       chan struct{}
	blackholeStarted    chan struct{}
	wg                  sync.WaitGroup
}

func newPostgresFaultProxy(t *testing.T, target string) *postgresFaultProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &postgresFaultProxy{listener: listener, target: target, connections: make(map[net.Conn]struct{}), commitDropped: make(chan struct{}, 1), blackholeStarted: make(chan struct{}, 1)}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.wg.Add(1)
			go func() { defer p.wg.Done(); p.serve(client) }()
		}
	}()
	return p
}

func (p *postgresFaultProxy) serve(client net.Conn) {
	defer client.Close()
	p.mu.Lock()
	target, partition := p.target, p.partition
	if !partition {
		p.connections[client] = struct{}{}
	}
	p.mu.Unlock()
	if partition {
		return
	}
	defer func() { p.mu.Lock(); delete(p.connections, client); p.mu.Unlock() }()
	server, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		return
	}
	defer server.Close()
	frontendDone := make(chan struct{})
	go func() {
		defer server.Close()
		defer close(frontendDone)
		buf := make([]byte, 32<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 && !p.blackhole.Load() {
				if _, writeErr := server.Write(buf[:n]); writeErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { _ = client.Close(); <-frontendDone }()
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(server, header); err != nil {
			return
		}
		length := int(binary.BigEndian.Uint32(header[1:]))
		if length < 4 || length > 16<<20 {
			return
		}
		payload := make([]byte, length-4)
		if _, err := io.ReadFull(server, payload); err != nil {
			return
		}
		if p.blackhole.Load() {
			continue
		}
		if header[0] == 'Z' && len(payload) == 1 && payload[0] == 'T' && p.blackholeAfterBegin.CompareAndSwap(true, false) {
			p.blackhole.Store(true)
			p.blackholeStarted <- struct{}{}
		}
		if header[0] == 'C' && strings.TrimSuffix(string(payload), "\x00") == "COMMIT" && p.dropCommit.CompareAndSwap(true, false) {
			p.commitDropped <- struct{}{}
			return
		}
		if _, err := client.Write(header); err != nil {
			return
		}
		if _, err := client.Write(payload); err != nil {
			return
		}
	}
}

func (p *postgresFaultProxy) Partition(enabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partition = enabled
	if enabled {
		for connection := range p.connections {
			_ = connection.Close()
		}
	}
}

func (p *postgresFaultProxy) Route(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = target
	for connection := range p.connections {
		_ = connection.Close()
	}
}

func (p *postgresFaultProxy) Close() {
	_ = p.listener.Close()
	p.Partition(true)
	p.wg.Wait()
}
