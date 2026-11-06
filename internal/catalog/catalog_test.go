package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func entry(tenant, source string, start, end int64) Entry {
	return Entry{Tenant: tenant, Start: start, End: end, SourceID: source, SourceFingerprint: strings.Repeat("a", 64), IndexKey: "tenants/" + tenant + "/index", DataKey: "tenants/" + tenant + "/data", IndexSHA256: strings.Repeat("b", 64)}
}
func contract() Contract {
	return Contract{CoverageStart: 100, WindowMS: 100, DedupMS: 1, AgeMS: 1000, LatenessMS: 100, Tenants: []string{"1:2"}, Sources: []Source{{ID: "a", Fingerprint: strings.Repeat("a", 64)}}}
}
func validManifest() *Manifest {
	m := Fresh("test")
	m.Generation = 1
	m.Contract = contract()
	m.Entries = []Entry{entry("1:2", "a", 100, 200)}
	return m
}
func localStore(t *testing.T) *Local {
	t.Helper()
	s, e := NewLocal(t.TempDir(), "test", true)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func mustCommit(t *testing.T, s Store, m *Manifest, expected uint64) {
	t.Helper()
	if _, e := Commit(context.Background(), s, m, expected); e != nil {
		t.Fatal(e)
	}
}

func TestCoverageIntersectionAndHoles(t *testing.T) {
	m := Fresh("test")
	m.Entries = []Entry{entry("1:2", "a", 100, 200), entry("1:2", "a", 200, 300), entry("1:2", "a", 400, 500), entry("1:2", "b", 100, 250), entry("9:9", "b", 250, 500)}
	for _, tc := range []struct {
		tenant      string
		sources     []string
		start, want int64
	}{{"1:2", []string{"a", "b"}, 100, 250}, {"1:2", []string{"a"}, 100, 300}, {"1:2", []string{"a"}, 150, 300}, {"1:2", []string{"a", "missing"}, 100, 100}, {"1:2", []string{"b"}, 0, 0}, {"1:2", nil, 100, 100}, {"unknown", []string{"a"}, 100, 100}} {
		if got := Coverage(m, tc.tenant, tc.sources, tc.start); got != tc.want {
			t.Errorf("%+v got=%d", tc, got)
		}
	}
}
func TestValidatePublication(t *testing.T) {
	if e := Validate(validManifest()); e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"version", func(m *Manifest) { m.Version = 1 }},
		{"identity", func(m *Manifest) { m.ID = "../../other" }},
		{"codec", func(m *Manifest) { m.VMVersion = "unknown" }},
		{"tenant", func(m *Manifest) { m.Entries[0].Tenant = "1" }},
		{"interval", func(m *Manifest) { m.Entries[0].End = 150 }},
		{"negativeTime", func(m *Manifest) { m.Entries[0].Start = -1 }},
		{"unaligned", func(m *Manifest) { m.Entries[0].Start = 101; m.Entries[0].End = 201 }},
		{"hash", func(m *Manifest) { m.Entries[0].IndexSHA256 = strings.Repeat("z", 64) }},
		{"fingerprint", func(m *Manifest) { m.Entries[0].SourceFingerprint = "short" }},
		{"tenantEscape", func(m *Manifest) { m.Entries[0].DataKey = "tenants/2:3/data" }},
		{"traversal", func(m *Manifest) { m.Entries[0].DataKey = "tenants/1:2/../data" }},
		{"sameObject", func(m *Manifest) { m.Entries[0].DataKey = m.Entries[0].IndexKey }},
		{"duplicate", func(m *Manifest) { m.Entries = append(m.Entries, m.Entries[0]) }},
		{"sourceMissing", func(m *Manifest) {
			m.Contract.Sources = append(m.Contract.Sources, Source{ID: "b", Fingerprint: strings.Repeat("a", 64)})
		}},
		{"tenantMissing", func(m *Manifest) { m.Contract.Tenants = append(m.Contract.Tenants, "3:4") }},
		{"dedup", func(m *Manifest) { m.Contract.DedupMS = 0 }},
		{"missing age", func(m *Manifest) { m.Contract.AgeMS = 0 }},
		{"negative lateness", func(m *Manifest) { m.Contract.LatenessMS = -1 }},
		{"unsealed age", func(m *Manifest) { m.Contract.LatenessMS = m.Contract.AgeMS }},
		{"unbound", func(m *Manifest) { m.Generation = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			tc.mutate(m)
			if e := Validate(m); e == nil {
				t.Fatal("invalid publication accepted")
			}
		})
	}
}
func TestInitializationRestartAndMissingState(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	if _, e := NewLocal(dir, "test", false); !errors.Is(e, ErrNotInitialized) {
		t.Fatalf("missing init: %v", e)
	}
	s, e := NewLocal(dir, "test", true)
	if e != nil {
		t.Fatal(e)
	}
	m, gen, e := Load(ctx, s)
	if e != nil || gen != 0 || m.ID != "test" {
		t.Fatalf("initial state: %v", e)
	}
	if _, e := NewLocal(dir, "test", true); e == nil {
		t.Fatal("overwriting initialization accepted")
	}
	if _, e := NewLocal(dir, "wrong", false); e == nil {
		t.Fatal("wrong catalog identity accepted")
	}
	m.Contract = contract()
	m.Entries = []Entry{entry("1:2", "a", 100, 200)}
	mustCommit(t, s, m, 0)
	s.Close()
	s, e = NewLocal(dir, "test", false)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	m, gen, e = Load(ctx, s)
	if e != nil || gen != 1 || len(m.Entries) != 1 {
		t.Fatalf("reopen: %v", e)
	}
	if e = os.Remove(filepath.Join(dir, Key)); e != nil {
		t.Fatal(e)
	}
	if _, _, e = Load(ctx, s); !errors.Is(e, ErrNotInitialized) {
		t.Fatalf("state deletion became fresh: %v", e)
	}
}
func TestCASConcurrentCommitAndSnapshot(t *testing.T) {
	ctx := context.Background()
	s := localStore(t)
	m := Fresh("test")
	m.Contract = contract()
	m.Entries = []Entry{entry("1:2", "a", 100, 200)}
	mustCommit(t, s, m, 0)
	old, gen, e := Load(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	left, _, _ := Load(ctx, s)
	right, _, _ := Load(ctx, s)
	left.Entries = append(left.Entries, entry("1:2", "a", 200, 300))
	right.Entries = append(right.Entries, entry("1:2", "a", 300, 400))
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, candidate := range []*Manifest{left, right} {
		wg.Add(1)
		go func(m *Manifest) { defer wg.Done(); _, e := Commit(ctx, s, m, gen); errs <- e }(candidate)
	}
	wg.Wait()
	close(errs)
	passed, conflicted := 0, 0
	for e := range errs {
		if e == nil {
			passed++
		} else if errors.Is(e, ErrConflict) {
			conflicted++
		} else {
			t.Fatal(e)
		}
	}
	if passed != 1 || conflicted != 1 {
		t.Fatalf("passed=%d conflicted=%d", passed, conflicted)
	}
	current, newGen, e := Load(ctx, s)
	if e != nil || newGen != 2 {
		t.Fatalf("current: %v", e)
	}
	if len(old.Entries) != 1 || old.Generation != 1 || Coverage(old, "1:2", []string{"a"}, 100) != 200 {
		t.Fatal("reader snapshot mutated")
	}
	if e = s.Publish(ctx, 1, current); e != nil {
		t.Fatalf("identical retry not idempotent: %v", e)
	}
	if _, e = Commit(ctx, s, old, gen); !errors.Is(e, ErrConflict) {
		t.Fatalf("stale commit: %v", e)
	}
	if old.Generation != 1 {
		t.Fatal("failed commit advanced caller generation")
	}
}
func TestImmutableContractAndHistory(t *testing.T) {
	ctx := context.Background()
	s := localStore(t)
	m := Fresh("test")
	m.Contract = contract()
	m.Entries = []Entry{entry("1:2", "a", 100, 200)}
	mustCommit(t, s, m, 0)
	for _, tc := range []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"replace", func(m *Manifest) { m.Entries[0].DataKey = "tenants/1:2/replaced" }},
		{"remove", func(m *Manifest) { m.Entries = m.Entries[1:] }},
		{"origin", func(m *Manifest) { m.Contract.CoverageStart = 0 }},
		{"archive age", func(m *Manifest) { m.Contract.AgeMS += 100 }},
		{"lateness", func(m *Manifest) { m.Contract.LatenessMS += 100 }},
		{"identity", func(m *Manifest) { m.ID = "another" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next, _, _ := Load(ctx, s)
			next.Entries = append(next.Entries, entry("1:2", "a", 200, 300))
			tc.mutate(next)
			if _, e := Commit(ctx, s, next, 1); e == nil {
				t.Fatal("mutation accepted")
			}
			actual, _, _ := Load(ctx, s)
			if actual.Generation != 1 || len(actual.Entries) != 1 {
				t.Fatal("rejected publication changed state")
			}
		})
	}
}
func TestCorruptOrTruncatedStateNeverBootstraps(t *testing.T) {
	for _, data := range []string{`{"version":2`, `{"version":2,"evil":true}`, `{} {}`, `{}`} {
		dir := t.TempDir()
		if e := os.WriteFile(filepath.Join(dir, Key), []byte(data), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := NewLocal(dir, "test", false); e == nil {
			t.Fatal("corruption accepted")
		}
		if _, e := NewLocal(dir, "test", true); e == nil {
			t.Fatal("corruption overwritten")
		}
	}
}
func TestHTTPAuthorizationAndPublication(t *testing.T) {
	ctx := context.Background()
	local := localStore(t)
	server := httptest.NewServer(Handler(local, "secret"))
	defer server.Close()
	bad, _ := NewHTTP(server.URL, "test", "")
	if _, e := bad.Read(ctx); e == nil {
		t.Fatal("unauthenticated read accepted")
	}
	remote, e := NewHTTP(server.URL, "test", "secret")
	if e != nil {
		t.Fatal(e)
	}
	m, gen, e := Load(ctx, remote)
	if e != nil || gen != 0 {
		t.Fatalf("read: %v", e)
	}
	m.Contract = contract()
	m.Entries = []Entry{entry("1:2", "a", 100, 200)}
	mustCommit(t, remote, m, gen)
	if e = remote.Publish(ctx, 0, m); e != nil {
		t.Fatalf("HTTP response-loss retry: %v", e)
	}
	resp, e := http.Get(server.URL + "/v1/catalog")
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.Header.Get("Cache-Control") != "no-store" || resp.StatusCode != 401 {
		t.Fatalf("headers/status: %v", resp)
	}
	for _, url := range []string{server.URL + "/path", server.URL + "?query=1", "http://user:password@example.com", "file:///tmp/catalog"} {
		if _, e = NewHTTP(url, "test", ""); e == nil {
			t.Errorf("invalid base accepted: %s", url)
		}
	}
}
func TestRemoteRejectsRollbackMutationAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		next func() *Manifest
	}{
		{"rollback", func() *Manifest { return Fresh("test") }},
		{"sameGenerationMutation", func() *Manifest { m := validManifest(); m.Entries[0].DataKey = "tenants/1:2/changed"; return m }},
		{"identity", func() *Manifest { m := validManifest(); m.ID = "wrong"; return m }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				m := validManifest()
				if calls > 1 {
					m = tc.next()
				}
				json.NewEncoder(w).Encode(m)
			}))
			defer server.Close()
			remote, _ := NewHTTP(server.URL, "test", "")
			if _, e := remote.Read(context.Background()); e != nil {
				t.Fatal(e)
			}
			if _, e := remote.Read(context.Background()); e == nil {
				t.Fatal("rollback/mutation accepted")
			}
		})
	}
}

type lostResponseStore struct {
	Store
	lose bool
}

func (s *lostResponseStore) Publish(ctx context.Context, gen uint64, m *Manifest) error {
	if e := s.Store.Publish(ctx, gen, m); e != nil {
		return e
	}
	if s.lose {
		s.lose = false
		return errors.New("injected response loss after durable commit")
	}
	return nil
}
func TestAmbiguousPublishReloadAndRetry(t *testing.T) {
	ctx := context.Background()
	local := localStore(t)
	wrapped := &lostResponseStore{Store: local, lose: true}
	server := httptest.NewServer(Handler(wrapped, ""))
	defer server.Close()
	remote, _ := NewHTTP(server.URL, "test", "")
	m, _, _ := Load(ctx, remote)
	m.Contract = contract()
	m.Entries = []Entry{entry("1:2", "a", 100, 200)}
	if _, e := Commit(ctx, remote, m, 0); e == nil {
		t.Fatal("injected loss was not visible")
	}
	if m.Generation != 0 {
		t.Fatal("ambiguous error advanced caller")
	}
	confirmed, gen, e := Load(ctx, remote)
	if e != nil || gen != 1 || len(confirmed.Entries) != 1 {
		t.Fatalf("reload: %v", e)
	}
	if _, e = Commit(ctx, remote, m, 0); e != nil {
		t.Fatalf("identical retry: %v", e)
	}
	// A later generation keeps this publication visible. Reusing the old expected
	// generation then conflicts; callers reload and recognize the retained entry.
	confirmed.Entries = append(confirmed.Entries, entry("1:2", "a", 200, 300))
	mustCommit(t, remote, confirmed, 1)
	if e = remote.Publish(ctx, 0, m); !errors.Is(e, ErrConflict) {
		t.Fatalf("old retry after later commit: %v", e)
	}
}
func TestRemoteFailureDoesNotReturnCachedManifest(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls > 1 {
			http.Error(w, "authority unavailable", 503)
			return
		}
		json.NewEncoder(w).Encode(validManifest())
	}))
	defer server.Close()
	remote, _ := NewHTTP(server.URL, "test", "")
	if _, e := remote.Read(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e := remote.Read(context.Background()); e == nil {
		t.Fatal("stale cached manifest returned")
	}
}
func TestCanceledRequestsAndGenerationOverflow(t *testing.T) {
	s := localStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.Read(ctx); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancellation: %v", e)
	}
	m := validManifest()
	m.Generation = ^uint64(0)
	if _, e := Commit(context.Background(), s, m, m.Generation); e == nil {
		t.Fatal("generation overflow accepted")
	}
	remote, _ := NewHTTP("http://127.0.0.1:1", "test", "")
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, e := remote.Read(ctx); e == nil {
		t.Fatal("unavailable authority succeeded")
	}
}

func TestHTTPRejectsUnauthorizedAndInvalidPublish(t *testing.T) {
	s := localStore(t)
	h := Handler(s, "token")
	candidate := validManifest()
	expected := uint64(0)
	payload, _ := json.Marshal(publication{Expected: &expected, Manifest: candidate})
	for _, auth := range []string{"", "Bearer wrong", "Bearer token-extra"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/catalog/commit", bytes.NewReader(payload))
		req.Header.Set("Authorization", auth)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 401 {
			t.Fatalf("unauthorized publish status %d", rec.Code)
		}
	}
	for _, body := range []string{`{}`, `{"expected_generation":0}`, `{"expected_generation":0,"manifest":null}`, string(payload) + `{}`, `{"unexpected":true}`} {
		req := httptest.NewRequest(http.MethodPost, "/v1/catalog/commit", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 400 {
			t.Fatalf("invalid publish status %d", rec.Code)
		}
	}
	m, gen, e := Load(context.Background(), s)
	if e != nil || gen != 0 || len(m.Entries) != 0 {
		t.Fatalf("rejected publication changed state: %v", e)
	}
}
