// Package catalog owns publication authority independently from the blob store.
// Repositories preserve append-only publication metadata. Runtime ownership and
// distributed failover require coordination beyond the repository transactions.
package catalog

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/yuuki/vmtier/internal/pack"
)

const Version = 2
const Key = "catalog.json"
const MaxEntries = 10000
const MaxBytes = 16 << 20

var ErrConflict = errors.New("catalog generation conflict")
var ErrNotInitialized = errors.New("catalog is not initialized")
var ErrInvalidPublication = errors.New("invalid catalog publication")

type Store interface {
	Read(context.Context) (*Manifest, error)
	Publish(context.Context, uint64, *Manifest) error
}
type Source struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
}
type Contract struct {
	CoverageStart int64    `json:"coverage_start_ms"`
	WindowMS      int64    `json:"window_ms"`
	AgeMS         int64    `json:"archive_age_ms"`
	LatenessMS    int64    `json:"lateness_ms"`
	DedupMS       int64    `json:"dedup_ms"`
	Tenants       []string `json:"tenants"`
	Sources       []Source `json:"sources"`
}
type Entry struct {
	Tenant            string `json:"tenant"`
	Start             int64  `json:"start_ms"`
	End               int64  `json:"end_ms"`
	SourceID          string `json:"source_id"`
	SourceFingerprint string `json:"source_fingerprint"`
	IndexKey          string `json:"index_key"`
	DataKey           string `json:"data_key"`
	IndexSHA256       string `json:"index_sha256"`
}
type Manifest struct {
	Version    int      `json:"version"`
	VMVersion  string   `json:"vm_version"`
	ID         string   `json:"id"`
	Generation uint64   `json:"generation"`
	Contract   Contract `json:"contract"`
	Entries    []Entry  `json:"entries"`
}

// Fresh constructs an unbound state, not a published catalog. Only explicit
// authority initialization persists this state. The variadic form allows tools
// to construct candidates before assigning an ID; Validate always requires ID.
func Fresh(id ...string) *Manifest {
	m := &Manifest{Version: Version, VMVersion: pack.VMVersion, Entries: []Entry{}}
	if len(id) > 0 {
		m.ID = id[0]
	}
	return m
}
func validID(id string) bool {
	if id == "" || len(id) > 128 || id == "." || id == ".." {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}
func canonicalContract(c Contract) Contract {
	c.Tenants = append([]string(nil), c.Tenants...)
	c.Sources = append([]Source(nil), c.Sources...)
	sort.Strings(c.Tenants)
	sort.Slice(c.Sources, func(i, j int) bool { return c.Sources[i].ID < c.Sources[j].ID })
	return c
}
func EqualContract(a, b Contract) bool {
	aa, _ := json.Marshal(canonicalContract(a))
	bb, _ := json.Marshal(canonicalContract(b))
	return bytes.Equal(aa, bb)
}
func validateContract(c Contract) error {
	if c.CoverageStart < 0 || c.WindowMS <= 0 || c.CoverageStart%c.WindowMS != 0 || c.DedupMS != 1 || c.LatenessMS < 0 || c.AgeMS <= c.LatenessMS {
		return errors.New("invalid catalog time/dedup/age/lateness contract")
	}
	if len(c.Tenants) < 1 || len(c.Tenants) > 100 || len(c.Sources) < 1 || len(c.Sources) > 100 {
		return errors.New("catalog requires 1..100 tenants and sources")
	}
	tenants := map[string]bool{}
	for _, s := range c.Tenants {
		t, e := pack.ParseTenant(s)
		if e != nil || t.String() != s || tenants[s] {
			return errors.New("invalid catalog tenant contract")
		}
		tenants[s] = true
	}
	sources := map[string]bool{}
	for _, s := range c.Sources {
		if !validID(s.ID) || !validHash(s.Fingerprint) || sources[s.ID] {
			return errors.New("invalid catalog source contract")
		}
		sources[s.ID] = true
	}
	return nil
}
func Validate(m *Manifest) error {
	if m == nil || m.Version != Version || m.VMVersion != pack.VMVersion || !validID(m.ID) {
		return errors.New("unsupported catalog version, VM version or identity")
	}
	if len(m.Entries) > MaxEntries {
		return errors.New("catalog exceeds entry limit; compaction not implemented")
	}
	if m.Generation == 0 {
		if len(m.Entries) != 0 || !EqualContract(m.Contract, Contract{}) {
			return errors.New("uninitialized generation must have no contract or entries")
		}
		return nil
	}
	if e := validateContract(m.Contract); e != nil {
		return e
	}
	tenants := map[string]bool{}
	for _, t := range m.Contract.Tenants {
		tenants[t] = true
	}
	sources := map[string]string{}
	for _, s := range m.Contract.Sources {
		sources[s.ID] = s.Fingerprint
	}
	bySource := map[string][]Entry{}
	windows := map[int64]map[string]bool{}
	for _, e := range m.Entries {
		if !tenants[e.Tenant] || sources[e.SourceID] != e.SourceFingerprint || !validHash(e.IndexSHA256) {
			return errors.New("entry violates tenant/source/checksum contract")
		}
		if e.Start < m.Contract.CoverageStart || e.Start < 0 || e.End <= e.Start || e.End-e.Start != m.Contract.WindowMS || (e.Start-m.Contract.CoverageStart)%m.Contract.WindowMS != 0 {
			return errors.New("entry must cover one full aligned contract window")
		}
		prefix := "tenants/" + e.Tenant + "/"
		if !validObjectKey(e.IndexKey, prefix) || !validObjectKey(e.DataKey, prefix) || e.IndexKey == e.DataKey {
			return errors.New("object escapes tenant namespace")
		}
		key := e.Tenant + "\x00" + e.SourceID
		bySource[key] = append(bySource[key], e)
		if windows[e.Start] == nil {
			windows[e.Start] = map[string]bool{}
		}
		if windows[e.Start][key] {
			return errors.New("duplicate window source")
		}
		windows[e.Start][key] = true
	}
	for _, w := range windows {
		if len(w) != len(tenants)*len(sources) {
			return errors.New("incomplete tenant/source window")
		}
	}
	for _, es := range bySource {
		sort.Slice(es, func(i, j int) bool { return es[i].Start < es[j].Start })
		for i := 1; i < len(es); i++ {
			if es[i].Start < es[i-1].End {
				return errors.New("overlapping catalog coverage")
			}
		}
	}
	return nil
}
func validHash(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, e := hex.DecodeString(s)
	return e == nil
}
func validObjectKey(key, prefix string) bool {
	if !strings.HasPrefix(key, prefix) || len(key) <= len(prefix) || len(key) > 1024 || path.Clean(key) != key || strings.ContainsAny(key, "\\\x00\r\n") {
		return false
	}
	for _, p := range strings.Split(key, "/") {
		if strings.HasPrefix(p, ".") {
			return false
		}
	}
	return true
}
func marshal(m *Manifest) ([]byte, error) {
	if e := Validate(m); e != nil {
		return nil, e
	}
	c := *m
	c.Contract = canonicalContract(c.Contract)
	b, e := json.Marshal(&c)
	if len(b) > MaxBytes {
		return nil, errors.New("catalog exceeds byte limit")
	}
	return b, e
}
func decode(b []byte) (*Manifest, error) {
	if len(b) > MaxBytes {
		return nil, errors.New("catalog exceeds byte limit")
	}
	var m Manifest
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(&m); e != nil {
		return nil, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("trailing catalog data")
	}
	if e := Validate(&m); e != nil {
		return nil, e
	}
	return &m, nil
}
func Load(ctx context.Context, s Store) (*Manifest, uint64, error) {
	if s == nil {
		return nil, 0, errors.New("catalog authority is required")
	}
	m, e := s.Read(ctx)
	if e != nil {
		return nil, 0, e
	}
	if e = Validate(m); e != nil {
		return nil, 0, e
	}
	return m, m.Generation, nil
}
func Commit(ctx context.Context, s Store, m *Manifest, expected uint64) (uint64, error) {
	if m == nil || m.Generation != expected || expected == ^uint64(0) {
		return 0, errors.New("invalid expected generation")
	}
	next := *m
	next.Generation = expected + 1
	next.Entries = append([]Entry(nil), m.Entries...)
	next.Contract = canonicalContract(m.Contract)
	if e := Validate(&next); e != nil {
		return 0, e
	}
	if s == nil {
		return 0, errors.New("catalog authority is required")
	}
	if e := s.Publish(ctx, expected, &next); e != nil {
		return 0, fmt.Errorf("catalog commit: %w", e)
	}
	m.Generation = next.Generation
	m.Contract = next.Contract
	return next.Generation, nil
}

// checkTransition validates while holding the authority lock. It returns true
// when an identical publication already committed (response-loss retry).
func checkTransition(old, next *Manifest, expected uint64) (bool, error) {
	if e := Validate(next); e != nil {
		return false, e
	}
	if old.ID != next.ID {
		return false, fmt.Errorf("%w: catalog identity mismatch", ErrInvalidPublication)
	}
	if expected == ^uint64(0) || next.Generation != expected+1 {
		return false, fmt.Errorf("%w: invalid next generation", ErrInvalidPublication)
	}
	if old.Generation == next.Generation {
		a, _ := marshal(old)
		b, _ := marshal(next)
		if bytes.Equal(a, b) {
			return true, nil
		}
	}
	if old.Generation != expected {
		return false, ErrConflict
	}
	if old.Generation > 0 && !EqualContract(old.Contract, next.Contract) {
		return false, fmt.Errorf("%w: catalog contract is immutable", ErrInvalidPublication)
	}
	if len(next.Entries) <= len(old.Entries) {
		return false, fmt.Errorf("%w: publication must append complete windows", ErrInvalidPublication)
	}
	for i, e := range old.Entries {
		if next.Entries[i] != e {
			return false, fmt.Errorf("%w: published entries are immutable", ErrInvalidPublication)
		}
	}
	return false, nil
}
func Coverage(m *Manifest, tenant string, sources []string, start int64) int64 {
	if m == nil || len(sources) == 0 {
		return start
	}
	minEnd := int64(^uint64(0) >> 1)
	for _, src := range sources {
		end := start
		var es []Entry
		for _, e := range m.Entries {
			if e.Tenant == tenant && e.SourceID == src {
				es = append(es, e)
			}
		}
		sort.Slice(es, func(i, j int) bool { return es[i].Start < es[j].Start })
		for _, e := range es {
			if e.Start > end {
				break
			}
			if e.End > end {
				end = e.End
			}
		}
		if end < minEnd {
			minEnd = end
		}
	}
	return minEnd
}
