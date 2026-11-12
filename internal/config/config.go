package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/yuuki/vmtier/internal/archive"
	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/cold"
	"github.com/yuuki/vmtier/internal/gateway"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

type Store struct {
	Type                  string  `json:"type"`
	Path                  string  `json:"path"`
	Endpoint              string  `json:"endpoint"`
	Region                string  `json:"region"`
	Bucket                string  `json:"bucket"`
	Prefix                string  `json:"prefix"`
	RequestsPerSecond     float64 `json:"requests_per_second,omitempty"`
	MaxConcurrentRequests int     `json:"max_concurrent_requests,omitempty"`
	MaxQueuedRequests     int     `json:"max_queued_requests,omitempty"`
}

type Service struct {
	MaxReplicas     int    `json:"max_replicas,omitempty"`
	ArchiveInterval string `json:"archive_interval,omitempty"`
	ArchiveTimeout  string `json:"archive_timeout,omitempty"`
	ArchiveLeaseTTL string `json:"archive_lease_ttl,omitempty"`
	// OrphanGCInterval enables garbage collection of failed attempts' objects.
	OrphanGCInterval string `json:"orphan_gc_interval,omitempty"`
	OrphanGCMinAge   string `json:"orphan_gc_min_age,omitempty"`
}

// Catalog selects an authoritative control plane, independent of blob storage.
type Catalog struct {
	Type   string `json:"type"`
	Path   string `json:"path"`
	URL    string `json:"url"`
	ID     string `json:"id"`
	DSNEnv string `json:"dsn_env,omitempty"`
}
type Source struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}
type Config struct {
	archiveTimeout                           time.Duration
	Store                                    Store    `json:"store"`
	Catalog                                  Catalog  `json:"catalog"`
	Service                                  Service  `json:"service,omitempty"`
	Tenants                                  []string `json:"tenants"`
	Sources                                  []Source `json:"sources"`
	ArchiveAge                               string   `json:"archive_age"`
	HotRetention                             string   `json:"hot_retention"`
	Lateness                                 string   `json:"lateness"`
	SafetyMargin                             string   `json:"safety_margin"`
	Window                                   string   `json:"window"`
	LatenessEnforced                         bool     `json:"lateness_enforced"`
	CoverageStart                            int64    `json:"coverage_start_ms"`
	CacheBytes                               int64    `json:"cache_bytes"`
	MaxQueryBytes                            int64    `json:"max_query_bytes"`
	MaxPackBlocks                            int      `json:"max_pack_blocks,omitempty"`
	QueryMemoryBytes                         int64    `json:"query_memory_bytes,omitempty"`
	age, retention, lateness, safety, window time.Duration
	archiveInterval                          time.Duration
	archiveLeaseTTL                          time.Duration
	orphanGCInterval, orphanGCMinAge         time.Duration
}

func Read(path string) (*Config, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if e != nil {
		return nil, e
	}
	if len(b) > 1<<20 {
		return nil, errors.New("config too large")
	}
	var c Config
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return nil, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("trailing config data")
	}
	if e = c.Validate(); e != nil {
		return nil, e
	}
	return &c, nil
}

// Validate checks each section in turn and normalizes the configuration in
// place: it applies defaults, canonicalizes tenants and trims URLs.
func (c *Config) Validate() error {
	if err := c.validateCatalog(); err != nil {
		return err
	}
	if err := c.validateStore(); err != nil {
		return err
	}
	if c.Catalog.Type == "sqlite" || c.Catalog.Type == "postgres" {
		if _, err := c.BlobLocator(); err != nil {
			return err
		}
	}
	if _, err := c.perReplicaStore(); err != nil {
		return err
	}
	if err := c.validateService(); err != nil {
		return err
	}
	if err := c.validatePolicy(); err != nil {
		return err
	}
	if len(c.Tenants) == 0 || len(c.Tenants) > 100 || len(c.Sources) == 0 || len(c.Sources) > 100 {
		return errors.New("require 1..100 tenants and sources")
	}
	if err := c.normalizeTenants(); err != nil {
		return err
	}
	if err := c.normalizeSources(); err != nil {
		return err
	}
	return c.validateBudgets()
}

// validateCatalog also trims the trailing slashes of an HTTP catalog URL.
func (c *Config) validateCatalog() error {
	if !safeID.MatchString(c.Catalog.ID) || c.Catalog.ID == "." || c.Catalog.ID == ".." {
		return errors.New("catalog.id must be a pinned nonempty safe identity (max 128 bytes); legacy S3-only catalogs require explicit migration")
	}
	switch c.Catalog.Type {
	case "local", "sqlite":
		if c.Catalog.Path == "" || c.Catalog.URL != "" || c.Catalog.DSNEnv != "" {
			return errors.New("local/SQLite catalog requires path and no url or dsn_env")
		}
		if c.Store.Type == "local" && samePath(c.Catalog.Path, c.Store.Path) {
			return errors.New("catalog and blob directories must be distinct")
		}
	case "http":
		u, err := url.Parse(c.Catalog.URL)
		if err != nil || !isHTTPBaseURL(u) || (u.Path != "" && u.Path != "/") || c.Catalog.Path != "" || c.Catalog.DSNEnv != "" {
			return errors.New("HTTP catalog requires an origin URL without credentials/path/query/fragment and no path field")
		}
		c.Catalog.URL = strings.TrimRight(c.Catalog.URL, "/")
	case "postgres":
		if !environmentName.MatchString(c.Catalog.DSNEnv) || c.Catalog.Path != "" || c.Catalog.URL != "" {
			return errors.New("PostgreSQL catalog requires dsn_env naming an environment variable, with no path or url")
		}
		if c.Store.Type != "s3" {
			return errors.New("PostgreSQL service requires shared S3 blob storage")
		}
	default:
		return errors.New("catalog.type must be local, http, sqlite or postgres; S3 is not a catalog authority")
	}
	return nil
}

// validateStore also applies the default S3 request limits of a SQLite catalog.
// A PostgreSQL catalog requires explicit aggregate limits.
func (c *Config) validateStore() error {
	switch c.Store.Type {
	case "local":
		if c.Store.Path == "" {
			return errors.New("local blob store requires path")
		}
		if c.Store.hasRequestLimits() {
			return errors.New("S3 request limits do not apply to local blobs")
		}
	case "s3":
		if c.Store.Bucket == "" {
			return errors.New("S3 blob store requires bucket")
		}
		if c.Catalog.Type == "sqlite" && !c.Store.hasRequestLimits() {
			c.Store.RequestsPerSecond, c.Store.MaxConcurrentRequests, c.Store.MaxQueuedRequests = 80, 8, 64
		}
		if c.Catalog.Type == "postgres" && (c.Store.RequestsPerSecond == 0 || c.Store.MaxConcurrentRequests == 0 || c.Store.MaxQueuedRequests == 0) {
			return errors.New("PostgreSQL service requires explicit aggregate S3 request limits")
		}
		if c.Store.hasRequestLimits() && !c.Store.requestLimitsBounded() {
			return errors.New("configure positive bounded S3 requests_per_second, max_concurrent_requests and max_queued_requests together")
		}
	default:
		return errors.New("store.type must be local or s3")
	}
	return nil
}

func (s Store) hasRequestLimits() bool {
	return s.RequestsPerSecond != 0 || s.MaxConcurrentRequests != 0 || s.MaxQueuedRequests != 0
}

func (s Store) requestLimitsBounded() bool {
	if math.IsNaN(s.RequestsPerSecond) || math.IsInf(s.RequestsPerSecond, 0) || s.RequestsPerSecond <= 0 || s.RequestsPerSecond > 1000000 {
		return false
	}
	return s.MaxConcurrentRequests >= 1 && s.MaxConcurrentRequests <= 10000 && s.MaxQueuedRequests >= 1 && s.MaxQueuedRequests <= 100000
}

func (c *Config) validateService() error {
	if c.Service.ArchiveLeaseTTL != "" && c.Catalog.Type != "postgres" {
		return errors.New("service.archive_lease_ttl requires a PostgreSQL catalog")
	}
	var ok bool
	if c.archiveLeaseTTL, ok = serviceDuration(c.Service.ArchiveLeaseTTL, 30*time.Second, 3*time.Second); !ok {
		return errors.New("service.archive_lease_ttl must be whole milliseconds between 3s and 24h")
	}
	if c.archiveTimeout, ok = serviceDuration(c.Service.ArchiveTimeout, 2*time.Minute, 10*time.Millisecond); !ok {
		return errors.New("service.archive_timeout must be whole milliseconds between 10ms and 24h")
	}
	if c.archiveInterval, ok = serviceDuration(c.Service.ArchiveInterval, 30*time.Second, 10*time.Millisecond); !ok {
		return errors.New("service.archive_interval must be whole milliseconds between 10ms and 24h")
	}
	return c.validateOrphanGC()
}

// DefaultOrphanGCMinAge is the default age at which garbage collection may
// claim the objects of an attempt that has not published.
const DefaultOrphanGCMinAge = 24 * time.Hour

// validateOrphanGC parses the optional garbage collection settings. Only the
// service backends have the object ledger that garbage collection needs. The
// minimum age keeps collection from discarding attempts that may still run.
func (c *Config) validateOrphanGC() error {
	c.orphanGCInterval, c.orphanGCMinAge = 0, 0
	if c.Service.OrphanGCInterval == "" {
		if c.Service.OrphanGCMinAge != "" {
			return errors.New("service.orphan_gc_min_age requires service.orphan_gc_interval")
		}
		return nil
	}
	if c.Catalog.Type != "sqlite" && c.Catalog.Type != "postgres" {
		return errors.New("service.orphan_gc_interval requires a SQLite or PostgreSQL catalog")
	}
	var ok bool
	if c.orphanGCInterval, ok = serviceDuration(c.Service.OrphanGCInterval, 0, 10*time.Millisecond); !ok {
		return errors.New("service.orphan_gc_interval must be whole milliseconds between 10ms and 24h")
	}
	c.orphanGCMinAge = DefaultOrphanGCMinAge
	if c.Service.OrphanGCMinAge != "" {
		d, err := time.ParseDuration(c.Service.OrphanGCMinAge)
		if err != nil || d%time.Millisecond != 0 || d > 30*24*time.Hour {
			return errors.New("service.orphan_gc_min_age must be whole milliseconds up to 720h")
		}
		c.orphanGCMinAge = d
	}
	if c.orphanGCMinAge < c.archiveTimeout {
		return errors.New("service.orphan_gc_min_age must be at least service.archive_timeout")
	}
	return nil
}

// serviceDuration parses an optional service period, which must be whole
// milliseconds between minimum and 24h. An empty value selects the default.
func serviceDuration(value string, defaultValue, minimum time.Duration) (time.Duration, bool) {
	if value == "" {
		return defaultValue, true
	}
	d, err := time.ParseDuration(value)
	return d, err == nil && d >= minimum && d <= 24*time.Hour && d%time.Millisecond == 0
}

// validatePolicy parses the archive durations and checks the retention,
// lateness and coverage contract they define.
func (c *Config) validatePolicy() error {
	fields := []struct {
		s string
		p *time.Duration
	}{{c.ArchiveAge, &c.age}, {c.HotRetention, &c.retention}, {c.Lateness, &c.lateness}, {c.SafetyMargin, &c.safety}, {c.Window, &c.window}}
	for _, f := range fields {
		v, e := time.ParseDuration(f.s)
		if e != nil {
			return e
		}
		if v%time.Millisecond != 0 {
			return errors.New("durations must be whole milliseconds")
		}
		*f.p = v
	}
	if c.lateness < 0 || c.age <= c.lateness || c.safety <= 0 || c.window <= 0 || c.window.Milliseconds() == 0 || c.age <= 0 || c.retention <= c.age || c.retention-c.age <= c.safety || c.retention-c.age-c.safety <= c.window {
		return errors.New("require H>L>=0, window>0, safety>0 and R-H>safety+window")
	}
	if !c.LatenessEnforced {
		return errors.New("bounded lateness must be enforced outside vmtier; set lateness_enforced only after enforcing ingress policy")
	}
	if c.CoverageStart <= 0 || c.CoverageStart%c.window.Milliseconds() != 0 {
		return errors.New("coverage_start_ms must be positive and UTC window aligned")
	}
	return nil
}

// normalizeTenants rewrites each tenant in its canonical form and rejects duplicates.
func (c *Config) normalizeTenants() error {
	seen := map[string]bool{}
	for i, t := range c.Tenants {
		p, e := pack.ParseTenant(t)
		if e != nil {
			return e
		}
		t = p.String()
		if seen[t] {
			return errors.New("duplicate tenant")
		}
		seen[t] = true
		c.Tenants[i] = t
	}
	return nil
}

// normalizeSources trims the trailing slashes of each source URL and rejects
// duplicate IDs or URLs.
func (c *Config) normalizeSources() error {
	seen := map[string]bool{}
	urls := map[string]bool{}
	for i, s := range c.Sources {
		if !safeID.MatchString(s.ID) || s.ID == "." || s.ID == ".." || seen[s.ID] {
			return errors.New("invalid or duplicate source ID")
		}
		seen[s.ID] = true
		u, e := url.Parse(s.URL)
		if e != nil || !isHTTPBaseURL(u) {
			return errors.New("invalid source URL; use hot-only vmselect base URL without credentials")
		}
		s.URL = strings.TrimRight(s.URL, "/")
		if urls[s.URL] {
			return errors.New("duplicate source URL")
		}
		urls[s.URL] = true
		c.Sources[i] = s
	}
	return nil
}

// isHTTPBaseURL reports whether u is an HTTP(S) URL with a host and without
// credentials, query or fragment.
func isHTTPBaseURL(u *url.URL) bool {
	return u.Host != "" && (u.Scheme == "http" || u.Scheme == "https") && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func (c *Config) validateBudgets() error {
	if c.CacheBytes < 0 || c.CacheBytes > 256<<20 || c.MaxQueryBytes < 0 || c.MaxQueryBytes > 1<<30 {
		return errors.New("cache/query budgets outside safe bounds")
	}
	if c.MaxPackBlocks < 0 || c.MaxPackBlocks > pack.DefaultLimits().MaxBlocks {
		return errors.New("max_pack_blocks must be 0 (default) or 1..100000")
	}
	limits := c.GatewayLimits()
	if c.QueryMemoryBytes < 0 || limits.QueryMemoryBytes < limits.QueryWorkBytes || limits.QueryMemoryBytes > 8<<30 {
		return errors.New("query_memory_bytes must cover max_query_bytes and be at most 8 GiB (defaults: 256 MiB shared, 128 MiB per query)")
	}
	return nil
}

// GatewayLimits reserves the reader's full logical work budget before each query.
func (c *Config) GatewayLimits() gateway.Limits {
	work, shared := c.MaxQueryBytes, c.QueryMemoryBytes
	if work == 0 {
		work = 128 << 20
	}
	if shared == 0 {
		shared = 256 << 20
	}
	return gateway.Limits{QueryWorkBytes: work, QueryMemoryBytes: shared}
}

// ValidateArchiveRange prevents partial or differently aligned windows from
// making later scheduler retries overlap previously committed coverage.
func (c *Config) ValidateArchiveRange(start, end int64) error {
	if c.window < time.Millisecond {
		return errors.New("configuration must be validated before archive")
	}
	if start < c.CoverageStart || start >= end {
		return errors.New("archive range must start at/after coverage_start_ms and have start < end")
	}
	if start%c.window.Milliseconds() != 0 || end%c.window.Milliseconds() != 0 {
		return errors.New("archive start and end must be UTC window aligned")
	}
	return nil
}
func (c *Config) OpenBlobs() (objectstore.BlobStore, error) {
	store, err := c.perReplicaStore()
	if err != nil {
		return nil, err
	}
	switch store.Type {
	case "local":
		return objectstore.NewLocal(store.Path, 64<<20)
	case "s3":
		return objectstore.NewS3(objectstore.S3Config{Endpoint: store.Endpoint, Region: store.Region, Bucket: store.Bucket, Prefix: store.Prefix, RequestsPerSecond: store.RequestsPerSecond, MaxConcurrentRequests: store.MaxConcurrentRequests, MaxQueuedRequests: store.MaxQueuedRequests})
	default:
		return nil, fmt.Errorf("unsupported store type %q", c.Store.Type)
	}
}
func (c *Config) OpenCatalog() (catalog.Store, error) {
	switch c.Catalog.Type {
	case "local":
		return catalog.NewLocal(c.Catalog.Path, c.Catalog.ID, false)
	case "http":
		return catalog.NewHTTP(c.Catalog.URL, c.Catalog.ID, os.Getenv("VMTIER_CATALOG_TOKEN"))
	case "sqlite", "postgres":
		return nil, errors.New("SQLite and PostgreSQL use the bounded repository; use serve or catalog maintenance")
	default:
		return nil, errors.New("invalid catalog type")
	}
}
func (c *Config) Archive(start, end int64) archive.Config {
	sources := make([]archive.Source, len(c.Sources))
	for i, s := range c.Sources {
		sources[i] = archive.Source{ID: s.ID, URL: s.URL}
	}
	return archive.Config{CoverageStart: c.CoverageStart, Tenants: c.Tenants, Sources: sources, StartMS: start, EndMS: end, Age: c.age, Retention: c.retention, Lateness: c.lateness, SafetyMargin: c.safety, Window: c.window, LatenessEnforced: c.LatenessEnforced, PackLimits: pack.Limits{MaxBlocks: c.MaxPackBlocks}}
}
func (c *Config) Reader(s objectstore.BlobStore, cs catalog.Store) *cold.Reader {
	r := &cold.Reader{Store: s, CatalogStore: cs, DedupMS: 1, LatenessMS: c.lateness.Milliseconds(), Tenants: map[string]bool{}, SourceFingerprints: map[string]string{}, CoverageStart: c.CoverageStart, Age: c.age, Window: c.window, CacheBytes: c.CacheBytes, MaxQueryBytes: c.MaxQueryBytes}
	for _, t := range c.Tenants {
		r.Tenants[t] = true
	}
	for _, s := range c.Sources {
		r.Sources = append(r.Sources, s.ID)
		r.SourceFingerprints[s.ID] = pack.Sum([]byte(s.URL))
	}
	return r
}
func (c *Config) Retention() time.Duration { return c.retention }

func (c *Config) ArchiveInterval() time.Duration { return c.archiveInterval }

func (c *Config) ArchiveTimeout() time.Duration { return c.archiveTimeout }

func (c *Config) ArchiveLeaseTTL() time.Duration { return c.archiveLeaseTTL }

// OrphanGC returns the garbage collection interval, zero when disabled, and
// the age at which an unpublished object may be claimed.
func (c *Config) OrphanGC() (interval, minAge time.Duration) {
	return c.orphanGCInterval, c.orphanGCMinAge
}

// PostgresDSN resolves credentials at use time; connection strings are never
// stored in the JSON configuration or included in configuration errors.
func (c *Config) PostgresDSN() (string, error) {
	if c.Catalog.Type != "postgres" || !environmentName.MatchString(c.Catalog.DSNEnv) {
		return "", errors.New("PostgreSQL catalog requires a valid dsn_env")
	}
	dsn := os.Getenv(c.Catalog.DSNEnv)
	if strings.TrimSpace(dsn) == "" {
		return "", fmt.Errorf("PostgreSQL connection environment variable %s is unset or empty", c.Catalog.DSNEnv)
	}
	return dsn, nil
}

func (c *Config) ArchiveContract() catalog.Contract {
	out := catalog.Contract{CoverageStart: c.CoverageStart, WindowMS: c.window.Milliseconds(), AgeMS: c.age.Milliseconds(), LatenessMS: c.lateness.Milliseconds(), DedupMS: 1, Tenants: append([]string(nil), c.Tenants...)}
	for _, source := range c.Sources {
		out.Sources = append(out.Sources, catalog.Source{ID: source.ID, Fingerprint: pack.Sum([]byte(source.URL))})
	}
	return out
}

func (c *Config) BlobLocator() (catalog.BlobLocator, error) {
	return catalog.NormalizeBlobLocator(catalog.BlobLocator{Type: c.Store.Type, Path: c.Store.Path, Endpoint: c.Store.Endpoint, Region: c.Store.Region, Bucket: c.Store.Bucket, Prefix: c.Store.Prefix})
}

func (c *Config) RepositoryReader(s objectstore.BlobStore, db catalog.Repository) (*cold.Reader, error) {
	blob, err := c.BlobLocator()
	if err != nil {
		return nil, err
	}
	r := c.Reader(s, nil)
	r.Repository, r.BlobLocator = db, blob
	return r, nil
}

func samePath(a, b string) bool {
	absoluteA, errA := filepath.Abs(a)
	absoluteB, errB := filepath.Abs(b)
	if errA != nil || errB != nil {
		return false
	}
	if resolved, err := filepath.EvalSymlinks(absoluteA); err == nil {
		absoluteA = resolved
	}
	if resolved, err := filepath.EvalSymlinks(absoluteB); err == nil {
		absoluteB = resolved
	}
	return absoluteA == absoluteB
}
