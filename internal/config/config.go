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
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

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
	ArchiveInterval string `json:"archive_interval,omitempty"`
	ArchiveTimeout  string `json:"archive_timeout,omitempty"`
}

// Catalog selects an authoritative control plane, independent of blob storage.
type Catalog struct {
	Type string `json:"type"`
	Path string `json:"path"`
	URL  string `json:"url"`
	ID   string `json:"id"`
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
	age, retention, lateness, safety, window time.Duration
	archiveInterval                          time.Duration
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
func (c *Config) Validate() error {
	if !safeID.MatchString(c.Catalog.ID) || c.Catalog.ID == "." || c.Catalog.ID == ".." {
		return errors.New("catalog.id must be a pinned nonempty safe identity (max 128 bytes); legacy S3-only catalogs require explicit migration")
	}
	switch c.Catalog.Type {
	case "local", "sqlite":
		if c.Catalog.Path == "" || c.Catalog.URL != "" {
			return errors.New("local/SQLite catalog requires path and no url")
		}
		if c.Store.Type == "local" && samePath(c.Catalog.Path, c.Store.Path) {
			return errors.New("catalog and blob directories must be distinct")
		}
	case "http":
		u, err := url.Parse(c.Catalog.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || c.Catalog.Path != "" {
			return errors.New("HTTP catalog requires an origin URL without credentials/path/query/fragment and no path field")
		}
		c.Catalog.URL = strings.TrimRight(c.Catalog.URL, "/")
	default:
		return errors.New("catalog.type must be local, http or sqlite; S3 is not a catalog authority")
	}
	switch c.Store.Type {
	case "local":
		if c.Store.Path == "" {
			return errors.New("local blob store requires path")
		}
		if c.Store.RequestsPerSecond != 0 || c.Store.MaxConcurrentRequests != 0 || c.Store.MaxQueuedRequests != 0 {
			return errors.New("S3 request limits do not apply to local blobs")
		}
	case "s3":
		if c.Store.Bucket == "" {
			return errors.New("S3 blob store requires bucket")
		}
		if c.Catalog.Type == "sqlite" && c.Store.RequestsPerSecond == 0 && c.Store.MaxConcurrentRequests == 0 && c.Store.MaxQueuedRequests == 0 {
			c.Store.RequestsPerSecond, c.Store.MaxConcurrentRequests, c.Store.MaxQueuedRequests = 80, 8, 64
		}
		if c.Store.RequestsPerSecond != 0 || c.Store.MaxConcurrentRequests != 0 || c.Store.MaxQueuedRequests != 0 {
			if math.IsNaN(c.Store.RequestsPerSecond) || math.IsInf(c.Store.RequestsPerSecond, 0) || c.Store.RequestsPerSecond <= 0 || c.Store.RequestsPerSecond > 1000000 || c.Store.MaxConcurrentRequests < 1 || c.Store.MaxConcurrentRequests > 10000 || c.Store.MaxQueuedRequests < 1 || c.Store.MaxQueuedRequests > 100000 {
				return errors.New("configure positive bounded S3 requests_per_second, max_concurrent_requests and max_queued_requests together")
			}
		}
	default:
		return errors.New("store.type must be local or s3")
	}
	if c.Catalog.Type == "sqlite" {
		if _, err := c.BlobLocator(); err != nil {
			return err
		}
	}
	c.archiveTimeout = 2 * time.Minute
	if c.Service.ArchiveTimeout != "" {
		var err error
		c.archiveTimeout, err = time.ParseDuration(c.Service.ArchiveTimeout)
		if err != nil || c.archiveTimeout < 10*time.Millisecond || c.archiveTimeout > 24*time.Hour || c.archiveTimeout%time.Millisecond != 0 {
			return errors.New("service.archive_timeout must be whole milliseconds between 10ms and 24h")
		}
	}
	c.archiveInterval = 30 * time.Second
	if c.Service.ArchiveInterval != "" {
		var err error
		c.archiveInterval, err = time.ParseDuration(c.Service.ArchiveInterval)
		if err != nil || c.archiveInterval < 10*time.Millisecond || c.archiveInterval > 24*time.Hour || c.archiveInterval%time.Millisecond != 0 {
			return errors.New("service.archive_interval must be whole milliseconds between 10ms and 24h")
		}
	}

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
	if len(c.Tenants) == 0 || len(c.Tenants) > 100 || len(c.Sources) == 0 || len(c.Sources) > 100 {
		return errors.New("require 1..100 tenants and sources")
	}
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
	seen = map[string]bool{}
	urls := map[string]bool{}
	for i, s := range c.Sources {
		if !safeID.MatchString(s.ID) || s.ID == "." || s.ID == ".." || seen[s.ID] {
			return errors.New("invalid or duplicate source ID")
		}
		seen[s.ID] = true
		u, e := url.Parse(s.URL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("invalid source URL; use hot-only vmselect base URL without credentials")
		}
		s.URL = strings.TrimRight(s.URL, "/")
		if urls[s.URL] {
			return errors.New("duplicate source URL")
		}
		urls[s.URL] = true
		c.Sources[i] = s
	}
	if c.CacheBytes < 0 || c.CacheBytes > 256<<20 || c.MaxQueryBytes < 0 || c.MaxQueryBytes > 1<<30 {
		return errors.New("cache/query budgets outside safe bounds")
	}
	return nil
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
	switch c.Store.Type {
	case "local":
		return objectstore.NewLocal(c.Store.Path, 64<<20)
	case "s3":
		return objectstore.NewS3(objectstore.S3Config{Endpoint: c.Store.Endpoint, Region: c.Store.Region, Bucket: c.Store.Bucket, Prefix: c.Store.Prefix, RequestsPerSecond: c.Store.RequestsPerSecond, MaxConcurrentRequests: c.Store.MaxConcurrentRequests, MaxQueuedRequests: c.Store.MaxQueuedRequests})
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
	case "sqlite":
		return nil, errors.New("SQLite uses the bounded repository; use serve or offline SQLite maintenance")
	default:
		return nil, errors.New("invalid catalog type")
	}
}
func (c *Config) Archive(start, end int64) archive.Config {
	sources := make([]archive.Source, len(c.Sources))
	for i, s := range c.Sources {
		sources[i] = archive.Source{ID: s.ID, URL: s.URL}
	}
	return archive.Config{CoverageStart: c.CoverageStart, Tenants: c.Tenants, Sources: sources, StartMS: start, EndMS: end, Age: c.age, Retention: c.retention, Lateness: c.lateness, SafetyMargin: c.safety, Window: c.window, LatenessEnforced: c.LatenessEnforced}
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
