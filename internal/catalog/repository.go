package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/yuuki/vmtier/internal/pack"
)

const (
	SQLiteSchemaVersion     = 1
	SQLiteFile              = "catalog.db"
	SQLiteMaxPacksPerWindow = 1024
)

// BlobLocator pins the location of relative object keys without storing secrets.
type BlobLocator struct {
	Type     string `json:"type"`
	Path     string `json:"path,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
	Region   string `json:"region,omitempty"`
	Bucket   string `json:"bucket,omitempty"`
	Prefix   string `json:"prefix,omitempty"`
}

// NormalizeBlobLocator applies the same S3 defaults and key rules as BlobStore.
// Local paths are absolute so changing the working directory cannot retarget keys.
func NormalizeBlobLocator(b BlobLocator) (BlobLocator, error) {
	switch b.Type {
	case "local":
		if b.Path == "" || b.Endpoint != "" || b.Region != "" || b.Bucket != "" || b.Prefix != "" {
			return b, errors.New("local blob locator requires only a path")
		}
		p, err := filepath.Abs(b.Path)
		if err != nil {
			return b, err
		}
		b.Path = filepath.Clean(p)
	case "s3":
		if b.Path != "" || b.Bucket == "" || strings.ContainsAny(b.Bucket, "/\\\r\n\x00") {
			return b, errors.New("invalid S3 blob locator")
		}
		if b.Region == "" {
			b.Region = "us-east-1"
		}
		if b.Prefix != "" {
			b.Prefix = strings.TrimSuffix(b.Prefix, "/")
			if b.Prefix == "" || len(b.Prefix) > 1024 || strings.HasPrefix(b.Prefix, "/") || path.Clean(b.Prefix) != b.Prefix || strings.ContainsAny(b.Prefix, "\\\x00\r\n") {
				return b, errors.New("invalid S3 prefix")
			}
			for _, part := range strings.Split(b.Prefix, "/") {
				if strings.HasPrefix(part, ".") {
					return b, errors.New("invalid S3 prefix component")
				}
			}
		}
		if b.Endpoint != "" {
			u, err := url.Parse(b.Endpoint)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
				return b, errors.New("invalid S3 endpoint")
			}
			b.Endpoint = strings.TrimSuffix(b.Endpoint, "/")
		}
	default:
		return b, errors.New("blob locator type must be local or s3")
	}
	encoded, _ := json.Marshal(b)
	if len(encoded) > 16<<10 {
		return b, errors.New("blob locator is too large")
	}
	return b, nil
}

type StreamCoverage struct {
	Tenant   string `json:"tenant"`
	SourceID string `json:"source_id"`
	End      int64  `json:"end_ms"`
}

type SQLiteStatus struct {
	SchemaVersion int              `json:"schema_version"`
	VMVersion     string           `json:"vm_version"`
	ID            string           `json:"id"`
	Generation    uint64           `json:"generation"`
	Contract      Contract         `json:"contract"`
	Blob          BlobLocator      `json:"blob"`
	Coverage      []StreamCoverage `json:"coverage"`
}

type PackRef struct {
	DataKey     string `json:"data_key"`
	IndexKey    string `json:"index_key"`
	DataSHA256  string `json:"data_sha256"`
	IndexSHA256 string `json:"index_sha256"`
	DataBytes   int64  `json:"data_bytes"`
	IndexBytes  int64  `json:"index_bytes"`
}

// Publication is one complete logical window for one configured stream. An
// empty pack list records a successfully verified empty window.
type Publication struct {
	Tenant   string    `json:"tenant"`
	SourceID string    `json:"source_id"`
	Start    int64     `json:"start_ms"`
	End      int64     `json:"end_ms"`
	Packs    []PackRef `json:"packs"`
}

type Window struct {
	Publication
	Generation uint64 `json:"generation"`
}

// Selection uses a half-open interval and finite plan limits. Zero limits select
// the defaults. MaxPlanBytes bounds descriptor size, not the process's total RAM.
type Selection struct {
	Tenant       string
	Start, End   int64
	MaxWindows   int
	MaxPacks     int
	MaxPlanBytes int64
}

// QueryPlan is a snapshot of immutable references; no database transaction stays
// open while the caller reads object data. Windows are ordered by source/time.
type QueryPlan struct {
	ID          string   `json:"id"`
	Generation  uint64   `json:"generation"`
	CoverageEnd int64    `json:"coverage_end_ms"`
	Windows     []Window `json:"windows"`
}

func validatePublication(c Contract, p Publication) (string, error) {
	foundTenant, foundSource := false, false
	for _, tenant := range c.Tenants {
		foundTenant = foundTenant || tenant == p.Tenant
	}
	for _, source := range c.Sources {
		foundSource = foundSource || source.ID == p.SourceID
	}
	if !foundTenant || !foundSource {
		return "", fmt.Errorf("%w: tenant/source is not configured", ErrInvalidPublication)
	}
	if p.Start < c.CoverageStart || p.Start < 0 || p.End <= p.Start || p.End-p.Start != c.WindowMS || (p.Start-c.CoverageStart)%c.WindowMS != 0 {
		return "", fmt.Errorf("%w: expected one aligned contract window", ErrInvalidPublication)
	}
	if len(p.Packs) > SQLiteMaxPacksPerWindow {
		return "", fmt.Errorf("%w: window has too many packs", ErrInvalidPublication)
	}
	keys := make(map[string]bool, len(p.Packs)*2)
	for _, ref := range p.Packs {
		prefix := "tenants/" + p.Tenant + "/"
		if !validObjectKey(ref.DataKey, prefix) || !validObjectKey(ref.IndexKey, prefix) || ref.DataKey == ref.IndexKey || keys[ref.DataKey] || keys[ref.IndexKey] {
			return "", fmt.Errorf("%w: invalid or repeated pack key", ErrInvalidPublication)
		}
		keys[ref.DataKey], keys[ref.IndexKey] = true, true
		// A legacy v2 empty window can reference a zero-byte data pack.
		if !validHash(ref.DataSHA256) || !validHash(ref.IndexSHA256) || ref.DataBytes < 0 || ref.DataBytes > 64<<20 || ref.IndexBytes <= 0 || ref.IndexBytes > 16<<20 {
			return "", fmt.Errorf("%w: invalid pack size or checksum", ErrInvalidPublication)
		}
	}
	p.Packs = append([]PackRef{}, p.Packs...)
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return pack.Sum(b), nil
}

func normalizeSelection(s Selection) (Selection, error) {
	if s.Start < 0 || s.End <= s.Start {
		return s, errors.New("selection requires a nonnegative half-open interval")
	}
	if s.MaxWindows == 0 {
		s.MaxWindows = 4096
	}
	if s.MaxPacks == 0 {
		s.MaxPacks = 4096
	}
	if s.MaxPlanBytes == 0 {
		s.MaxPlanBytes = 16 << 20
	}
	if s.MaxWindows < 1 || s.MaxWindows > 100000 || s.MaxPacks < 1 || s.MaxPacks > 100000 || s.MaxPlanBytes < 1 || s.MaxPlanBytes > 64<<20 {
		return s, errors.New("selection limits outside supported bounds")
	}
	return s, nil
}
