package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/yuuki/vmtier/internal/pack"
	"github.com/yuuki/vmtier/internal/querylimit"
)

const (
	SQLiteSchemaVersion     = 1
	SQLiteFile              = "catalog.db"
	MaxPacksPerWindow       = 1024
	SQLiteMaxPacksPerWindow = MaxPacksPerWindow
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
	var err error
	switch b.Type {
	case "local":
		b, err = normalizeLocalLocator(b)
	case "s3":
		b, err = normalizeS3Locator(b)
	default:
		return b, errors.New("blob locator type must be local or s3")
	}
	if err != nil {
		return b, err
	}
	encoded, _ := json.Marshal(b)
	if len(encoded) > 16<<10 {
		return b, errors.New("blob locator is too large")
	}
	return b, nil
}

func normalizeLocalLocator(b BlobLocator) (BlobLocator, error) {
	if b.Path == "" || b.Endpoint != "" || b.Region != "" || b.Bucket != "" || b.Prefix != "" {
		return b, errors.New("local blob locator requires only a path")
	}
	p, err := filepath.Abs(b.Path)
	if err != nil {
		return b, err
	}
	b.Path = filepath.Clean(p)
	return b, nil
}

// normalizeS3Locator applies the default region and trims the trailing
// slashes of the prefix and endpoint.
func normalizeS3Locator(b BlobLocator) (BlobLocator, error) {
	if b.Path != "" || b.Bucket == "" || strings.ContainsAny(b.Bucket, "/\\\r\n\x00") {
		return b, errors.New("invalid S3 blob locator")
	}
	if b.Region == "" {
		b.Region = "us-east-1"
	}
	if b.Prefix != "" {
		b.Prefix = strings.TrimSuffix(b.Prefix, "/")
		if err := validateS3Prefix(b.Prefix); err != nil {
			return b, err
		}
	}
	if b.Endpoint != "" {
		u, err := url.Parse(b.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return b, errors.New("invalid S3 endpoint")
		}
		b.Endpoint = strings.TrimSuffix(b.Endpoint, "/")
	}
	return b, nil
}

// validateS3Prefix requires a clean relative key prefix without hidden components.
func validateS3Prefix(prefix string) error {
	if prefix == "" || len(prefix) > 1024 || strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix || strings.ContainsAny(prefix, "\\\x00\r\n") {
		return errors.New("invalid S3 prefix")
	}
	for _, part := range strings.Split(prefix, "/") {
		if strings.HasPrefix(part, ".") {
			return errors.New("invalid S3 prefix component")
		}
	}
	return nil
}

type StreamCoverage struct {
	Tenant   string `json:"tenant"`
	SourceID string `json:"source_id"`
	End      int64  `json:"end_ms"`
}

// Repository owns initialized publication metadata independently of blob storage.
// Implementations pin catalog identity and return consistent, bounded selections.
// Publication callers must verify source completeness and uploaded objects first.
// Close belongs to the runtime owner, not individual readers or archive passes.
type Repository interface {
	Status(context.Context) (Status, error)
	PublishWindow(context.Context, uint64, Publication) (uint64, error)
	Select(context.Context, Selection) (QueryPlan, error)
	Close() error
}

var _ Repository = (*SQLite)(nil)

// Status describes one consistent snapshot of an initialized catalog.
type Status struct {
	SchemaVersion int              `json:"schema_version"`
	VMVersion     string           `json:"vm_version"`
	ID            string           `json:"id"`
	Generation    uint64           `json:"generation"`
	Contract      Contract         `json:"contract"`
	Blob          BlobLocator      `json:"blob"`
	Coverage      []StreamCoverage `json:"coverage"`
}

// SQLiteStatus is retained for SQLite maintenance and backup compatibility.
type SQLiteStatus = Status

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
	if len(p.Packs) > MaxPacksPerWindow {
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

// tenantCoverageEnd returns the coverage end of the tenant's least advanced
// stream, or MaxInt64 when the tenant has no stream.
func tenantCoverageEnd(coverage []StreamCoverage, tenant string) int64 {
	end := int64(math.MaxInt64)
	for _, c := range coverage {
		if c.Tenant == tenant && c.End < end {
			end = c.End
		}
	}
	return end
}

// windowStarts returns the first and last start of the windows that a
// selection overlaps, beginning no earlier than the coverage origin.
func windowStarts(sel Selection, c Contract) (lower, upper int64) {
	lower = max((sel.Start/c.WindowMS)*c.WindowMS, c.CoverageStart)
	upper = ((sel.End - 1) / c.WindowMS) * c.WindowMS
	return lower, upper
}

// planReader builds one query plan in a repository's read transaction. It
// charges the selected windows and pack references against the selection's
// window, pack and plan byte limits. Each repository supplies its own queries.
type planReader struct {
	tx        *sql.Tx
	sel       Selection
	state     Status
	planBytes int64
	packCount int
	selected  []selectedWindow
}

// selectedWindow is a stored window whose pack references are not loaded yet.
// digest is the stored publication digest, if the repository reads it.
type selectedWindow struct {
	id     int64
	count  int
	digest string
	window Window
}

func newPlanReader(tx *sql.Tx, sel Selection, state Status) *planReader {
	return &planReader{tx: tx, sel: sel, state: state, planBytes: 128}
}

// windowLimit bounds a window query to one row more than the plan can still
// hold, so that a plan over the window limit is detected.
func (p *planReader) windowLimit() int { return p.sel.MaxWindows - len(p.selected) + 1 }

func (p *planReader) newWindow(sourceID string) selectedWindow {
	var v selectedWindow
	v.window.Tenant, v.window.SourceID = p.sel.Tenant, sourceID
	return v
}

// addWindow validates a stored window and adds it to the plan within the
// window and plan byte limits.
func (p *planReader) addWindow(v selectedWindow, generation int64) error {
	if generation < 1 || generation > int64(p.state.Generation) || v.count < 0 || v.count > MaxPacksPerWindow || v.window.End-v.window.Start != p.state.Contract.WindowMS {
		return errors.New("invalid stored window")
	}
	v.window.Generation = uint64(generation)
	p.planBytes += 256 + int64(len(p.sel.Tenant)+len(v.window.SourceID))
	if len(p.selected) >= p.sel.MaxWindows || p.planBytes > p.sel.MaxPlanBytes {
		return querylimit.Errorf(querylimit.CatalogPlan, "query catalog plan limit exceeded")
	}
	p.selected = append(p.selected, v)
	return nil
}

// reservePacks charges the packs of a selected window against the pack limit.
func (p *planReader) reservePacks(count int) error {
	if count > p.sel.MaxPacks-p.packCount {
		return querylimit.Errorf(querylimit.CatalogPlan, "query catalog pack limit exceeded")
	}
	p.packCount += count
	return nil
}

// readPacks sets the pack references of v from rows, which must list them in
// ordinal order, within the plan byte limit.
func (p *planReader) readPacks(rows *sql.Rows, v *selectedWindow) error {
	v.window.Packs = []PackRef{}
	for rows.Next() {
		var ordinal int
		var ref PackRef
		if err := rows.Scan(&ordinal, &ref.DataKey, &ref.IndexKey, &ref.DataSHA256, &ref.IndexSHA256, &ref.DataBytes, &ref.IndexBytes); err != nil {
			return err
		}
		if ordinal != len(v.window.Packs) || ordinal >= v.count {
			return errors.New("invalid stored pack order/count")
		}
		p.planBytes += 256 + int64(len(ref.DataKey)+len(ref.IndexKey)+len(ref.DataSHA256)+len(ref.IndexSHA256))
		if p.planBytes > p.sel.MaxPlanBytes {
			return querylimit.Errorf(querylimit.CatalogPlan, "query catalog plan byte limit exceeded")
		}
		v.window.Packs = append(v.window.Packs, ref)
	}
	return rows.Err()
}

// checkPacks requires a loaded window to have all of its pack references and
// to form a valid publication, whose digest it returns.
func (p *planReader) checkPacks(v selectedWindow) (string, error) {
	if len(v.window.Packs) != v.count {
		return "", errors.New("stored window is missing pack references")
	}
	return validatePublication(p.state.Contract, v.window.Publication)
}
