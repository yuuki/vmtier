package catalog

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/yuuki/vmtier/internal/pack"
)

type publication struct {
	Expected *uint64   `json:"expected_generation"`
	Manifest *Manifest `json:"manifest"`
}

// Handler exposes the private authority API. For remote production deployment
// place it behind authenticated TLS; the optional bearer token is never logged.
func Handler(store Store, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if r.URL.Path == "/v1/catalog" && r.Method == http.MethodGet {
			m, e := store.Read(ctx)
			if e != nil {
				http.Error(w, "catalog authority unavailable: "+e.Error(), http.StatusServiceUnavailable)
				return
			}
			b, e := marshal(m)
			if e != nil {
				http.Error(w, "invalid authoritative state", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(b)
			return
		}
		if r.URL.Path == "/v1/catalog/commit" && r.Method == http.MethodPost {
			r.Body = http.MaxBytesReader(w, r.Body, MaxBytes+1024)
			defer r.Body.Close()
			d := json.NewDecoder(r.Body)
			d.DisallowUnknownFields()
			var p publication
			if e := d.Decode(&p); e != nil {
				http.Error(w, "invalid publication body", http.StatusBadRequest)
				return
			}
			var extra any
			if d.Decode(&extra) != io.EOF {
				http.Error(w, "trailing publication body", http.StatusBadRequest)
				return
			}
			if p.Expected == nil || p.Manifest == nil {
				http.Error(w, "expected_generation and manifest are required", http.StatusBadRequest)
				return
			}
			b, e := marshal(p.Manifest)
			if e != nil || *p.Expected == ^uint64(0) || p.Manifest.Generation != *p.Expected+1 {
				http.Error(w, "invalid publication", http.StatusBadRequest)
				return
			}
			if e = store.Publish(ctx, *p.Expected, p.Manifest); e != nil {
				status := http.StatusServiceUnavailable
				if errors.Is(e, ErrConflict) {
					status = http.StatusConflict
				}
				if errors.Is(e, ErrInvalidPublication) {
					status = http.StatusBadRequest
				}
				http.Error(w, e.Error(), status)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(b)
			return
		}
		if r.URL.Path == "/v1/catalog" || r.URL.Path == "/v1/catalog/commit" {
			http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
			return
		}
		http.NotFound(w, r)
	})
}

// Remote performs a fresh authority read per query. The gate serializes HTTP
// snapshots so an older in-flight response cannot race a newer one. Only the
// last generation/hash is retained; this is rollback detection while this client
// lives, not a durable checkpoint across a complete process restart.
type Remote struct {
	base, id, token string
	client          *http.Client
	gate            chan struct{}
	seen            bool
	generation      uint64
	hash            string
}

func NewHTTP(baseURL, id, token string) (*Remote, error) {
	u, e := url.Parse(baseURL)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("catalog URL must be an HTTP(S) origin without credentials, path, query or fragment")
	}
	if !validID(id) || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("invalid catalog identity or token")
	}
	c := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("catalog redirects are forbidden") }}
	return &Remote{base: strings.TrimRight(baseURL, "/"), id: id, token: token, client: c, gate: make(chan struct{}, 1)}, nil
}
func (r *Remote) lock(ctx context.Context) (func(), error) {
	select {
	case r.gate <- struct{}{}:
		return func() { <-r.gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (r *Remote) request(ctx context.Context, method, path string, body []byte) (*Manifest, error) {
	req, e := http.NewRequestWithContext(ctx, method, r.base+path, bytes.NewReader(body))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Cache-Control", "no-cache, no-store")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	res, e := r.client.Do(req)
	if e != nil {
		return nil, fmt.Errorf("catalog request failed (publish outcome may require reload): %w", e)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		if res.StatusCode == http.StatusConflict {
			return nil, fmt.Errorf("%w: %s", ErrConflict, strings.TrimSpace(string(b)))
		}
		return nil, fmt.Errorf("catalog HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(b)))
	}
	b, e := io.ReadAll(io.LimitReader(res.Body, MaxBytes+1))
	if e != nil {
		return nil, e
	}
	return decode(b)
}
func (r *Remote) observe(m *Manifest) error {
	if m.ID != r.id {
		return errors.New("catalog identity mismatch")
	}
	b, e := marshal(m)
	if e != nil {
		return e
	}
	sum := pack.Sum(b)
	if r.seen && (m.Generation < r.generation || (m.Generation == r.generation && sum != r.hash)) {
		return errors.New("catalog rollback or same-generation mutation detected")
	}
	r.seen = true
	r.generation = m.Generation
	r.hash = sum
	return nil
}
func (r *Remote) Read(ctx context.Context) (*Manifest, error) {
	unlock, e := r.lock(ctx)
	if e != nil {
		return nil, e
	}
	defer unlock()
	m, e := r.request(ctx, http.MethodGet, "/v1/catalog", nil)
	if e != nil {
		return nil, e
	}
	if e = r.observe(m); e != nil {
		return nil, e
	}
	return m, nil
}
func (r *Remote) Publish(ctx context.Context, expected uint64, next *Manifest) error {
	if next == nil || next.ID != r.id {
		return errors.New("catalog identity mismatch")
	}
	wanted, e := marshal(next)
	if e != nil {
		return e
	}
	if expected == ^uint64(0) || next.Generation != expected+1 {
		return errors.New("invalid next generation")
	}
	b, e := json.Marshal(publication{Expected: &expected, Manifest: next})
	if e != nil {
		return e
	}
	if len(b) > MaxBytes+1024 {
		return errors.New("publication exceeds byte limit")
	}
	unlock, e := r.lock(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	m, e := r.request(ctx, http.MethodPost, "/v1/catalog/commit", b)
	if e != nil {
		return e
	}
	got, e := marshal(m)
	if e != nil {
		return e
	}
	if !bytes.Equal(got, wanted) {
		return errors.New("catalog publication acknowledgement differs from candidate; reload required")
	}
	return r.observe(m)
}
func (r *Remote) Close() error { r.client.CloseIdleConnections(); return nil }
