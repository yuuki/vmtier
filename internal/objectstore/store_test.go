package objectstore

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func blobContract(t *testing.T, s BlobStore, prefix string) {
	t.Helper()
	ctx := context.Background()
	key := prefix + "tenant/attempt-1/pack"
	if _, err := s.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Get: %v", err)
	}
	data := []byte("native-pack-payload")
	if err := s.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	// Identical retries are allowed; changed contents must use a new attempt key.
	if err := s.Put(ctx, key, data); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	b, err := s.Get(ctx, key)
	if err != nil || !bytes.Equal(b, data) {
		t.Fatalf("Get=%q %v", b, err)
	}
	b, err = s.GetRange(ctx, key, 3, 7)
	if err != nil || !bytes.Equal(b, data[3:10]) {
		t.Fatalf("range=%q %v", b, err)
	}
	if _, err = s.GetRange(ctx, key, 0, int64(len(data)+1)); err == nil {
		t.Fatal("oversized range accepted")
	}
	if _, err = s.GetRange(ctx, key, 0, 0); err == nil {
		t.Fatal("zero range accepted")
	}
	if _, err = s.GetRange(ctx, key, -1, 1); err == nil {
		t.Fatal("negative offset accepted")
	}
	if _, err = s.GetRange(ctx, key, int64(^uint64(0)>>1), 2); err == nil {
		t.Fatal("range overflow accepted")
	}
	if err = s.Put(ctx, prefix+"tenant/empty", nil); err != nil {
		t.Fatal(err)
	}
	if b, err = s.Get(ctx, prefix+"tenant/empty"); err != nil || len(b) != 0 {
		t.Fatalf("empty Get: %q %v", b, err)
	}
	for _, key := range []string{"../escape", "/absolute", "a/../../escape", "a//b", ".writer.lock", "a/.hidden", "a\\b", "a\nheader"} {
		if err := s.Put(ctx, key, nil); err == nil {
			t.Errorf("invalid key accepted: %q", key)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Get(cancelled, key); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read: %v", err)
	}
}

func TestLocalBlobContract(t *testing.T) {
	s, err := NewLocal(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	blobContract(t, s, "")
	stats := s.Stats()
	if stats.Gets == 0 || stats.RangeGets == 0 || stats.Puts == 0 || stats.DownloadedBytes == 0 || stats.UploadedBytes == 0 {
		t.Fatalf("missing counters: %+v", stats)
	}
	if err := s.Put(context.Background(), "huge", make([]byte, 1025)); err == nil {
		t.Fatal("oversized PUT accepted")
	}
	if err := s.Put(context.Background(), "tenant/attempt-1/pack", []byte("different")); !errors.Is(err, ErrKeyReuse) {
		t.Fatalf("different-byte reuse not rejected: %v", err)
	}
}

func TestLocalConcurrentKeyReuse(t *testing.T) {
	dir := t.TempDir()
	a, err := NewLocal(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewLocal(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := a
			if i%2 == 0 {
				s = b
			}
			err := s.Put(context.Background(), "same-attempt-key", []byte(fmt.Sprint(i)))
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrKeyReuse) {
				t.Errorf("Put error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("expected one content, got %d", wins.Load())
	}
}

func TestLocalSymlinkCannotEscape(t *testing.T) {
	dir, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(outside+"/secret", []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, dir+"/escape"); err != nil {
		t.Fatal(err)
	}
	s, err := NewLocal(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Get(context.Background(), "escape/secret"); err == nil {
		t.Fatal("symlink escaped object root")
	}
	if err = s.Put(context.Background(), "escape/new", []byte("data")); err == nil {
		t.Fatal("symlink write escaped object root")
	}
}

func s3ForTest(t *testing.T, endpoint string, max int64, attempts int) *S3 {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test-only")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-only-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	s, err := NewS3(S3Config{Endpoint: endpoint, Bucket: "test", MaxObjectBytes: max, MaxAttempts: attempts, Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// This HTTP backend models only the required S3 data-plane APIs. It deliberately
// rejects every conditional request and omits all ETags. It is not a simulation
// of Sakura's entire service or of real SigV4 credential authorization.
type basicS3 struct {
	mu          sync.Mutex
	objects     map[string][]byte
	conditional atomic.Int32
	badMD5      atomic.Int32
	putFailures atomic.Int32
}

func xmlError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>test backend error</Message></Error>", code)
}

func (s *basicS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for key := range r.Header {
		if strings.HasPrefix(strings.ToLower(key), "if-") {
			s.conditional.Add(1)
			xmlError(w, 501, "NotImplemented")
			return
		}
	}
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		xmlError(w, 403, "AccessDenied")
		return
	}
	// Explicit WhenRequired modes must avoid optional checksum/trailer APIs.
	if r.Header.Get("X-Amz-Sdk-Checksum-Algorithm") != "" || r.Header.Get("X-Amz-Checksum-Mode") != "" || r.Header.Get("X-Amz-Trailer") != "" || strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		xmlError(w, 501, "UnexpectedChecksumNegotiation")
		return
	}
	switch r.Method {
	case http.MethodPut:
		b, err := io.ReadAll(io.LimitReader(r.Body, 1025))
		if err != nil || len(b) > 1024 {
			xmlError(w, 400, "InvalidRequest")
			return
		}
		digest := md5.Sum(b)
		if r.Header.Get("Content-MD5") != base64.StdEncoding.EncodeToString(digest[:]) {
			s.badMD5.Add(1)
			xmlError(w, 400, "BadDigest")
			return
		}
		if s.putFailures.Load() > 0 {
			s.putFailures.Add(-1)
			xmlError(w, 503, "SlowDown")
			return
		}
		s.mu.Lock()
		s.objects[r.URL.Path] = append([]byte(nil), b...)
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK) // deliberately no ETag
	case http.MethodGet:
		s.mu.Lock()
		b, ok := s.objects[r.URL.Path]
		b = append([]byte(nil), b...)
		s.mu.Unlock()
		if !ok {
			xmlError(w, 404, "NoSuchKey")
			return
		}
		if r.Header.Get("Range") != "" {
			var start, end int64
			n, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
			if err != nil || n != 2 || start < 0 || end < start || end >= int64(len(b)) {
				xmlError(w, 416, "InvalidRange")
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(b)))
			w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(b[start : end+1])
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	default:
		xmlError(w, 405, "MethodNotAllowed")
	}
}

func TestS3OrdinaryPutNoETagOrConditionalRequests(t *testing.T) {
	backend := &basicS3{objects: map[string][]byte{}}
	srv := httptest.NewServer(backend)
	defer srv.Close()
	s := s3ForTest(t, srv.URL, 1024, 1)
	opts := s.client.Options()
	if opts.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired || opts.ResponseChecksumValidation != aws.ResponseChecksumValidationWhenRequired {
		t.Fatal("checksum modes not explicitly WhenRequired")
	}
	blobContract(t, s, "")
	if backend.conditional.Load() != 0 || backend.badMD5.Load() != 0 {
		t.Fatalf("unexpected headers/checksum failures: %d/%d", backend.conditional.Load(), backend.badMD5.Load())
	}
	if s.Stats().Puts != 3 {
		t.Fatalf("unexpected upload attempts %+v", s.Stats())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestS3CorruptUploadRejectedByContentMD5(t *testing.T) {
	backend := &basicS3{objects: map[string][]byte{}}
	srv := httptest.NewServer(backend)
	defer srv.Close()
	s := s3ForTest(t, srv.URL, 1024, 1)
	opts := s.client.Options()
	opts.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			_ = r.Body.Close()
			body[0] ^= 1 // simulate payload corruption after Content-MD5 computation
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	s.client = s3.New(opts)
	err := s.Put(context.Background(), "attempt-unique", []byte("payload"))
	if err == nil || !strings.Contains(err.Error(), "BadDigest") {
		t.Fatalf("corrupt upload accepted: %v", err)
	}
	if backend.badMD5.Load() != 1 {
		t.Fatal("server did not validate Content-MD5")
	}
	if _, err := s.Get(context.Background(), "attempt-unique"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("corrupt payload became visible: %v", err)
	}
}

func TestS3RetriesIdenticalPutWithoutConditionals(t *testing.T) {
	backend := &basicS3{objects: map[string][]byte{}}
	backend.putFailures.Store(1)
	srv := httptest.NewServer(backend)
	defer srv.Close()
	s := s3ForTest(t, srv.URL, 1024, 2)
	data := []byte("same attempt, same bytes")
	if err := s.Put(context.Background(), "attempt-unique", data); err != nil {
		t.Fatal(err)
	}
	b, err := s.Get(context.Background(), "attempt-unique")
	if err != nil || !bytes.Equal(b, data) {
		t.Fatalf("retry changed data: %q %v", b, err)
	}
	if s.Stats().Puts != 2 || backend.conditional.Load() != 0 || backend.badMD5.Load() != 0 {
		t.Fatalf("retry counters/headers: %+v", s.Stats())
	}
}

func TestS3RangeRejectsMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name, contentRange, body string
		length                   int
	}{
		{"missing range", "", "abc", 3}, {"wrong start", "bytes 0-2/100", "abc", 3}, {"wrong total", "bytes 3-5/5", "abc", 3}, {"short range", "bytes 3-5/100", "ab", 2}, {"truncated body", "bytes 3-5/100", "ab", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") != "bytes=3-5" {
					t.Errorf("unexpected Range %q", r.Header.Get("Range"))
				}
				w.Header().Set("Content-Length", fmt.Sprint(tc.length))
				if tc.contentRange != "" {
					w.Header().Set("Content-Range", tc.contentRange)
				}
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			s := s3ForTest(t, srv.URL, 1024, 1)
			if _, err := s.GetRange(context.Background(), "pack", 3, 3); err == nil {
				t.Fatal("malformed response accepted")
			}
		})
	}
}

func TestS3RejectsOversizedGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2048")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	s := s3ForTest(t, srv.URL, 1024, 1)
	if _, err := s.Get(context.Background(), "pack"); err == nil {
		t.Fatal("oversized object accepted")
	}
}

func TestS3RetriesServerErrors(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			xmlError(w, 503, "SlowDown")
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	s := s3ForTest(t, srv.URL, 1024, 2)
	b, err := s.Get(context.Background(), "pack")
	if err != nil || string(b) != "ok" || requests.Load() != 2 {
		t.Fatalf("retry: data=%q err=%v requests=%d", b, err, requests.Load())
	}
	if s.Stats().Gets != 2 {
		t.Fatalf("HTTP retry attempts missing from counters: %+v", s.Stats())
	}
}

// This writes small, uniquely named objects into an explicitly supplied test
// bucket; it never deletes them. Configure the bucket's normal cleanup policy.
// It does not require or test conditional PUT or ETags.
func TestS3Integration(t *testing.T) {
	endpoint := os.Getenv("VMTIER_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("VMTIER_TEST_S3_ENDPOINT not set")
	}
	bucket := os.Getenv("VMTIER_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "vmtier-test"
	}
	s, err := NewS3(S3Config{Endpoint: endpoint, Bucket: bucket, Region: os.Getenv("VMTIER_TEST_S3_REGION"), MaxObjectBytes: 1024, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	blobContract(t, s, fmt.Sprintf("test-%d/", time.Now().UnixNano()))
}
