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
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type S3Config struct {
	Endpoint       string
	Bucket         string
	Region         string
	Prefix         string
	MaxObjectBytes int64
	Timeout        time.Duration
	MaxAttempts    int
}

// S3 uses ordinary PutObject, GetObject and single-range GetObject only.
// It does not send conditional headers or require ETags. Catalog publication is
// handled by a separate authority, never by this data-plane client.
type S3 struct {
	client         *s3.Client
	bucket, prefix string
	max            int64
	timeout        time.Duration
	stats          counters
}

// Count HTTP attempts, including SDK retries, and bytes actually consumed by
// the HTTP transport/SDK. Error response bodies are included in download bytes.
type countedTransport struct {
	base  http.RoundTripper
	stats *counters
}
type countedBody struct {
	io.ReadCloser
	count *atomic.Uint64
}

func (b countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.count.Add(uint64(n))
	return n, err
}
func (t countedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	switch r.Method {
	case http.MethodGet:
		if r.Header.Get("Range") != "" {
			t.stats.ranges.Add(1)
		} else {
			t.stats.gets.Add(1)
		}
	case http.MethodPut:
		t.stats.puts.Add(1)
	}
	if r.Body != nil {
		r.Body = countedBody{r.Body, &t.stats.uploaded}
	}
	resp, err := t.base.RoundTrip(r)
	if resp != nil && resp.Body != nil {
		resp.Body = countedBody{resp.Body, &t.stats.downloaded}
	}
	return resp, err
}

func NewS3(c S3Config) (*S3, error) {
	if c.MaxObjectBytes == 0 {
		c.MaxObjectBytes = DefaultMaxObjectBytes
	}
	if c.MaxObjectBytes < 1 || c.MaxObjectBytes > 1<<30 {
		return nil, fmt.Errorf("max object bytes must be in [1,1GiB]")
	}
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Second
	}
	if c.Timeout < 0 {
		return nil, fmt.Errorf("timeout must be positive")
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 10 {
		return nil, fmt.Errorf("max attempts must be in [1,10]")
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.Bucket == "" || strings.ContainsAny(c.Bucket, "/\\\r\n") {
		return nil, fmt.Errorf("invalid S3 bucket")
	}
	if c.Prefix != "" {
		c.Prefix = strings.TrimSuffix(c.Prefix, "/")
		if err := validKey(c.Prefix); err != nil {
			return nil, fmt.Errorf("invalid S3 prefix: %w", err)
		}
		c.Prefix += "/"
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("invalid S3 endpoint")
		}
	}
	access, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if access == "" || secret == "" {
		return nil, fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
	}
	store := &S3{bucket: c.Bucket, prefix: c.Prefix, max: c.MaxObjectBytes, timeout: c.Timeout}
	opts := s3.Options{
		Region:                     c.Region,
		Credentials:                credentials.NewStaticCredentialsProvider(access, secret, os.Getenv("AWS_SESSION_TOKEN")),
		HTTPClient:                 &http.Client{Timeout: c.Timeout, Transport: countedTransport{http.DefaultTransport, &store.stats}},
		RetryMaxAttempts:           c.MaxAttempts,
		UsePathStyle:               true,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	if c.Endpoint != "" {
		opts.BaseEndpoint = aws.String(c.Endpoint)
	}
	store.client = s3.New(opts)
	return store, nil
}

func (s *S3) Stats() Snapshot { return s.stats.snapshot() }

func s3Error(err error) error {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return fmt.Errorf("%w: %s", ErrNotFound, api.ErrorCode())
		}
	}
	return err
}

func (s *S3) get(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := validKey(key); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	in := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + key)}
	max := s.max
	if length > 0 {
		if err := checkRange(offset, length, s.max); err != nil {
			return nil, err
		}
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		max = length
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		return nil, s3Error(err)
	}
	defer out.Body.Close()
	if out.ContentLength == nil || *out.ContentLength < 0 || *out.ContentLength > max {
		return nil, fmt.Errorf("invalid or oversized S3 Content-Length")
	}
	if length > 0 {
		if *out.ContentLength != length {
			return nil, fmt.Errorf("truncated S3 range")
		}
		var first, last, total int64
		if out.ContentRange == nil {
			return nil, fmt.Errorf("S3 response lacks Content-Range")
		}
		n, err := fmt.Sscanf(*out.ContentRange, "bytes %d-%d/%d", &first, &last, &total)
		if err != nil || n != 3 || first != offset || last != offset+length-1 || total <= last || *out.ContentRange != fmt.Sprintf("bytes %d-%d/%d", first, last, total) {
			return nil, fmt.Errorf("invalid S3 Content-Range")
		}
	}
	b, err := readBounded(out.Body, max)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != *out.ContentLength {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	return s.get(ctx, key, 0, 0)
}
func (s *S3) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := checkRange(offset, length, s.max); err != nil {
		return nil, err
	}
	return s.get(ctx, key, offset, length)
}

// Put uploads under an attempt-unique key. The caller may retry identical bytes
// after an ambiguous response, but must never reuse the key for other contents.
// Content-MD5 is an upload-integrity check, not the durable content identity;
// archiver readback and Cold reads independently verify SHA-256.
func (s *S3) Put(ctx context.Context, key string, data []byte) error {
	if err := validKey(key); err != nil {
		return err
	}
	if int64(len(data)) > s.max {
		return fmt.Errorf("object exceeds %d byte limit", s.max)
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	digest := md5.Sum(data) // S3 Content-MD5 compatibility; SHA-256 remains authoritative.
	in := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + key), Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))), ContentMD5: aws.String(base64.StdEncoding.EncodeToString(digest[:]))}
	_, err := s.client.PutObject(ctx, in)
	if err != nil {
		return s3Error(err)
	}
	return nil
}
