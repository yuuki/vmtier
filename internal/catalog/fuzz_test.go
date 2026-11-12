package catalog

import (
	"bytes"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yuuki/vmtier/internal/pack"
)

// FuzzDecodeManifest checks the JSON v2 catalog decoder on untrusted authority
// responses: an accepted manifest re-encodes to the same canonical bytes.
func FuzzDecodeManifest(f *testing.F) {
	for _, m := range []*Manifest{validManifest(), Fresh("fresh")} {
		b, err := marshal(m)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte(`{"version":2,"vm_version":"v1.155.0-cluster","id":"x","generation":0,"contract":{},"entries":null}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := decode(data)
		if err != nil {
			return
		}
		if err := Validate(m); err != nil {
			t.Fatalf("decoded manifest fails validation: %v", err)
		}
		canonical, err := marshal(m)
		if err != nil {
			t.Fatalf("decoded manifest cannot be re-encoded: %v", err)
		}
		again, err := decode(canonical)
		if err != nil {
			t.Fatalf("canonical manifest does not decode: %v", err)
		}
		if b, err := marshal(again); err != nil || !bytes.Equal(b, canonical) {
			t.Fatalf("canonical encoding is not a fixed point: %v\n%s\n%s", err, canonical, b)
		}
		if again.ID != m.ID || again.Generation != m.Generation || !EqualContract(again.Contract, m.Contract) || len(again.Entries) != len(m.Entries) {
			t.Fatalf("round trip changed the manifest: %+v vs %+v", m, again)
		}
		for i := range m.Entries {
			if again.Entries[i] != m.Entries[i] {
				t.Fatalf("round trip changed entry %d", i)
			}
		}
	})
}

// FuzzObjectKeyConfinement checks the key rule that publication and catalog
// validation apply: an accepted key stays inside its tenant namespace when
// joined to any root, and no key is accepted for two different tenants.
func FuzzObjectKeyConfinement(f *testing.F) {
	for _, seed := range []struct{ a, b, key string }{
		{"1:2", "2:3", "tenants/1:2/attempts/00/aa.pack"},
		{"1:2", "1:23", "tenants/1:2/../1:23/x"},
		{"1:2", "1:2", "tenants/1:2/./x"},
		{"7:3", "7:30", "tenants/7:3//x"},
		{"0", "0:0", "tenants/0/.hidden/x"},
	} {
		f.Add(seed.a, seed.b, seed.key)
	}
	f.Fuzz(func(t *testing.T, tenantA, tenantB, key string) {
		prefix := "tenants/" + tenantA + "/"
		if !validObjectKey(key, prefix) {
			return
		}
		if !filepath.IsLocal(key) {
			t.Fatalf("accepted key %q is not local to the blob root", key)
		}
		for _, root := range []string{"/srv/blobs", "bucket-prefix"} {
			if joined := path.Join(root, key); !strings.HasPrefix(joined, path.Join(root, prefix)+"/") {
				t.Fatalf("accepted key %q escapes %q under %q: %q", key, prefix, root, joined)
			}
		}
		for _, part := range strings.Split(key, "/") {
			if part == "" || part == "." || part == ".." {
				t.Fatalf("accepted key %q has component %q", key, part)
			}
		}
		// Contract tenants are canonical account:project strings.
		a, errA := pack.ParseTenant(tenantA)
		b, errB := pack.ParseTenant(tenantB)
		if errA == nil && errB == nil && a.String() == tenantA && b.String() == tenantB && tenantA != tenantB && validObjectKey(key, "tenants/"+tenantB+"/") {
			t.Fatalf("key %q is accepted for tenants %q and %q", key, tenantA, tenantB)
		}
	})
}

// FuzzLedgerKeys checks that the object ledger only accepts, and garbage
// collection only claims, keys confined to a configured tenant's prefix: a
// recorded key can never direct a deletion outside the archive namespace.
func FuzzLedgerKeys(f *testing.F) {
	for _, seed := range []struct{ tenant, a, b string }{
		{"1:2", "tenants/1:2/attempts/00/aa.pack", "tenants/1:2/attempts/00/aa.json"},
		{"1:2", "tenants/1:2/attempts/00/aa.pack", "tenants/1:2/attempts/00/aa.pack"},
		{"1:2", "tenants/1:23/x", "tenants/1:2/../1:3/x"},
		{"3:4", "tenants/3:4/x", "tenants/3:4/.x"},
	} {
		f.Add(seed.tenant, seed.a, seed.b)
	}
	c := contract()
	c.Tenants = append(c.Tenants, "3:4")
	f.Fuzz(func(t *testing.T, tenant, a, b string) {
		if validatePendingKeys(c, tenant, []string{a, b}) == nil {
			if a == b || (tenant != "1:2" && tenant != "3:4") {
				t.Fatalf("recorded %q, %q for tenant %q", a, b, tenant)
			}
			for _, key := range []string{a, b} {
				if !validObjectKey(key, "tenants/"+tenant+"/") || checkClaimedKey(c, key) != nil {
					t.Fatalf("recorded key %q escapes tenant %q", key, tenant)
				}
			}
		}
		for _, key := range []string{a, b} {
			if checkClaimedKey(c, key) == nil && !validObjectKey(key, "tenants/1:2/") && !validObjectKey(key, "tenants/3:4/") {
				t.Fatalf("claimed key %q is outside the configured tenants", key)
			}
		}
		if keys, err := validateForget([]string{a, b}); err == nil {
			for _, key := range keys {
				if !filepath.IsLocal(key) || !strings.HasPrefix(key, "tenants/") {
					t.Fatalf("forgot key %q outside the archive namespace", key)
				}
			}
		}
	})
}

// FuzzNormalizeBlobLocator checks that normalization is idempotent. Services
// compare the normalized configured locator with the one pinned at catalog
// creation, so a second normalization must not change it.
func FuzzNormalizeBlobLocator(f *testing.F) {
	f.Add("s3", "", "https://s3.example.com/", "", "bucket", "archive/vmtier/")
	f.Add("s3", "", "", "eu-1", "bucket", "")
	f.Add("s3", "", "http://127.0.0.1:9000", "", "b", "a//b")
	f.Add("local", "relative/blobs", "", "", "", "")
	f.Add("local", "/abs/../blobs/", "", "", "", "")
	f.Fuzz(func(t *testing.T, typ, p, endpoint, region, bucket, prefix string) {
		once, err := NormalizeBlobLocator(BlobLocator{Type: typ, Path: p, Endpoint: endpoint, Region: region, Bucket: bucket, Prefix: prefix})
		if err != nil {
			return
		}
		twice, err := NormalizeBlobLocator(once)
		if err != nil || twice != once {
			t.Fatalf("normalization is not idempotent: %+v -> %+v (%v)", once, twice, err)
		}
		if once.Type == "s3" && once.Prefix != "" {
			if err := validateS3Prefix(once.Prefix); err != nil || strings.HasSuffix(once.Prefix, "/") {
				t.Fatalf("normalized prefix %q is not a clean key prefix: %v", once.Prefix, err)
			}
		}
		if once.Type == "local" && !filepath.IsAbs(once.Path) {
			t.Fatalf("normalized local path %q is relative", once.Path)
		}
	})
}
