package archive

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/objectstore"
	"github.com/yuuki/vmtier/internal/pack"
)

// Both publication backends use the same independent completeness checks.
func verifiedExport(ctx context.Context, cfg Config, source Source, tenant pack.Tenant, start, end int64) (*pack.Index, []byte, uint64, error) {
	idx, data, err := export(ctx, cfg, source, tenant, start, end)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("export tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	samples, err := extractSamples(idx, data, cfg.MaxSamples)
	if err != nil {
		return nil, nil, 0, err
	}
	counts := countsOf(samples)
	if err = verifyRaw(ctx, cfg, source, tenant, start, end, samples); err != nil {
		return nil, nil, 0, fmt.Errorf("raw verification tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	if err = verifyCounts(ctx, cfg, source, tenant, start, end, counts); err != nil {
		return nil, nil, 0, fmt.Errorf("verify tenant %s source %s: %w", tenant.String(), source.ID, err)
	}
	if err = hook(cfg, "exported"); err != nil {
		return nil, nil, 0, err
	}
	var total uint64
	for _, count := range counts {
		total += uint64(count.normal + count.stale)
	}
	return idx, data, total, nil
}

// A fresh attempt namespace prevents a retry from modifying published objects.
func uploadPack(ctx context.Context, blobs objectstore.BlobStore, cfg Config, idx *pack.Index, data []byte) (catalog.PackRef, error) {
	var ref catalog.PackRef
	indexData, err := pack.MarshalIndex(idx, cfg.PackLimits)
	if err != nil {
		return ref, err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return ref, fmt.Errorf("allocate unique archive attempt: %w", err)
	}
	prefix := fmt.Sprintf("tenants/%s/attempts/%s/%s", idx.Tenant.String(), hex.EncodeToString(nonce[:]), pack.Sum(indexData))
	ref = catalog.PackRef{
		DataKey: prefix + ".pack", IndexKey: prefix + ".json",
		DataSHA256: pack.Sum(data), IndexSHA256: pack.Sum(indexData),
		DataBytes: int64(len(data)), IndexBytes: int64(len(indexData)),
	}
	if err = blobs.Put(ctx, ref.DataKey, data); err != nil {
		return ref, fmt.Errorf("upload payload: %w", err)
	}
	if err = blobs.Put(ctx, ref.IndexKey, indexData); err != nil {
		return ref, fmt.Errorf("upload index: %w", err)
	}
	if err = hook(cfg, "uploaded"); err != nil {
		return ref, err
	}
	for _, item := range []struct {
		key  string
		data []byte
	}{{ref.DataKey, data}, {ref.IndexKey, indexData}} {
		got, err := blobs.Get(ctx, item.key)
		if err != nil {
			return ref, fmt.Errorf("verify uploaded object: %w", err)
		}
		if !bytes.Equal(got, item.data) {
			return ref, errors.New("uploaded object checksum or length mismatch")
		}
	}
	return ref, nil
}
