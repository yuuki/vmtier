package pack

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// fuzzLimits keep fuzzed inputs small while leaving every structural bound
// reachable.
var fuzzLimits = Limits{MaxInputBytes: 1 << 20, MaxPackBytes: 1 << 20, MaxIndexBytes: 1 << 20}

// requirePublishable asserts what the archiver relies on before it publishes a
// pack: the index it uploads parses back to the same directory, and every
// block it references decodes, clipped to the logical window, under the
// index's tenant.
func requirePublishable(t *testing.T, idx *Index, data []byte, limits Limits) {
	t.Helper()
	if int64(len(data)) != idx.Size || Sum(data) != idx.SHA256 {
		t.Fatalf("pack size/checksum disagree with index: %d/%s", len(data), idx.SHA256)
	}
	encoded, err := MarshalIndex(idx, limits)
	if err != nil {
		t.Fatalf("built index cannot be marshaled: %v", err)
	}
	parsed, err := ParseIndex(encoded, limits)
	if err != nil {
		t.Fatalf("uploaded index would not parse back: %v", err)
	}
	if !reflect.DeepEqual(idx, parsed) {
		t.Fatalf("index round trip differs:\n%+v\n%+v", idx, parsed)
	}
	for si, s := range parsed.Series {
		for bi, ref := range s.Blocks {
			mb, err := LoadBlock(parsed, si, bi, data[ref.Offset:ref.Offset+ref.Length])
			if err != nil {
				t.Fatalf("published block %d/%d does not load: %v", si, bi, err)
			}
			if mb == nil {
				continue
			}
			var name storage.MetricName
			if err := name.Unmarshal(mb.MetricName); err != nil || name.AccountID != idx.Tenant.AccountID || name.ProjectID != idx.Tenant.ProjectID {
				t.Fatalf("tenant not restored: %+v %v", name, err)
			}
			if err := mb.Block.UnmarshalData(); err != nil {
				t.Fatal(err)
			}
			ts, _ := mb.Block.AppendRowsWithTimeRangeFilter(nil, nil, storage.TimeRange{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64})
			if len(ts) == 0 || ts[0] < idx.Start || ts[len(ts)-1] >= idx.End {
				t.Fatalf("block not clipped to [%d,%d): %v", idx.Start, idx.End, ts)
			}
		}
	}
}

// blockRecord identifies one archived native record by its labels and bytes.
type blockRecord struct{ labels, sum string }

// recordsInOrder lists the blocks of packs in the order they were appended.
func recordsInOrder(t *testing.T, idx *Index) []blockRecord {
	t.Helper()
	byOffset := make(map[int64]blockRecord)
	var offsets []int64
	for _, s := range idx.Series {
		labels, err := json.Marshal(s.Labels)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range s.Blocks {
			byOffset[b.Offset] = blockRecord{string(labels), b.SHA256}
			offsets = append(offsets, b.Offset)
		}
	}
	slices.Sort(offsets)
	out := make([]blockRecord, 0, len(offsets))
	for _, o := range offsets {
		out = append(out, byOffset[o])
	}
	return out
}

// nativeSeeds returns small native exports for the logical window [0,100),
// built with the pinned VM encoder, including a label that is not valid UTF-8.
func nativeSeeds() [][]byte {
	simple, _ := nativeFixture(0, 100, map[string]string{"__name__": "x"}, []int64{0, 50}, []int64{2, 3}, 0)
	clipped, _ := nativeFixture(0, 100, map[string]string{"__name__": "y", "job": "a", "instance": "b"}, []int64{-10, 10, 99, 100, 200}, []int64{1, 2, 3, 4, 5}, -2)
	invalid, _ := nativeFixture(0, 100, map[string]string{"__name__": "z", "job": "a\xffb"}, []int64{5}, []int64{7}, 0)
	twoRecords := append(append([]byte(nil), simple...), clipped[16:]...)
	return [][]byte{simple, clipped, invalid, twoRecords}
}

// portableSeeds returns the portable blocks of the seeds and of the captured
// v1.155.0 HTTP response, read from the native framing.
func portableSeeds(tb testing.TB) [][]byte {
	tb.Helper()
	hexData, err := os.ReadFile("testdata/native-v1.155.0.hex")
	if err != nil {
		tb.Fatal(err)
	}
	captured, err := hex.DecodeString(strings.TrimSpace(string(hexData)))
	if err != nil {
		tb.Fatal(err)
	}
	var out [][]byte
	for _, native := range append(nativeSeeds(), captured) {
		src := native[16:]
		for len(src) >= 4 {
			n := int(binary.BigEndian.Uint32(src))
			if len(src) < 4+n+4 {
				break
			}
			src = src[4+n:] // metric name
			n = int(binary.BigEndian.Uint32(src))
			if len(src) < 4+n {
				break
			}
			out = append(out, src[4:4+n])
			src = src[4+n:]
		}
	}
	if len(out) == 0 {
		tb.Fatal("no portable seeds")
	}
	return out
}

// FuzzBuildPacks checks the rotating builder that the service archiver uses:
// each emitted pack is publishable, and rotation never changes which records
// are archived compared with the single-pack builder.
func FuzzBuildPacks(f *testing.F) {
	for _, seed := range nativeSeeds() {
		f.Add(seed, uint8(0))
		f.Add(seed, uint8(1))
	}
	f.Fuzz(func(t *testing.T, data []byte, maxBlocks uint8) {
		if len(data) > 1<<16 {
			return
		}
		limits := fuzzLimits
		limits.MaxBlocks = int(maxBlocks%4) + 1
		var rotated []blockRecord
		err := BuildPacks(bytes.NewReader(data), Tenant{3, 4}, 0, 100, limits, 1024, 10000000, func(idx *Index, packData []byte) error {
			if len(idx.Series) == 0 {
				t.Fatal("empty pack emitted")
			}
			requirePublishable(t, idx, packData, limits)
			rotated = append(rotated, recordsInOrder(t, idx)...)
			return nil
		})
		if err != nil {
			return
		}
		idx, packData, err := Build(bytes.NewReader(data), Tenant{3, 4}, 0, 100, fuzzLimits)
		if err != nil {
			return
		}
		requirePublishable(t, idx, packData, fuzzLimits)
		if single := recordsInOrder(t, idx); !reflect.DeepEqual(rotated, single) && (len(rotated) != 0 || len(single) != 0) {
			t.Fatalf("rotation changed the archived records:\n%v\n%v", rotated, single)
		}
	})
}

// FuzzParseIndex checks the reader's index decoder on untrusted object bytes:
// an accepted index re-encodes to an index that parses to the same directory.
func FuzzParseIndex(f *testing.F) {
	for _, seed := range nativeSeeds() {
		idx, _, err := Build(bytes.NewReader(seed), Tenant{1, 2}, 0, 100, Limits{})
		if err != nil {
			continue
		}
		if encoded, err := json.Marshal(idx); err == nil {
			f.Add(encoded)
		}
	}
	f.Add([]byte(`{"version":1}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			return
		}
		idx, err := ParseIndex(data, Limits{})
		if err != nil {
			return
		}
		encoded, err := MarshalIndex(idx, Limits{})
		if err != nil {
			t.Fatalf("accepted index cannot be re-encoded: %v", err)
		}
		again, err := ParseIndex(encoded, Limits{})
		if err != nil || !reflect.DeepEqual(idx, again) {
			t.Fatalf("index round trip differs: %v\n%+v\n%+v", err, idx, again)
		}
		if IndexMemoryEstimate(idx) <= 0 {
			t.Fatal("nonpositive index memory estimate")
		}
	})
}

// FuzzDecodePortable compares the bounded portable decoder with the pinned VM
// codec and checks that LoadBlock rebuilds wire blocks without changing rows.
func FuzzDecodePortable(f *testing.F) {
	for _, seed := range portableSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := decodePortable(data)
		if err != nil {
			return
		}
		n := len(p.ts)
		if n < 1 || n > maxRows || len(p.values) != n {
			t.Fatalf("decoded %d timestamps and %d values", n, len(p.values))
		}
		for i := 1; i < n; i++ {
			if p.ts[i] < p.ts[i-1] {
				t.Fatalf("unordered timestamps: %v", p.ts)
			}
		}
		want := decimal.AppendDecimalToFloat(nil, p.values, p.scale)

		// The pinned VM codec must read the same bytes as the same rows.
		var vm storage.Block
		tail, err := vm.UnmarshalPortable(data)
		if err != nil || len(tail) != 0 {
			t.Fatalf("pinned VM codec rejects an accepted block: %v (tail %d)", err, len(tail))
		}
		all := storage.TimeRange{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}
		vmTS, vmVS := vm.AppendRowsWithTimeRangeFilter(nil, nil, all)
		requireRows(t, "pinned VM codec", vmTS, vmVS, p.ts, want)

		// LoadBlock re-encodes decoded rows into vmselect wire blocks.
		if p.ts[n-1] == math.MaxInt64 {
			return // The half-open logical window cannot include MaxInt64.
		}
		mn := MetricName(Tenant{}, map[string]string{"__name__": "fuzz"})
		idx := &Index{Version: FormatVersion, VMVersion: VMVersion, VMRevision: VMRevision, Start: p.ts[0], End: p.ts[n-1] + 1, Series: []Series{{
			Labels:     map[string]string{"__name__": "fuzz"},
			NativeName: mn.MarshalNoAccountIDProjectID(nil),
			Blocks:     []BlockRef{{Length: int64(len(data)), MinTimestamp: p.ts[0], MaxTimestamp: p.ts[n-1], Rows: n, SHA256: Sum(data)}},
		}}}
		mb, err := LoadBlock(idx, 0, 0, data)
		if err != nil || mb == nil {
			t.Fatalf("LoadBlock rejects a decoded block: %v", err)
		}
		if err := mb.Block.UnmarshalData(); err != nil {
			t.Fatal(err)
		}
		wireTS, wireVS := mb.Block.AppendRowsWithTimeRangeFilter(nil, nil, all)
		requireRows(t, "wire block", wireTS, wireVS, p.ts, want)
	})
}

func requireRows(t *testing.T, what string, gotTS []int64, gotVS []float64, wantTS []int64, wantVS []float64) {
	t.Helper()
	if !reflect.DeepEqual(gotTS, wantTS) || len(gotVS) != len(wantVS) {
		t.Fatalf("%s timestamps differ: %v vs %v", what, gotTS, wantTS)
	}
	for i := range wantVS {
		if math.Float64bits(gotVS[i]) != math.Float64bits(wantVS[i]) {
			t.Fatalf("%s value %d differs: %v vs %v", what, i, gotVS[i], wantVS[i])
		}
	}
}

// FuzzParseTenant checks that tenant parsing and formatting agree, because
// object keys and catalog streams use the formatted tenant.
func FuzzParseTenant(f *testing.F) {
	for _, s := range []string{"0", "1:2", "42:7", "4294967295:4294967295", "01:2", "1:", ":1", "1:2:3", "-1"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		tenant, err := ParseTenant(s)
		if err != nil {
			return
		}
		canonical := tenant.String()
		again, err := ParseTenant(canonical)
		if err != nil || again != tenant || again.String() != canonical {
			t.Fatalf("tenant %q formats as %q, which parses as %+v (%v)", s, canonical, again, err)
		}
		if strings.ContainsAny(canonical, "/\\.") {
			t.Fatalf("formatted tenant %q could change an object key path", canonical)
		}
	})
}

// A series whose labels are not valid UTF-8 cannot round-trip through the JSON
// index, so neither builder may produce a pack for it.
func TestBuildersRejectLabelsThatJSONCannotRepresent(t *testing.T) {
	for _, labels := range []map[string]string{
		{"__name__": "x", "job": "a\xffb"},
		{"__name__": "x\xc3", "job": "a"},
		{"__name__": "x", "k\xfe": "v"},
	} {
		native, _ := nativeFixture(0, 100, labels, []int64{5}, []int64{7}, 0)
		if _, _, err := Build(bytes.NewReader(native), Tenant{}, 0, 100, Limits{}); err == nil || !strings.Contains(err.Error(), "UTF-8") {
			t.Fatalf("Build accepted %q: %v", labels, err)
		}
		err := BuildPacks(bytes.NewReader(native), Tenant{}, 0, 100, Limits{}, 4, 1000, func(*Index, []byte) error {
			t.Fatalf("BuildPacks emitted a pack for %q", labels)
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "UTF-8") {
			t.Fatalf("BuildPacks accepted %q: %v", labels, err)
		}
	}
}
