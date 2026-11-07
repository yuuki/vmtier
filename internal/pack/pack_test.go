package pack

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/klauspost/compress/zstd"
)

func nativeFixture(start, end int64, labels map[string]string, ts, values []int64, scale int16) ([]byte, []byte) {
	mn := MetricName(Tenant{99, 88}, labels)
	name := mn.MarshalNoAccountIDProjectID(nil)
	var b storage.Block
	b.Init(&storage.TSID{MetricID: 12345}, ts, values, scale, 64)
	payload := b.MarshalPortable(nil)
	out := encoding.MarshalInt64(nil, start)
	out = encoding.MarshalInt64(out, end-1)
	out = encoding.MarshalUint32(out, uint32(len(name)))
	out = append(out, name...)
	out = encoding.MarshalUint32(out, uint32(len(payload)))
	out = append(out, payload...)
	return out, payload
}

func TestNativeRoundTripClipsWindowAndPreservesDecimal(t *testing.T) {
	ts := []int64{100, 200, 300, 400, 500}
	values := []int64{9007199254740991, 9007199254740992, 9007199254740993, -9007199254740993, 7}
	labels := map[string]string{"__name__": "counter", "z": "last", "a": "first"}
	native, payload := nativeFixture(200, 401, labels, ts, values, -3)
	idx, data, err := Build(bytes.NewReader(native), Tenant{7, 8}, 200, 401, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("opaque portable payload was changed")
	}
	ref := idx.Series[0].Blocks[0]
	if ref.MinTimestamp != 100 || ref.MaxTimestamp != 500 || ref.Rows != 5 {
		t.Fatalf("physical extent lost: %+v", ref)
	}
	mb, err := LoadBlock(idx, 0, 0, data)
	if err != nil {
		t.Fatal(err)
	}
	var name storage.MetricName
	if err := name.Unmarshal(mb.MetricName); err != nil {
		t.Fatal(err)
	}
	if name.AccountID != 7 || name.ProjectID != 8 {
		t.Fatal("tenant not reattached")
	}
	if string(name.Tags[0].Key) != "a" || string(name.Tags[1].Key) != "z" {
		t.Fatal("noncanonical labels")
	}
	// Round trip over vmstorage wire representation, not only our portable format.
	wire := mb.Marshal(nil)
	var received storage.MetricBlock
	if tail, err := received.Unmarshal(wire); err != nil || len(tail) != 0 {
		t.Fatalf("wire: %v tail=%x", err, tail)
	}
	if err := received.Block.UnmarshalData(); err != nil {
		t.Fatal(err)
	}
	p, err := decodePortable(received.Block.MarshalPortable(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.ts, ts[1:4]) || !reflect.DeepEqual(p.values, values[1:4]) || p.scale != -3 {
		t.Fatalf("lossy conversion: %+v", p)
	}
	encoded, err := MarshalIndex(idx, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseIndex(encoded, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(idx, parsed) {
		t.Fatal("index round trip differs")
	}
}

func TestSpecialValuesAndDuplicateTimestamps(t *testing.T) {
	ts := []int64{10, 20, 20, 30, 40}
	vs := []float64{math.Inf(-1), math.Inf(1), decimal.StaleNaN, 1.23456789012345, -0.0000000012345}
	ints, scale := decimal.AppendFloatToDecimal(nil, vs)
	native, _ := nativeFixture(10, 41, map[string]string{"__name__": "special"}, ts, ints, scale)
	idx, data, err := Build(bytes.NewReader(native), Tenant{}, 10, 41, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	mb, err := LoadBlock(idx, 0, 0, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := mb.Block.UnmarshalData(); err != nil {
		t.Fatal(err)
	}
	gotTS, gotVS := mb.Block.AppendRowsWithTimeRangeFilter(nil, nil, storage.TimeRange{MinTimestamp: 10, MaxTimestamp: 40})
	if !reflect.DeepEqual(gotTS, ts) {
		t.Fatalf("timestamp multiplicity lost: %v", gotTS)
	}
	want := decimal.AppendDecimalToFloat(nil, ints, scale)
	for i := range want {
		if math.Float64bits(gotVS[i]) != math.Float64bits(want[i]) {
			t.Fatalf("value %d changed: %v vs %v", i, gotVS[i], want[i])
		}
	}
}

func TestAdjacentWindowsKeepOnlyOwnedSamples(t *testing.T) {
	labels := map[string]string{"__name__": "x"}
	ts := []int64{99, 100, 199, 200, 299, 300}
	values := []int64{1, 2, 3, 4, 5, 6}
	for _, win := range [][2]int64{{100, 200}, {200, 300}} {
		native, _ := nativeFixture(win[0], win[1], labels, ts, values, 0)
		idx, data, err := Build(bytes.NewReader(native), Tenant{}, win[0], win[1], Limits{})
		if err != nil {
			t.Fatal(err)
		}
		mb, err := LoadBlock(idx, 0, 0, data)
		if err != nil {
			t.Fatal(err)
		}
		if err := mb.Block.UnmarshalData(); err != nil {
			t.Fatal(err)
		}
		got, _ := mb.Block.AppendRowsWithTimeRangeFilter(nil, nil, storage.TimeRange{MinTimestamp: 0, MaxTimestamp: 1000})
		if len(got) != 2 || got[0] != win[0] || got[1] != win[1]-1 {
			t.Fatalf("wrong logical rows: %v", got)
		}
	}
}

func TestTruncationAndHeaderMismatch(t *testing.T) {
	native, _ := nativeFixture(0, 100, map[string]string{"__name__": "x"}, []int64{0, 50, 99}, []int64{2, 3, 4}, 0)
	for n := 0; n < len(native); n++ {
		// The format explicitly allows an empty stream after the time header.
		// Archive completeness cannot be established from framing alone.
		if n == 16 {
			continue
		}
		if _, _, err := Build(bytes.NewReader(native[:n]), Tenant{}, 0, 100, Limits{}); err == nil {
			t.Fatalf("truncation at %d accepted", n)
		}
	}
	if _, _, err := Build(bytes.NewReader(native), Tenant{}, 0, 101, Limits{}); err == nil {
		t.Fatal("header mismatch accepted")
	}
	if _, _, err := Build(bytes.NewReader(native), Tenant{}, 100, 100, Limits{}); err == nil {
		t.Fatal("empty interval accepted")
	}
}

func TestResourceLimitsAndMalformedData(t *testing.T) {
	native, _ := nativeFixture(0, 100, map[string]string{"__name__": "x"}, []int64{0, 50, 99}, []int64{2, 3, 4}, 0)
	for _, l := range []Limits{{MaxPackBytes: 1}, {MaxIndexBytes: 1}, {MaxInputBytes: 16}, {MaxBlockBytes: 1}, {MaxMetricNameBytes: 1}, {MaxSeries: -1}} {
		if _, _, err := Build(bytes.NewReader(native), Tenant{}, 0, 100, l); err == nil {
			t.Fatalf("limit accepted: %+v", l)
		}
	}
	idx, data, err := Build(bytes.NewReader(native), Tenant{}, 0, 100, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := LoadBlock(idx, 0, 0, corrupt); err == nil {
		t.Fatal("corruption accepted")
	}
	if _, err := LoadBlock(idx, 0, 0, data[:len(data)-1]); err == nil {
		t.Fatal("truncated range accepted")
	}
	if _, err := LoadBlock(idx, 1, 0, data); err == nil {
		t.Fatal("invalid series index accepted")
	}
	idx.Series[0].Blocks[0].Offset = math.MaxInt64
	if err := ValidateIndex(idx, Limits{}); err == nil {
		t.Fatal("overflow offset accepted")
	}
}

func TestCodecAndIndexValidation(t *testing.T) {
	native, _ := nativeFixture(0, 100, map[string]string{"__name__": "x"}, []int64{0}, []int64{2}, 0)
	idx, _, err := Build(bytes.NewReader(native), Tenant{}, 0, 100, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	idx.VMVersion = "v999"
	if err := ValidateIndex(idx, Limits{}); err == nil {
		t.Fatal("unknown codec accepted")
	}
	idx.VMVersion = VMVersion
	data, _ := json.Marshal(idx)
	if _, err := ParseIndex(append(data, []byte(" {}")...), Limits{}); err == nil {
		t.Fatal("trailing index accepted")
	}
	if _, err := ParseIndex(data, Limits{MaxIndexBytes: 1}); err == nil {
		t.Fatal("large index accepted")
	}
	idx.Series = append(idx.Series, idx.Series[0])
	if err := ValidateIndex(idx, Limits{}); err == nil {
		t.Fatal("duplicate dictionary accepted")
	}
}

func TestBoundedZstdAndHugeRows(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	bomb := enc.EncodeAll(make([]byte, 2<<20), nil)
	if _, _, err := expand(bomb, encoding.MarshalTypeZSTDNearestDelta); err == nil {
		t.Fatal("oversize expansion accepted")
	}
	var b []byte
	b = encoding.MarshalVarInt64(b, 0)
	b = encoding.MarshalVarInt64(b, 0)
	b = encoding.MarshalVarInt64(b, 0)
	b = encoding.MarshalVarUint64(b, math.MaxUint32)
	if _, err := decodePortable(b); err == nil {
		t.Fatal("oversize row count accepted")
	}
}

func TestEmptyPack(t *testing.T) {
	data := encoding.MarshalInt64(nil, 0)
	data = encoding.MarshalInt64(data, 99)
	idx, payload, err := Build(bytes.NewReader(data), Tenant{}, 0, 100, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Series) != 0 || len(payload) != 0 || idx.Size != 0 {
		t.Fatal("invalid empty pack")
	}
	if _, err := MarshalIndex(idx, Limits{}); err != nil {
		t.Fatal(err)
	}
}

func TestTenantParsing(t *testing.T) {
	for _, s := range []string{"0", "7:9", "4294967295:4294967295"} {
		tt, e := ParseTenant(s)
		if e != nil {
			t.Fatal(e)
		}
		if _, e := ParseTenant(tt.String()); e != nil {
			t.Fatal(e)
		}
	}
	for _, s := range []string{"", "-1", "1:2:3", "4294967296", "1:x"} {
		if _, e := ParseTenant(s); e == nil {
			t.Fatalf("invalid tenant %q accepted", s)
		}
	}
}

func TestGoldenNativeFixture(t *testing.T) {
	hexData, err := os.ReadFile("testdata/native-v1.128.0.hex")
	if err != nil {
		t.Fatal(err)
	}
	data, err := hex.DecodeString(strings.TrimSpace(string(hexData)))
	if err != nil {
		t.Fatal(err)
	}
	generated, _ := nativeFixture(0, 100, map[string]string{"__name__": "x"}, []int64{0, 50}, []int64{2, 2}, 0)
	if !bytes.Equal(data, generated) {
		t.Fatalf("pinned codec changed: got %x", generated)
	}
	idx, payload, err := Build(bytes.NewReader(data), Tenant{42, 7}, 0, 100, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBlock(idx, 0, 0, payload); err != nil {
		t.Fatal(err)
	}
}

func TestNativeNameOrderPreserved(t *testing.T) {
	// job/instance precede lexically earlier labels in stock VM storage names.
	mn := storage.MetricName{MetricGroup: []byte("x")}
	mn.AddTag("job", "api")
	mn.AddTag("instance", "one")
	mn.AddTag("aaa", "first")
	mn.AddTag("z", "last")
	name := mn.MarshalNoAccountIDProjectID(nil)
	_, payload := nativeFixture(0, 100, map[string]string{"__name__": "x"}, []int64{0}, []int64{2}, 0)
	native := encoding.MarshalInt64(nil, 0)
	native = encoding.MarshalInt64(native, 99)
	native = encoding.MarshalUint32(native, uint32(len(name)))
	native = append(native, name...)
	native = encoding.MarshalUint32(native, uint32(len(payload)))
	native = append(native, payload...)
	idx, data, err := Build(bytes.NewReader(native), Tenant{7, 8}, 0, 100, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	mb, err := LoadBlock(idx, 0, 0, data)
	if err != nil {
		t.Fatal(err)
	}
	mn.AccountID = 7
	mn.ProjectID = 8
	if !bytes.Equal(mb.MetricName, mn.Marshal(nil)) {
		t.Fatal("source metric name order was changed")
	}
	idx.Series[0].Labels["job"] = "tampered"
	if err := ValidateIndex(idx, Limits{}); err == nil {
		t.Fatal("name/dictionary mismatch accepted")
	}
}

func TestPortableLengthOverflow(t *testing.T) {
	data := encoding.MarshalVarUint64(nil, math.MaxUint64)
	if _, n := boundedBytes(data); n != 0 {
		t.Fatal("overflowing length accepted")
	}
	data = encoding.MarshalVarUint64(nil, math.MaxUint64-1)
	if _, n := boundedBytes(data); n != 0 {
		t.Fatal("overflowing length accepted")
	}
}

func TestMalformedDelta2SingleRowDoesNotPanic(t *testing.T) {
	b := encoding.MarshalVarInt64(nil, 0)
	b = encoding.MarshalVarInt64(b, 0)
	b = encoding.MarshalVarInt64(b, 2)
	b = encoding.MarshalVarUint64(b, 1)
	b = encoding.MarshalVarInt64(b, 0)
	b = append(b, byte(encoding.MarshalTypeNearestDelta2), byte(encoding.MarshalTypeConst), 64, 0, 0)
	if _, err := decodePortable(b); err == nil {
		t.Fatal("invalid single-row delta2 accepted")
	}
}

func TestCompressedPortableRoundTrip(t *testing.T) {
	ts := make([]int64, 8192)
	vs := make([]int64, len(ts))
	for i := range ts {
		if i > 0 {
			ts[i] = ts[i-1] + int64(1+i%17)
		}
		vs[i] = int64((i%128)*17 + i%3)
	}
	native, payload := nativeFixture(0, ts[len(ts)-1]+1, map[string]string{"__name__": "compressed"}, ts, vs, -4)
	src := payload
	for i := 0; i < 5; i++ {
		_, n := encoding.UnmarshalVarUint64(src)
		if n <= 0 {
			t.Fatal("bad generated header")
		}
		src = src[n:]
	}
	if src[0] != byte(encoding.MarshalTypeZSTDNearestDelta2) && src[0] != byte(encoding.MarshalTypeZSTDNearestDelta) && src[1] != byte(encoding.MarshalTypeZSTDNearestDelta2) && src[1] != byte(encoding.MarshalTypeZSTDNearestDelta) {
		t.Fatal("fixture did not exercise zstd")
	}
	idx, data, err := Build(bytes.NewReader(native), Tenant{}, 0, ts[len(ts)-1]+1, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	mb, err := LoadBlock(idx, 0, 0, data)
	if err != nil {
		t.Fatal(err)
	}
	p, err := decodePortable(mb.Block.MarshalPortable(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.ts, ts) || !reflect.DeepEqual(p.values, vs) || p.scale != -4 {
		t.Fatal("compressed codec changed samples")
	}
}

func FuzzNativeParser(f *testing.F) {
	native, _ := nativeFixture(0, 100, map[string]string{"__name__": "x"}, []int64{0, 50}, []int64{2, 3}, 0)
	f.Add(native)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			return
		}
		_, _, _ = Build(bytes.NewReader(data), Tenant{}, 0, 100, Limits{MaxInputBytes: 1 << 20, MaxPackBytes: 1 << 20, MaxIndexBytes: 1 << 20})
	})
}

func TestPreviousReleasePackRejected(t *testing.T) {
	native, _ := nativeFixture(0, 100, map[string]string{"__name__": "x"}, []int64{0}, []int64{2}, 0)
	idx, payload, err := Build(bytes.NewReader(native), Tenant{}, 0, 100, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ version, revision string }{
		{"v1.128.0-cluster", "9a009c491da4c17531893ab4acb6a5f1b5e0eaf2"},
		{VMVersion, "9a009c491da4c17531893ab4acb6a5f1b5e0eaf2"},
	} {
		idx.VMVersion, idx.VMRevision = tc.version, tc.revision
		if err := ValidateIndex(idx, Limits{}); err == nil {
			t.Fatalf("old revision accepted: %s/%s", tc.version, tc.revision)
		}
		if _, err := LoadBlock(idx, 0, 0, payload); err == nil {
			t.Fatalf("old revision decoded through unchecked index: %s/%s", tc.version, tc.revision)
		}
	}
}

func TestCurrentReleaseHTTPNativeFixture(t *testing.T) {
	hexData, err := os.ReadFile("testdata/native-v1.155.0.hex")
	if err != nil {
		t.Fatal(err)
	}
	data, err := hex.DecodeString(strings.TrimSpace(string(hexData)))
	if err != nil {
		t.Fatal(err)
	}
	const start int64 = 1793923200000
	idx, payload, err := Build(bytes.NewReader(data), Tenant{42, 7}, start, start+100, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.Series) != 1 || len(idx.Series[0].Blocks) != 1 || !reflect.DeepEqual(idx.Series[0].Labels, map[string]string{"__name__": "fixture_155", "job": "codec"}) {
		t.Fatalf("unexpected fixture index: %+v", idx)
	}
	mb, err := LoadBlock(idx, 0, 0, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := mb.Block.UnmarshalData(); err != nil {
		t.Fatal(err)
	}
	ts, vs := mb.Block.AppendRowsWithTimeRangeFilter(nil, nil, storage.TimeRange{MinTimestamp: start, MaxTimestamp: start + 99})
	if !reflect.DeepEqual(ts, []int64{start + 10, start + 50}) || !reflect.DeepEqual(vs, []float64{2, 3.5}) {
		t.Fatalf("captured HTTP samples changed: %v %v", ts, vs)
	}
	var name storage.MetricName
	if err := name.Unmarshal(mb.MetricName); err != nil || name.AccountID != 42 || name.ProjectID != 7 {
		t.Fatalf("tenant restoration failed: %+v %v", name, err)
	}
}
