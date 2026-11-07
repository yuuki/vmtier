package pack

import (
	"bytes"
	"errors"
	"testing"
)

func TestBuildPacksRotatesOnlyBetweenCompleteRecords(t *testing.T) {
	first, payload := nativeFixture(100, 200, map[string]string{"__name__": "x"}, []int64{100, 110}, []int64{1, 2}, 0)
	second, _ := nativeFixture(100, 200, map[string]string{"__name__": "y"}, []int64{120, 130}, []int64{3, 4}, 0)
	input := append(append([]byte(nil), first...), second[16:]...)
	one, _, err := Build(bytes.NewReader(first), Tenant{1, 2}, 100, 200, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	index, err := MarshalIndex(one, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for name, limits := range map[string]Limits{
		"bytes":  {MaxPackBytes: int64(len(payload))},
		"blocks": {MaxBlocks: 1},
		"series": {MaxSeries: 1},
		"index":  {MaxIndexBytes: int64(len(index))},
	} {
		t.Run(name, func(t *testing.T) {
			packs := 0
			stats, err := BuildPacksMeasured(bytes.NewReader(input), Tenant{1, 2}, 100, 200, limits, 10, 100, func(idx *Index, data []byte) error {
				packs++
				if idx.Start != 100 || idx.End != 200 || idx.Tenant != (Tenant{1, 2}) {
					t.Fatalf("coverage/tenant changed: %+v", idx)
				}
				if len(idx.Series) != 1 || len(idx.Series[0].Blocks) != 1 {
					t.Fatalf("record split: %+v", idx)
				}
				if _, err := MarshalIndex(idx, limits); err != nil {
					return err
				}
				if _, err := LoadBlock(idx, 0, 0, data); err != nil {
					return err
				}
				return nil
			})
			if err != nil || packs != 2 {
				t.Fatalf("packs=%d: %v", packs, err)
			}
			if stats.NativeInputBytes != int64(len(input)) || stats.PhysicalSamples != 4 || stats.Records != 2 {
				t.Fatalf("wrong input accounting: %+v", stats)
			}
		})
	}
}

func TestBuildPacksWindowBoundsAndFailure(t *testing.T) {
	first, payload := nativeFixture(100, 200, map[string]string{"__name__": "x"}, []int64{90, 100, 100, 199, 200}, []int64{1, 2, 3, 4, 5}, 0)
	input := append(append(append([]byte(nil), first...), first[16:]...), first[16:]...)
	for _, tc := range []struct {
		name           string
		data           []byte
		limits         Limits
		packs, samples int
	}{
		{"physical including clipped and replicas", input, Limits{MaxBlocks: 1}, 10, 14},
		{"pack count", input, Limits{MaxBlocks: 1}, 2, 100},
		{"whole stream input", input, Limits{MaxBlocks: 1, MaxInputBytes: int64(len(input) - 1)}, 10, 100},
		{"single record", first, Limits{MaxPackBytes: int64(len(payload) - 1)}, 10, 100},
		{"truncated after staged pack", input[:len(input)-1], Limits{MaxBlocks: 1}, 10, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			emitted := 0
			err := BuildPacks(bytes.NewReader(tc.data), Tenant{}, 100, 200, tc.limits, tc.packs, tc.samples, func(*Index, []byte) error { emitted++; return nil })
			if err == nil {
				t.Fatal("over-limit/incomplete input accepted")
			}
			if tc.name == "truncated after staged pack" && emitted != 1 {
				t.Fatalf("expected one staged pack before truncation: %d", emitted)
			}
		})
	}
	called := 0
	if err := BuildPacks(bytes.NewReader(first[:16]), Tenant{}, 100, 200, Limits{}, 10, 100, func(*Index, []byte) error { called++; return nil }); err != nil || called != 0 {
		t.Fatalf("empty input emitted packs: %d %v", called, err)
	}
	sentinel := errors.New("failed staging")
	if err := BuildPacks(bytes.NewReader(input), Tenant{}, 100, 200, Limits{MaxBlocks: 1}, 10, 100, func(*Index, []byte) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("staging failure lost: %v", err)
	}
}

func TestBuildPacksEncodedIndexBoundForRepeatedSeries(t *testing.T) {
	native, _ := nativeFixture(100, 200, map[string]string{"__name__": "x", "escape": "\"<&"}, []int64{100}, []int64{1}, 0)
	input := append([]byte(nil), native...)
	for range 40 {
		input = append(input, native[16:]...)
	}
	for _, max := range []int64{750, 1000, 1500, 2000} {
		limits := Limits{MaxIndexBytes: max}
		err := BuildPacks(bytes.NewReader(input), Tenant{}, 100, 200, limits, 100, 100, func(idx *Index, data []byte) error {
			encoded, err := MarshalIndex(idx, limits)
			if err != nil {
				return err
			}
			if int64(len(encoded)) > max {
				t.Fatalf("index length %d > %d", len(encoded), max)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("max %d: %v", max, err)
		}
	}
}
