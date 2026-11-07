package archive

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
)

const maxOracleBytes = 128 << 20

// The independent oracle remains mandatory. Only the current row and metric
// are decoded alongside the bounded native sample map. The pinned VM emits
// metric before values; alternate JSON field order needs at most one bounded
// series of temporary rows, never the whole matrix or its raw JSON document.
func verifyRawResponse(body io.Reader, maxBytes int64, maxSamples int, start, end int64, want map[string][]sample) error {
	limited := &io.LimitedReader{R: body, N: maxBytes + 1}
	d := json.NewDecoder(limited)
	v := rawVerifier{d: d, maxSamples: maxSamples, start: start, end: end, want: want, seen: make(map[string]bool)}
	err := v.envelope()
	if err == nil {
		var token json.Token
		token, err = d.Token()
		if err == io.EOF {
			err = nil
		} else if err == nil {
			err = fmt.Errorf("trailing raw verification content: %v", token)
		}
	}
	if limited.N <= 0 {
		return errors.New("raw verification response too large; choose a smaller window before catalog initialization; changing a bound window requires migration to a new catalog")
	}
	if err != nil {
		return fmt.Errorf("incomplete or invalid raw verification JSON: %w", err)
	}
	if len(v.seen) != len(want) {
		return errors.New("native export includes series absent from raw source")
	}
	return nil
}

type rawVerifier struct {
	d                 *json.Decoder
	maxSamples, total int
	start, end        int64
	want              map[string][]sample
	seen              map[string]bool
}

func delimiter(d *json.Decoder, want json.Delim) error {
	t, err := d.Token()
	if err != nil {
		return err
	}
	if t != want {
		return fmt.Errorf("expected JSON %q", want)
	}
	return nil
}

// Reject repeated object members rather than allowing a later value to hide a
// partial response, earlier failure status, or previously decoded result.
func objectFields(d *json.Decoder, field func(string) error) error {
	if err := delimiter(d, '{'); err != nil {
		return err
	}
	seen := make(map[string]bool)
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := t.(string)
		if !ok || seen[key] {
			return errors.New("invalid or duplicate raw verification field")
		}
		seen[key] = true
		if err := field(key); err != nil {
			return err
		}
	}
	return delimiter(d, '}')
}

// Discard unrecognized extension fields incrementally too. Decoding into an
// interface or RawMessage here would reintroduce whole-response allocations.
func skipJSON(d *json.Decoder) error {
	depth := 0
	for {
		t, err := d.Token()
		if err != nil {
			return err
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
		if depth == 0 {
			return nil
		}
	}
}

func (v *rawVerifier) envelope() error {
	status, data := false, false
	err := objectFields(v.d, func(key string) error {
		switch key {
		case "status":
			var value string
			if err := v.d.Decode(&value); err != nil {
				return err
			}
			if value != "success" {
				return errors.New("raw verification did not return success")
			}
			status = true
		case "isPartial":
			var value bool
			if err := v.d.Decode(&value); err != nil {
				return err
			}
			if value {
				return errors.New("raw verification returned partial data")
			}
		case "warnings":
			// Warnings may be absent, null, or empty, as in the old decoder.
			t, err := v.d.Token()
			if err != nil {
				return err
			}
			if t == nil {
				return nil
			}
			if t != json.Delim('[') {
				return errors.New("invalid raw verification warnings")
			}
			if v.d.More() {
				return errors.New("raw verification returned warnings")
			}
			return delimiter(v.d, ']')
		case "data":
			data = true
			return v.matrix()
		default:
			return skipJSON(v.d)
		}
		return nil
	})
	if err == nil && (!status || !data) {
		return errors.New("raw verification missing success matrix")
	}
	return err
}

func (v *rawVerifier) matrix() error {
	typ, result := false, false
	err := objectFields(v.d, func(key string) error {
		switch key {
		case "resultType":
			var value string
			if err := v.d.Decode(&value); err != nil {
				return err
			}
			if value != "matrix" {
				return errors.New("raw verification did not return matrix")
			}
			typ = true
		case "result":
			result = true
			t, err := v.d.Token()
			if err != nil {
				return err
			}
			if t == nil {
				return nil
			}
			if t != json.Delim('[') {
				return errors.New("invalid raw verification result")
			}
			for v.d.More() {
				if err := v.series(); err != nil {
					return err
				}
			}
			return delimiter(v.d, ']')
		default:
			return skipJSON(v.d)
		}
		return nil
	})
	if err == nil && (!typ || !result) {
		return errors.New("raw verification missing matrix result")
	}
	return err
}

func (v *rawVerifier) series() error {
	metric, values := false, false
	var expected, pending []sample
	count := 0
	err := objectFields(v.d, func(key string) error {
		switch key {
		case "metric":
			var labels map[string]string
			if err := v.d.Decode(&labels); err != nil {
				return err
			}
			if labels == nil {
				return errors.New("raw verification missing metric labels")
			}
			encoded, err := json.Marshal(labels)
			if err != nil {
				return err
			}
			key := string(encoded)
			if v.seen[key] {
				return errors.New("duplicate raw verification series")
			}
			var ok bool
			expected, ok = v.want[key]
			if !ok {
				return errors.New("raw source includes series absent from native export")
			}
			v.seen[key] = true
			metric = true
			for i, row := range pending {
				if err := v.compare(row, expected, i); err != nil {
					return err
				}
			}
			pending = nil
		case "values":
			values = true
			if err := delimiter(v.d, '['); err != nil {
				return err
			}
			var pair []json.RawMessage
			for v.d.More() {
				v.total++
				if v.total > v.maxSamples {
					return errors.New("raw verification sample limit exceeded")
				}
				// Reuse the pair's backing buffers across rows. No matrix-sized
				// collection of RawMessages survives comparison.
				if err := v.d.Decode(&pair); err != nil {
					return err
				}
				if len(pair) != 2 {
					return errors.New("invalid raw verification row")
				}
				timestamp, err := rawTimestamp(pair[0])
				if err != nil {
					return err
				}
				var value string
				if err := json.Unmarshal(pair[1], &value); err != nil {
					return err
				}
				actual, err := strconv.ParseFloat(value, 64)
				if err != nil {
					return errors.New("invalid raw verification value")
				}
				row := sample{timestamp, actual}
				if metric {
					if err := v.compare(row, expected, count); err != nil {
						return err
					}
				} else {
					pending = append(pending, row)
				}
				count++
			}
			return delimiter(v.d, ']')
		default:
			return skipJSON(v.d)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !metric || !values {
		return errors.New("raw verification missing metric or values")
	}
	if count != len(expected) {
		return errors.New("raw source/native sample count mismatch")
	}
	return nil
}

func (v *rawVerifier) compare(actual sample, expected []sample, i int) error {
	if i >= len(expected) {
		return errors.New("raw source/native sample count mismatch")
	}
	if actual.timestamp < v.start || actual.timestamp >= v.end || actual.timestamp != expected[i].timestamp {
		return errors.New("raw source/native timestamp mismatch")
	}
	if math.IsNaN(actual.value) {
		// The native decimal codec only supports the stale NaN sentinel.
		if !decimal.IsStaleNaN(expected[i].value) {
			return errors.New("raw source/native stale marker mismatch")
		}
	} else if math.Float64bits(actual.value) != math.Float64bits(expected[i].value) {
		return errors.New("raw source/native value mismatch")
	}
	return nil
}
