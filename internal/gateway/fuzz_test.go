package gateway

import (
	"regexp"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/yuuki/vmtier/internal/pack"
)

// fuzzFilter is one label filter of a fuzzed query.
type fuzzFilter struct {
	key, value         string
	negative, isRegexp bool
}

// expected evaluates f with plain Go semantics: an anchored regexp, missing
// labels as empty values, and VM's rewrite of a negative ".*" matcher into
// "label is absent or empty". ok is false where the reference does not apply.
func (f fuzzFilter) expected(labels map[string]string) (matched, ok bool) {
	key := f.key
	if key == "" {
		key = "__name__"
	}
	value := labels[key]
	switch {
	case f.isRegexp && f.negative && f.value == ".*":
		return value == "", true
	case f.isRegexp:
		// VM's literal fast paths treat '.' as any byte, as its Hot index
		// matcher does; plain Go regexps do not match newlines with '.'.
		re, err := regexp.Compile("^(?:" + f.value + ")$")
		if err != nil || strings.Contains(value, "\n") {
			return false, false
		}
		matched = re.MatchString(value)
	default:
		matched = value == f.value
	}
	return matched != f.negative, true
}

// FuzzMatcher checks that the two matching paths of a Cold query agree: the
// reader filters index label dictionaries with MatchLabels before any Range
// GET, and the gateway filters decoded wire metric names with Match. Both must
// also agree with the reference semantics of each filter.
func FuzzMatcher(f *testing.F) {
	for _, seed := range []struct {
		name, labelKey, labelValue string
		a, b                       fuzzFilter
		or                         bool
	}{
		{"up", "job", "api", fuzzFilter{"job", "api", false, false}, fuzzFilter{"", "up", false, false}, false},
		{"up", "job", "api", fuzzFilter{"job", "a.*", false, true}, fuzzFilter{"job", ".+i", true, true}, true},
		{"up", "job", "", fuzzFilter{"job", ".*", true, true}, fuzzFilter{"instance", "", false, false}, false},
		{"http_requests_total", "code", "500", fuzzFilter{"", "http_.*", false, true}, fuzzFilter{"code", "5..|404", false, true}, false},
		{"x", "env", "prod\nnext", fuzzFilter{"env", ".*prod.*", false, true}, fuzzFilter{"env", "", true, false}, true},
		{"x", "k", "v", fuzzFilter{"__graphite__", "x", false, false}, fuzzFilter{"k", "(", false, true}, false},
	} {
		f.Add(seed.name, seed.labelKey, seed.labelValue, seed.a.key, seed.a.value, seed.a.negative, seed.a.isRegexp, seed.b.key, seed.b.value, seed.b.negative, seed.b.isRegexp, seed.or)
	}
	f.Fuzz(func(t *testing.T, name, labelKey, labelValue, keyA, valueA string, negativeA, regexpA bool, keyB, valueB string, negativeB, regexpB, or bool) {
		if len(valueA) > 256 || len(valueB) > 256 {
			return // keep regexp compilation cheap
		}
		filters := []fuzzFilter{{keyA, valueA, negativeA, regexpA}, {keyB, valueB, negativeB, regexpB}}
		tagFilters := make([]storage.TagFilter, len(filters))
		for i, flt := range filters {
			tagFilters[i] = storage.TagFilter{Key: []byte(flt.key), Value: []byte(flt.value), IsNegative: flt.negative, IsRegexp: flt.isRegexp}
		}
		groups := [][]storage.TagFilter{tagFilters}
		if or {
			groups = [][]storage.TagFilter{tagFilters[:1], tagFilters[1:]}
		}
		m, err := CompileMatcher(groups)
		if err != nil {
			return
		}
		labels := map[string]string{"__name__": name}
		if labelKey != "" && labelKey != "__name__" {
			labels[labelKey] = labelValue
		}
		// The gateway sees the tenant-qualified wire name that LoadBlock builds.
		wire := pack.MetricName(pack.Tenant{AccountID: 1, ProjectID: 2}, labels).Marshal(nil)
		var decoded storage.MetricName
		if err := decoded.Unmarshal(wire); err != nil {
			t.Fatal(err)
		}
		byLabels, byName := m.MatchLabels(labels), m.Match(&decoded)
		if byLabels != byName {
			t.Fatalf("index pre-filter (%v) and wire filter (%v) disagree for %q", byLabels, byName, labels)
		}
		a, okA := filters[0].expected(labels)
		b, okB := filters[1].expected(labels)
		if !okA || !okB {
			return
		}
		want := a && b
		if or {
			want = a || b
		}
		if byName != want {
			t.Fatalf("matcher returned %v, reference %v, for %q with %+v (or=%v)", byName, want, labels, filters, or)
		}
	})
}
