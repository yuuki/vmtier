// Package gateway adapts immutable Cold blocks to the pinned OSS vmselect RPC.
package gateway

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/regexutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
)

// Matcher is a compiled disjunction of conjunctions of label filters.
// The implementation uses the pinned VM regex implementation. Missing labels
// have empty values, including VM's special handling of a literal !~".*".
type Matcher struct{ groups [][]labelMatcher }
type labelMatcher struct {
	key, value  string
	negative    bool
	re          *regexutil.PromRegex
	missingOnly bool
}

// CompileMatcher compiles filters once per query, before scanning an index.
func CompileMatcher(filters [][]storage.TagFilter) (*Matcher, error) {
	m := &Matcher{}
	for _, group := range filters {
		var dst []labelMatcher
		for _, f := range group {
			key := string(f.Key)
			if key == "" {
				key = "__name__"
			}
			if key == "__graphite__" {
				return nil, fmt.Errorf("unsupported Graphite matcher")
			}
			lm := labelMatcher{key: key, value: string(f.Value), negative: f.IsNegative}
			if f.IsRegexp {
				// storage.TagFilters.Add rewrites this exact negative expression
				// into a negative .+ matcher, retaining absent labels.
				if lm.value == ".*" && lm.negative {
					lm.missingOnly = true
				} else {
					var err error
					lm.re, err = regexutil.NewPromRegex(lm.value)
					if err != nil {
						return nil, fmt.Errorf("invalid label regexp: %w", err)
					}
				}
			}
			dst = append(dst, lm)
		}
		m.groups = append(m.groups, dst)
	}
	return m, nil
}

func (f labelMatcher) match(value string) bool {
	if f.missingOnly {
		return value == ""
	}
	matched := value == f.value
	if f.re != nil {
		matched = f.re.MatchString(value)
	}
	if f.negative {
		return !matched
	}
	return matched
}

// MatchLabels applies a compiled query to the canonical label dictionary.
func (m *Matcher) MatchLabels(labels map[string]string) bool {
	return m.match(func(key string) string { return labels[key] })
}

// Match applies a compiled query to a decoded metric name. Tenant checking is
// deliberately separate: a label filter must never act as tenant authorization.
func (m *Matcher) Match(mn *storage.MetricName) bool {
	return m.match(func(key string) string {
		if key == "__name__" {
			return string(mn.MetricGroup)
		}
		return string(mn.GetTagValue(key))
	})
}

func (m *Matcher) match(value func(string) string) bool {
	if len(m.groups) == 0 {
		return true
	}
	for _, group := range m.groups {
		ok := true
		for _, f := range group {
			if !f.match(value(f.key)) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
