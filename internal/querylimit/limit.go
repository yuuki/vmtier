// Package querylimit identifies bounded resource refusals without error-text labels.
package querylimit

import (
	"errors"
	"fmt"
)

const (
	MemoryAdmission = "memory_admission"
	SharedReads     = "shared_reads"
	ReadBytes       = "read_bytes"
	DecodedBytes    = "decoded_bytes"
	CatalogPlan     = "catalog_plan"
	Blocks          = "blocks"
	Series          = "series"
	ResponseBytes   = "response_bytes"
)

func Reasons() []string {
	return []string{MemoryAdmission, SharedReads, ReadBytes, DecodedBytes, CatalogPlan, Blocks, Series, ResponseBytes}
}

type refusal struct {
	reason  string
	message string
}

func (e *refusal) Error() string { return e.message }

func Errorf(reason, format string, args ...any) error {
	return &refusal{reason, fmt.Sprintf(format, args...)}
}

func Reason(err error) string {
	var e *refusal
	if !errors.As(err, &e) {
		return ""
	}
	for _, reason := range Reasons() {
		if e.reason == reason {
			return reason
		}
	}
	return ""
}
