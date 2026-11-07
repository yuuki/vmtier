package archive

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPreflightMeasuresVerifiedWindowAndEmptyWindow(t *testing.T) {
	for _, mode := range []string{"", "empty"} {
		t.Run(mode, func(t *testing.T) {
			source, _ := newSQLiteSource(t, mode)
			cfg := baseConfig(source.URL)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := Preflight(ctx, cfg, testNow)
			if err != nil || !got.Verified || got.NativeInputBytes < 16 {
				t.Fatalf("preflight: %+v %v", got, err)
			}
			if mode == "empty" {
				if len(got.Packs) != 0 || got.DataBytes != 0 || got.IndexBytes != 0 || got.PhysicalSamples != 0 || got.VerifiedSamples != 0 {
					t.Fatalf("empty observation created a pack: %+v", got)
				}
			} else if len(got.Packs) != 1 || got.UniqueSeries != 1 || got.DataBytes <= 0 || got.IndexBytes <= 0 || got.PhysicalSamples <= int(got.VerifiedSamples) || got.VerifiedSamples != 3 {
				t.Fatalf("lost physical duplicates or invalid accounting: %+v", got)
			}
			example, _ := json.Marshal(got)
			t.Logf("successful preflight: %s", example)
			if got.MaxPhysicalSamples != 5000000 || got.MaxPacks < 1 || got.PackLimits.MaxPackBytes != 64<<20 {
				t.Fatalf("effective limits missing: %+v", got)
			}
		})
	}
}

func TestPreflightRejectsIncompleteAndOverBudgetObservations(t *testing.T) {
	for _, mode := range []string{"record-boundary", "partial", "physical-limit", "record-limit"} {
		t.Run(mode, func(t *testing.T) {
			source, _ := newSQLiteSource(t, mode)
			cfg := baseConfig(source.URL)
			if mode == "physical-limit" {
				cfg.MaxSamples = 1
			}
			if mode == "record-limit" {
				cfg.PackLimits.MaxPackBytes = 1
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := Preflight(ctx, cfg, testNow)
			if err == nil || got.Verified || got.Error == "" || got.VerifiedSamples != 0 {
				t.Fatalf("incomplete observation reported success: %+v %v", got, err)
			}
			example, _ := json.Marshal(got)
			t.Logf("rejected preflight: %s", example)
			if mode == "physical-limit" && !strings.Contains(err.Error(), "sample") {
				t.Fatal(err)
			}
		})
	}
}

func TestPreflightRequiresOneWindowAndDeadline(t *testing.T) {
	source, fixture := newSQLiteSource(t, "")
	cfg := baseConfig(source.URL)
	if _, err := Preflight(context.Background(), cfg, testNow); err == nil {
		t.Fatal("unbounded preflight accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cfg.EndMS += cfg.Window.Milliseconds()
	if _, err := Preflight(ctx, cfg, testNow); err == nil {
		t.Fatal("multiple windows accepted")
	}
	if fixture.exportCount("7:3") != 0 {
		t.Fatal("invalid preflight read source")
	}
}
