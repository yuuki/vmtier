package main

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine running. Imported
// VictoriaMetrics packages start process-lifetime goroutines from init (for
// example the fasttime ticker); IgnoreCurrent excludes only those that exist
// before the first test starts.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreCurrent())
}
