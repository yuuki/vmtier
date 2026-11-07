package main

import (
	"bytes"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/yuuki/vmtier/internal/pack"
)

func TestReleaseMetadataOverridesToolchainStamp(t *testing.T) {
	info := &debug.BuildInfo{GoVersion: "go1.27.3", Main: debug.Module{Version: "v0.0.0-20261107013401-29cb29ebcf2e"},
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "29cb29ebcf2e0d97e325b498203d6951eb8a8e46"}}}
	b := resolveBuild("v0.1.0", "0123456789abcdef0123456789abcdef01234567", info)
	if b.Version != "v0.1.0" || b.Commit != "0123456789abcdef0123456789abcdef01234567" || b.GoVersion != "go1.27.3" {
		t.Fatalf("release metadata not preferred: %+v", b)
	}
}

func TestDevelopmentBuildUsesModuleAndVCSStamp(t *testing.T) {
	info := &debug.BuildInfo{GoVersion: "go1.27.3", Main: debug.Module{Version: "v0.1.1-0.20261107013401-29cb29ebcf2e+dirty"},
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "29cb29ebcf2e0d97e325b498203d6951eb8a8e46"}}}
	b := resolveBuild("", "", info)
	if b.Version != "v0.1.1-0.20261107013401-29cb29ebcf2e+dirty" || b.Commit != "29cb29ebcf2e0d97e325b498203d6951eb8a8e46" {
		t.Fatalf("toolchain stamp ignored: %+v", b)
	}
	b = resolveBuild("", "", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}})
	if b.Version != "devel" || b.Commit != "unknown" {
		t.Fatalf("unstamped build is not explicit: %+v", b)
	}
}

func TestVersionOutputKeepsCompatibilityPin(t *testing.T) {
	var out bytes.Buffer
	writeVersion(&out, buildMetadata{Version: "v0.1.0", Commit: "abc", GoVersion: "go1.27.3", Platform: "linux/amd64"})
	line := out.String()
	for _, want := range []string{"vmtier v0.1.0 (commit abc, go1.27.3 linux/amd64);", "SQLite schema", pack.VMVersion + " " + pack.VMRevision + "\n"} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %q in %q", want, line)
		}
	}
	out.Reset()
	writeBuildInfoMetric(&out, buildMetadata{Version: `v0.1.0"x`, Commit: "abc", GoVersion: "go1.27.3"})
	if got := out.String(); !strings.HasPrefix(got, `vmtier_build_info{version="v0.1.0\"x",commit="abc",go_version="go1.27.3",vm_version=`) || !strings.HasSuffix(got, "} 1\n") {
		t.Fatalf("unexpected build info metric %q", got)
	}
}
