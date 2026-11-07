package main

import (
	"fmt"
	"io"
	"runtime"
	"runtime/debug"

	"github.com/yuuki/vmtier/internal/catalog"
	"github.com/yuuki/vmtier/internal/pack"
)

// The release build sets these with -ldflags -X because it compiles an
// exported commit without VCS metadata. Development builds leave them empty
// and fall back to the module and VCS information stamped by the Go toolchain.
var (
	version string
	commit  string
)

type buildMetadata struct {
	Version   string
	Commit    string
	GoVersion string
	Platform  string
}

func currentBuild() buildMetadata {
	info, _ := debug.ReadBuildInfo()
	return resolveBuild(version, commit, info)
}

func resolveBuild(version, commit string, info *debug.BuildInfo) buildMetadata {
	b := buildMetadata{Version: version, Commit: commit, GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH}
	if info != nil {
		b.GoVersion = info.GoVersion
		if b.Version == "" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			b.Version = info.Main.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && b.Commit == "" {
				b.Commit = setting.Value
			}
		}
	}
	if b.Version == "" {
		b.Version = "devel"
	}
	if b.Commit == "" {
		b.Commit = "unknown"
	}
	return b
}

func writeVersion(w io.Writer, b buildMetadata) {
	fmt.Fprintf(w, "vmtier %s (commit %s, %s %s); JSON catalog v%d; SQLite schema %d; VM %s %s\n",
		b.Version, b.Commit, b.GoVersion, b.Platform, catalog.Version, catalog.SQLiteSchemaVersion, pack.VMVersion, pack.VMRevision)
}

// vmtier_build_info identifies the running artifact; its labels are fixed for
// the process lifetime, so the series count stays constant.
func writeBuildInfoMetric(w io.Writer, b buildMetadata) {
	fmt.Fprintf(w, "vmtier_build_info{version=%q,commit=%q,go_version=%q,vm_version=%q} 1\n", b.Version, b.Commit, b.GoVersion, pack.VMVersion)
}
