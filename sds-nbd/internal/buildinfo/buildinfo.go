// Package buildinfo carries version metadata stamped at link time via
// -ldflags -X (see the Makefile release target). The values are set by
// the release build; dev builds report "dev".
package buildinfo

// Version is the release version (YYYYmmddNN on released builds).
var Version = "dev"

// Commit is the full git SHA the binary was built from.
var Commit = "unknown"

// Channel is the release channel: develop, testing or stable.
var Channel = "develop"

// Date is the commit date baked into the build (RFC3339).
var Date = ""

// Line renders the "name version (commit channel date)" one-liner that
// the -version flags print.
func Line(name string) string {
	s := name + " " + Version + " (" + Commit
	if Channel != "" {
		s += " " + Channel
	}
	if Date != "" {
		s += " " + Date
	}
	return s + ")"
}
