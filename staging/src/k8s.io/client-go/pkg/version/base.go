package version

var (
	gitMajor = "1"
	gitMinor = "34"
	gitVersion   = "v1.34.13-k3s1"
	gitCommit    = "7bb110fa71779d0d9ced8c056619249daa260d1a"
	gitTreeState = "clean"
	buildDate = "2026-10-28T14:05:52Z"
)

const (
	// DefaultKubeBinaryVersion is the hard coded k8 binary version based on the latest K8s release.
	// It is supposed to be consistent with gitMajor and gitMinor, except for local tests, where gitMajor and gitMinor are "".
	// Should update for each minor release!
	DefaultKubeBinaryVersion = "1.34"
)
