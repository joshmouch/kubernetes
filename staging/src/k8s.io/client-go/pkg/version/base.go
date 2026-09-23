package version

var (
	gitMajor = "1"
	gitMinor = "34"
	gitVersion   = "v1.34.12-k3s1"
	gitCommit    = "58d9a2d3e3ae4f28b89620c243ad6f5799d8f91e"
	gitTreeState = "clean"
	buildDate = "2026-09-23T20:09:59Z"
)

const (
	// DefaultKubeBinaryVersion is the hard coded k8 binary version based on the latest K8s release.
	// It is supposed to be consistent with gitMajor and gitMinor, except for local tests, where gitMajor and gitMinor are "".
	// Should update for each minor release!
	DefaultKubeBinaryVersion = "1.34"
)
