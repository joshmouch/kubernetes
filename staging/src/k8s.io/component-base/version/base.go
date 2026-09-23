package version

var (
	gitMajor = "1"
	gitMinor = "36"
	gitVersion   = "v1.36.5-k3s1"
	gitCommit    = "9273f82799a6eed890e0353e384b7dff880580df"
	gitTreeState = "clean"
	buildDate = "2026-09-23T20:18:04Z"
)

const (
	// DefaultKubeBinaryVersion is the hard coded k8 binary version based on the latest K8s release.
	// It is supposed to be consistent with gitMajor and gitMinor, except for local tests, where gitMajor and gitMinor are "".
	// Should update for each minor release!
	DefaultKubeBinaryVersion = "1.36"
)
