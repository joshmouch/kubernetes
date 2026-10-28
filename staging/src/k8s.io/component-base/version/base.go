package version

var (
	gitMajor = "1"
	gitMinor = "37"
	gitVersion   = "v1.37.2-k3s1"
	gitCommit    = "2132c5f5a71be10ae52537b543815b860fdb67dd"
	gitTreeState = "clean"
	buildDate = "2026-10-28T18:38:29Z"
)

const (
	// DefaultKubeBinaryVersion is the hard coded k8 binary version based on the latest K8s release.
	// It is supposed to be consistent with gitMajor and gitMinor, except for local tests, where gitMajor and gitMinor are "".
	// Should update for each minor release!
	DefaultKubeBinaryVersion = "1.37"
)
