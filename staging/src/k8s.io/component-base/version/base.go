package version

var (
	gitMajor = "1"
	gitMinor = "35"
	gitVersion   = "v1.35.10-k3s1"
	gitCommit    = "79c02a762645da9f753c337709dbb0deceba99dc"
	gitTreeState = "clean"
	buildDate = "2026-10-28T14:35:07Z"
)

const (
	// DefaultKubeBinaryVersion is the hard coded k8 binary version based on the latest K8s release.
	// It is supposed to be consistent with gitMajor and gitMinor, except for local tests, where gitMajor and gitMinor are "".
	// Should update for each minor release!
	DefaultKubeBinaryVersion = "1.35"
)
