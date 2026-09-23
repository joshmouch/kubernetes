package version

var (
	gitMajor = "1"
	gitMinor = "35"
	gitVersion   = "v1.35.9-k3s1"
	gitCommit    = "5083c750509162209a70103ad9abc67671a94bcf"
	gitTreeState = "clean"
	buildDate = "2026-09-23T20:12:44Z"
)

const (
	// DefaultKubeBinaryVersion is the hard coded k8 binary version based on the latest K8s release.
	// It is supposed to be consistent with gitMajor and gitMinor, except for local tests, where gitMajor and gitMinor are "".
	// Should update for each minor release!
	DefaultKubeBinaryVersion = "1.35"
)
