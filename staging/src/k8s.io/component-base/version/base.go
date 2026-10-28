package version

var (
	gitMajor = "1"
	gitMinor = "36"
	gitVersion   = "v1.36.6-k3s1"
	gitCommit    = "0328f18adfaa25c77afcf0b9c121289831e8aa9a"
	gitTreeState = "clean"
	buildDate = "2026-10-28T19:02:12Z"
)

const (
	// DefaultKubeBinaryVersion is the hard coded k8 binary version based on the latest K8s release.
	// It is supposed to be consistent with gitMajor and gitMinor, except for local tests, where gitMajor and gitMinor are "".
	// Should update for each minor release!
	DefaultKubeBinaryVersion = "1.36"
)
