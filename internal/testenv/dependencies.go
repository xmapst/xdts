package testenv

// Dependency describes one fixture required by an integration test. Reason
// contains only the missing configuration key, never its credential value.
type Dependency struct {
	Requirement Requirement
	Available   bool
	Reason      string
}

type Dependencies []Dependency

// Require validates every switch before choosing a missing-fixture outcome.
// A required missing service takes priority over optional missing services,
// so an optional database cannot hide a required missing broker or vice versa.
func (dependencies Dependencies) Require(t ISkipFataler) {
	t.Helper()
	for _, dependency := range dependencies {
		dependency.Requirement.Check(t)
	}
	for _, dependency := range dependencies {
		if !dependency.Available && dependency.Requirement.Required() {
			dependency.Requirement.Unavailable(t, dependency.Reason)
			return
		}
	}
	for _, dependency := range dependencies {
		if !dependency.Available {
			dependency.Requirement.Unavailable(t, dependency.Reason)
			return
		}
	}
}
