// Package suite provides optional testify integration with automatic test binding.
package suite

import (
	"context"
	"testing"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	testify "github.com/stretchr/testify/suite"
)

// Suite embeds testify.Suite and binds tests without occupying setup/teardown
// hooks. Initialize Env before calling testify.Run. Like testify, it does not
// support parallel tests. Overrides of SetT or Run must delegate to this base.
type Suite struct {
	testify.Suite
	Env *integrationtests.Environment
}

// SetT is called by testify before setup and each test method.
func (s *Suite) SetT(t *testing.T) {
	s.Suite.SetT(t)
	if s.Env != nil {
		s.Env.Bind(t)
	}
}

// Context returns the current test's environment-linked context.
func (s *Suite) Context() context.Context { return s.Env.Bind(s.T()) }

// Run binds nested subtests while preserving testify's hooks and panic handling.
func (s *Suite) Run(name string, test func()) bool {
	return s.Suite.Run(name, func() {
		if s.Env != nil {
			s.Env.Bind(s.T())
		}
		test()
	})
}
