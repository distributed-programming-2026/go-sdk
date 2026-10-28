package suite

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	testify "github.com/stretchr/testify/suite"
)

type exampleSuite struct {
	Suite
	contexts       []context.Context
	setupCalled    bool
	subSetupCalled bool
}

func (s *exampleSuite) SetupTest() {
	s.setupCalled = true
	s.contexts = append(s.contexts, s.Context())
}
func (s *exampleSuite) SetupSubTest() { s.subSetupCalled = true }
func (s *exampleSuite) TestBinding() {
	s.True(s.setupCalled)
	outer := s.Context()
	s.Run("nested", func() {
		s.True(s.subSetupCalled)
		child := s.Context()
		s.NotEqual(outer, child)
		s.contexts = append(s.contexts, child)
	})
	s.Equal(outer, s.Context())
}
func TestAutomaticBindingAndNestedHooks(t *testing.T) {
	env := integrationtests.New(integrationtests.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	s := &exampleSuite{Suite: Suite{Env: env}}
	t.Run("suite", func(t *testing.T) { testify.Run(t, s) })
	for _, ctx := range s.contexts {
		if ctx.Err() == nil {
			t.Fatal("completed test still has active context")
		}
	}
	env.Abort(context.Canceled) // Completed tests must already be unregistered.
}
