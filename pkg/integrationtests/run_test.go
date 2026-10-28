package integrationtests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	scenarioAbort   = "abort"
	scenarioCancel  = "cancel"
	scenarioSuccess = "success"
)

var childEnv *Environment

func TestMain(m *testing.M) {
	if os.Getenv("SDK_RUN_CHILD") == "" {
		os.Exit(m.Run())
	}
	childEnv = New(Config{
		Timeouts: Timeouts{Tests: 100 * time.Millisecond, Down: time.Second},
		Setup: func(_ context.Context, env *Environment) error {
			return env.AddCleanup("marker", func(context.Context) error {
				// #nosec G703 -- parent test supplies a path inside t.TempDir.
				return os.WriteFile(os.Getenv("SDK_CLEANUP_MARKER"), []byte("cleaned"), 0o600)
			})
		},
	})
	os.Exit(Run(m, childEnv))
}

func TestRunnerChild(t *testing.T) {
	scenario := os.Getenv("SDK_RUN_CHILD")
	if scenario == "" {
		t.Skip("subprocess only")
	}
	ctx := childEnv.Bind(t)
	switch scenario {
	case "parallel":
		go func() { time.Sleep(20 * time.Millisecond); childEnv.Abort(errors.New("parallel abort")) }()
		for i := range 40 {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Parallel()
				ctx := childEnv.Bind(t)
				if i%2 == 0 {
					return
				}
				<-ctx.Done()
			})
		}
	case "hang":
		select {}
	case scenarioAbort:
		childEnv.Abort(errors.New("infrastructure broken"))
		<-ctx.Done()
		select {}
	case scenarioCancel:
		go childEnv.Abort(context.Canceled)
		<-ctx.Done()
	case "failure":
		t.Error("ordinary assertion failure")
	case scenarioSuccess:
	default:
		t.Fatal("unknown scenario")
	}
}

func TestRunnerCleanupBeforeExit(t *testing.T) {
	for _, scenario := range []string{"hang", scenarioAbort, scenarioCancel, "parallel", "failure", scenarioSuccess} {
		t.Run(scenario, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "cleanup")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			// #nosec G204 G702 -- rerun this test executable, with fixed test arguments.
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRunnerChild$", "-test.timeout=5s")
			cmd.Env = append(os.Environ(), "SDK_RUN_CHILD="+scenario, "SDK_CLEANUP_MARKER="+marker)
			output, err := cmd.CombinedOutput()
			if strings.Contains(string(output), "DATA RACE") || strings.Contains(string(output), "after ") && strings.Contains(string(output), "has completed") {
				t.Fatalf("unsafe test registration:\n%s", output)
			}
			if scenario == scenarioSuccess && err != nil {
				t.Fatalf("successful child: %v\n%s", err, output)
			}
			if scenario != scenarioSuccess && err == nil {
				t.Fatalf("failed child succeeded:\n%s", output)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("cleanup was not executed: %v\n%s", err, output)
			}
			if (scenario == scenarioAbort || scenario == scenarioCancel) && !strings.Contains(string(output), "integration environment aborted") {
				t.Fatalf("active test not failed:\n%s", output)
			}
		})
	}
}

func TestRunnerRejectsConflictingNativeTimeout(t *testing.T) {
	// #nosec G204 G702 -- rerun this test executable, with fixed test arguments.
	cmd := exec.Command(os.Args[0], "-test.run=^TestRunnerChild$", "-test.timeout=50ms")
	cmd.Env = append(os.Environ(), "SDK_RUN_CHILD=success")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "must exceed") {
		t.Fatalf("timeout validation: %v\n%s", err, output)
	}
}

func ExampleNew() {
	env := New(Config{})
	fmt.Println(env.Timeouts().Tests)
	// Output: 5m0s
}

func TestCleanupFailureMakesSuccessfulRunFail(t *testing.T) {
	env := New(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Setup: func(_ context.Context, env *Environment) error {
			return env.AddCleanup("broken closer", func(context.Context) error { return errors.New("close failed") })
		},
	})
	code, aborted := execute(env, func() int { return 0 })
	if code != 1 || !aborted {
		t.Fatalf("cleanup failure returned code=%d aborted=%v", code, aborted)
	}
}
