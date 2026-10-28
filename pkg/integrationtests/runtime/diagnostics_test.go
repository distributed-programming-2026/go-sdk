package runtime

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const logFailure = "failure"

func TestDiagnosticOutputChild(t *testing.T) {
	if os.Getenv("SDK_LOG_CHILD") == "" {
		t.Skip("subprocess only")
	}
	enabled, disabled := service("enabled"), service("disabled")
	disabled.Logs = Disabled
	env, _ := testEnvironment(Config{Containers: []ContainerSpec{enabled, disabled}})
	startEnvironment(t, env)
	env.Bind(t)
	env.instances[0].logs.write(time.Now(), "enabled-test-output")
	env.instances[1].logs.write(time.Now(), "disabled-test-output")
	if os.Getenv("SDK_LOG_CHILD") == logFailure {
		t.Error("deliberate assertion failure")
	}
}

func TestDiagnosticOutputOnlyOnFailure(t *testing.T) {
	for _, scenario := range []string{"success", logFailure} {
		t.Run(scenario, func(t *testing.T) {
			// #nosec G204 G702 -- this test executable and fixed arguments only.
			cmd := exec.Command(os.Args[0], "-test.run=^TestDiagnosticOutputChild$", "-test.v")
			cmd.Env = append(os.Environ(), "SDK_LOG_CHILD="+scenario)
			output, err := cmd.CombinedOutput()
			failed := scenario == logFailure
			if (err != nil) != failed {
				t.Fatalf("child: %v\n%s", err, output)
			}
			if strings.Contains(string(output), "enabled-test-output") != failed {
				t.Fatalf("wrong log visibility:\n%s", output)
			}
			if strings.Contains(string(output), "disabled-test-output") {
				t.Fatalf("disabled logs leaked:\n%s", output)
			}
		})
	}
}

func TestCleanupDeadlineBoundsUncooperativeHook(t *testing.T) {
	env, _ := testEnvironment(Config{Timeouts: Timeouts{Down: 20 * time.Millisecond}})
	release := make(chan struct{})
	if err := env.AddCleanup("blocked", func(context.Context) error { <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	defer close(release)
	started := time.Now()
	if err := env.Down(context.Background()); err == nil {
		t.Fatal("deadline ignored")
	}
	if time.Since(started) > time.Second {
		t.Fatal("cleanup exceeded its budget")
	}
}
