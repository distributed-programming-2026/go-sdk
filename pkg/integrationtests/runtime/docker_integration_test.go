package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const dockerMySQL = "mysql"

func TestDockerLifecycle(t *testing.T) {
	if os.Getenv("INTEGRATIONTESTS_DOCKER") != "1" {
		t.Skip("set INTEGRATIONTESTS_DOCKER=1 to run Docker checks")
	}
	env := New(Config{Containers: []ContainerSpec{
		{Name: dockerMySQL, Request: testcontainers.ContainerRequest{
			Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"},
			Env:        map[string]string{"MYSQL_ROOT_PASSWORD": "sdk-root"},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server"),
		}},
		{Name: "query", Manual: true, DependsOn: []string{dockerMySQL}, Request: testcontainers.ContainerRequest{Image: "mysql:8.4"},
			Configure: func(_ context.Context, env *Environment, r *testcontainers.ContainerRequest) error {
				endpoint, err := env.NetworkEndpoint(dockerMySQL, "3306/tcp")
				if err != nil {
					return err
				}
				r.Cmd = []string{dockerMySQL, "--host=" + strings.Split(endpoint, ":")[0], "--user=root", "--password=sdk-root", "-e", "SELECT 42"}
				return nil
			},
		},
	}})
	startEnvironment(t, env)
	ctx := env.Bind(t)
	host, err := env.HostEndpoint(ctx, dockerMySQL, "3306/tcp")
	if err != nil || host == "" || strings.HasSuffix(host, ":3306") {
		t.Fatalf("dynamic endpoint %q: %v", host, err)
	}
	for range 2 {
		result, err := env.RunJob(ctx, "query")
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("job %+v: %v", result, err)
		}
	}
	result, err := env.RunJob(ctx, "query", WithCommand("sh", "-c", "echo deliberate-job-failure; exit 7"))
	if err == nil || result.ExitCode != 7 {
		t.Fatalf("failed job %+v: %v", result, err)
	}
	if env.Context().Err() != nil {
		t.Fatal("ordinary job failure aborted environment")
	}
	if !strings.Contains(result.Logs, "deliberate-job-failure") {
		t.Fatalf("final job output missing: %q", result.Logs)
	}
	if err := env.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, inst := range env.instances {
		if _, err := inst.container.State(context.Background()); err == nil {
			t.Fatalf("container %s still exists", inst.container.GetContainerID())
		}
	}
}

func TestDockerBuildAndHooks(t *testing.T) {
	if os.Getenv("INTEGRATIONTESTS_DOCKER") != "1" {
		t.Skip("set INTEGRATIONTESTS_DOCKER=1 to run Docker checks")
	}
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "Dockerfile"), []byte("FROM mysql:8.4\nCMD [\"sh\", \"-c\", \"echo built-job\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var hooks []string
	env := New(Config{Containers: []ContainerSpec{{Name: "built", Manual: true, Request: testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{Context: directory, Repo: "integrationtests-build-check", Tag: fmt.Sprint(time.Now().UnixNano())},
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{{
			PreBuilds: []testcontainers.ContainerRequestHook{func(context.Context, testcontainers.ContainerRequest) error {
				hooks = append(hooks, "before")
				return nil
			}},
			PostBuilds: []testcontainers.ContainerRequestHook{func(context.Context, testcontainers.ContainerRequest) error {
				hooks = append(hooks, "after")
				return nil
			}},
		}},
	}}}})
	if err := env.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	image := env.specs["built"].Request.Image
	t.Cleanup(func() {
		client, err := testcontainers.NewDockerClientWithOpts(context.Background())
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Close()
		if _, err := client.ImageRemove(context.Background(), image, dockerclient.ImageRemoveOptions{}); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		if err := env.Down(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if strings.Join(hooks, ",") != "before,after" {
		t.Fatalf("build hooks: %v", hooks)
	}
	if err := env.Up(t.Context()); err != nil {
		t.Fatal(err)
	}
	result, err := env.RunJob(t.Context(), "built")
	if err != nil || !strings.Contains(result.Logs, "built-job") {
		t.Fatalf("built job %+v: %v", result, err)
	}
}
