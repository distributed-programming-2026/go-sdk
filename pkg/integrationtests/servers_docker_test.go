package integrationtests_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests/mockgrpc"
	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests/mockhttp"
	dockerclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func buildMockConsumer(t *testing.T) string {
	t.Helper()
	client, err := testcontainers.NewDockerClientWithOpts(t.Context())
	require.NoError(t, err)
	defer client.Close()
	info, err := client.Info(t.Context(), dockerclient.InfoOptions{})
	require.NoError(t, err)
	architecture := info.Info.Architecture
	switch architecture {
	case "aarch64":
		architecture = "arm64"
	case "x86_64":
		architecture = "amd64"
	}
	directory := t.TempDir()
	// #nosec G204 G702 -- fixed Go compiler command; output is inside t.TempDir.
	command := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", filepath.Join(directory, "client"), "./testdata/mockclient")
	command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+architecture)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "Dockerfile"), []byte("FROM scratch\nCOPY client /client\nENTRYPOINT [\"/client\"]\n"), 0o600))
	return directory
}

func TestDockerConsumersReachHTTPAndGRPCMocksDuringStartup(t *testing.T) {
	if os.Getenv("INTEGRATIONTESTS_DOCKER") != "1" {
		t.Skip("set INTEGRATIONTESTS_DOCKER=1 to run Docker checks")
	}
	directory := buildMockConsumer(t)
	tagSuffix := fmt.Sprint(time.Now().UnixNano())
	var httpCalls, grpcCalls atomic.Int32
	httpMock := mockhttp.New("billing", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"status":"ready"}`)); err != nil {
			t.Error(err)
		}
	}))
	healthMock := health.NewServer()
	healthMock.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	grpcMock := mockgrpc.New("catalog", func(s *grpc.Server) { healthpb.RegisterHealthServer(s, healthMock) }, grpc.ChainUnaryInterceptor(func(ctx context.Context, r any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		grpcCalls.Add(1)
		return next(ctx, r)
	}))
	consumer := func(name string, manual bool) integrationtests.ContainerSpec {
		return integrationtests.ContainerSpec{Name: name, Manual: manual, DependsOn: []string{"billing", "catalog"},
			Request: testcontainers.ContainerRequest{FromDockerfile: testcontainers.FromDockerfile{Context: directory, Repo: "integrationtests-mock-consumer", Tag: name + tagSuffix}},
			Configure: func(_ context.Context, env *integrationtests.Environment, r *testcontainers.ContainerRequest) error {
				httpAddress, err := env.NetworkEndpoint("billing")
				if err != nil {
					return err
				}
				grpcAddress, err := env.NetworkEndpoint("catalog")
				if err != nil {
					return err
				}
				r.Env = map[string]string{"HTTP_MOCK": "http://" + httpAddress, "GRPC_MOCK": grpcAddress}
				if manual {
					r.Cmd = []string{"job"}
				} else {
					r.WaitingFor = wait.ForLog("mocks-ready")
				}
				return nil
			},
		}
	}
	env := integrationtests.New(integrationtests.Config{Servers: []integrationtests.ServerSpec{httpMock, grpcMock}, Containers: []integrationtests.ContainerSpec{consumer("api", false), consumer("job", true)}})
	t.Cleanup(func() { require.NoError(t, env.Down(context.Background())) })
	require.NoError(t, env.Prepare(t.Context()))
	// Prepared images are cache by default; this fixture explicitly removes them.
	t.Cleanup(func() {
		require.NoError(t, env.Down(context.Background()))
		client, err := testcontainers.NewDockerClientWithOpts(context.Background())
		require.NoError(t, err)
		defer client.Close()
		for _, tag := range []string{"api", "job"} {
			_, err := client.ImageRemove(context.Background(), "integrationtests-mock-consumer:"+tag+tagSuffix, dockerclient.ImageRemoveOptions{})
			require.NoError(t, err)
		}
	})
	require.NoError(t, env.Up(t.Context()))
	ctx := env.Bind(t)
	for range 2 {
		result, err := env.RunJob(ctx, "job")
		require.NoError(t, err, "%s", result.Logs)
		require.Contains(t, result.Logs, "mocks-ready")
	}
	require.EqualValues(t, 3, httpCalls.Load())
	require.EqualValues(t, 3, grpcCalls.Load())
	require.NoError(t, env.Down(context.Background()))
}
