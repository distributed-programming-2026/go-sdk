package mockgrpc_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests/mockgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type healthMock struct {
	healthpb.UnimplementedHealthServer
	calls atomic.Int32
}

func (m *healthMock) Check(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	m.calls.Add(1)
	switch request.Service {
	case "unavailable":
		return nil, status.Error(codes.Unavailable, "try again")
	case "panic":
		panic("broken fixture")
	case "slow":
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	default:
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	}
}

func connection(t *testing.T, spec integrationtests.ServerSpec) (*integrationtests.Environment, *grpc.ClientConn) {
	t.Helper()
	env := integrationtests.New(integrationtests.Config{Servers: []integrationtests.ServerSpec{spec}})
	require.NoError(t, env.Prepare(t.Context()))
	t.Cleanup(func() { require.NoError(t, env.Down(context.Background())) })
	require.NoError(t, env.Up(t.Context()))
	address, err := env.HostEndpoint(t.Context(), spec.Name)
	require.NoError(t, err)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return env, conn
}

func TestGRPCMockResponsesErrorsDeadlineAndPanic(t *testing.T) {
	mock := &healthMock{}
	var intercepted atomic.Int32
	spec := mockgrpc.New("catalog", func(s *grpc.Server) { healthpb.RegisterHealthServer(s, mock) }, grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		intercepted.Add(1)
		return next(ctx, request)
	}))
	env, conn := connection(t, spec)
	client := healthpb.NewHealthClient(conn)
	ctx := env.Bind(t)
	response, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "unavailable"})
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "panic"})
	require.Equal(t, codes.Internal, status.Code(err))
	deadline, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = client.Check(deadline, &healthpb.HealthCheckRequest{Service: "slow"})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.EqualValues(t, 4, mock.calls.Load())
	require.EqualValues(t, 4, intercepted.Load())
	require.NoError(t, env.Down(context.Background()))
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.Error(t, err)
}

func TestGRPCMockStreaming(t *testing.T) {
	mock := health.NewServer()
	mock.SetServingStatus("catalog", healthpb.HealthCheckResponse_SERVING)
	env, conn := connection(t, mockgrpc.New("catalog", func(s *grpc.Server) { healthpb.RegisterHealthServer(s, mock) }))
	ctx, cancel := context.WithCancel(env.Bind(t))
	defer cancel()
	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{Service: "catalog"})
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, first.Status)
	mock.SetServingStatus("catalog", healthpb.HealthCheckResponse_NOT_SERVING)
	second, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, second.Status)
	cancel()
	_, err = stream.Recv()
	require.Equal(t, codes.Canceled, status.Code(err))
}
