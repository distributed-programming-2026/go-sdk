// Package mockgrpc runs real gRPC dependencies using registered Go services.
package mockgrpc

import (
	"context"
	"errors"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// New declares a gRPC server. Register generated protobuf service implementations
// in register. Options support credentials, codecs and user interceptors.
func New(name string, register func(*grpc.Server), options ...grpc.ServerOption) integrationtests.ServerSpec {
	options = append([]grpc.ServerOption(nil), options...)
	return integrationtests.ServerSpec{Name: name, New: func(_ context.Context, logs integrationtests.ServerLogger) (integrationtests.Server, error) {
		if register == nil {
			return nil, errors.New("nil gRPC mock registration")
		}
		opts := append([]grpc.ServerOption(nil), options...)
		opts = append(opts, grpc.ChainUnaryInterceptor(unaryTrace(logs)), grpc.ChainStreamInterceptor(streamTrace(logs)))
		server := grpc.NewServer(opts...)
		// Stop partially initialized servers if registration panics.
		completed := false
		defer func() {
			if !completed {
				server.Stop()
			}
		}()
		register(server)
		completed = true
		return &grpcServer{Server: server}, nil
	}}
}

type grpcServer struct{ *grpc.Server }

func (s *grpcServer) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() { s.GracefulStop(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.Stop()
		return context.Cause(ctx)
	}
}

func unaryTrace(logs integrationtests.ServerLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (response any, err error) {
		started := time.Now()
		defer func() {
			if r := recover(); r != nil {
				err = status.Errorf(codes.Internal, "mock handler panic: %v", r)
			}
			logs.Printf("gRPC %s code=%s duration=%s", info.FullMethod, status.Code(err), time.Since(started))
		}()
		return next(ctx, request)
	}
}

func streamTrace(logs integrationtests.ServerLogger) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) (err error) {
		started := time.Now()
		defer func() {
			if r := recover(); r != nil {
				err = status.Errorf(codes.Internal, "mock stream panic: %v", r)
			}
			logs.Printf("gRPC stream %s code=%s duration=%s", info.FullMethod, status.Code(err), time.Since(started))
		}()
		return next(server, stream)
	}
}
