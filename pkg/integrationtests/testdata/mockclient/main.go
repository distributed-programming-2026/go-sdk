// mockclient is a real containerized consumer used by Docker integration tests.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func run(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, os.Getenv("HTTP_MOCK")+"/ready", nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	var body struct {
		Status string `json:"status"`
	}
	err = json.NewDecoder(response.Body).Decode(&body)
	if closeErr := response.Body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK || body.Status != "ready" {
		return fmt.Errorf("HTTP fixture: status=%d body=%+v", response.StatusCode, body)
	}
	conn, err := grpc.NewClient(os.Getenv("GRPC_MOCK"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	health, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if health.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("gRPC fixture: %v", health.Status)
	}
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := run(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("mocks-ready")
	if len(os.Args) > 1 && os.Args[1] == "job" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
