package runtime

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

type localServer struct {
	serve    func(net.Listener) error
	shutdown func(context.Context) error
}

func (s *localServer) Serve(listener net.Listener) error  { return s.serve(listener) }
func (s *localServer) Shutdown(ctx context.Context) error { return s.shutdown(ctx) }

func httpSpec(name string) ServerSpec {
	return ServerSpec{Name: name, New: func(context.Context, ServerLogger) (Server, error) {
		return &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })}, nil
	}}
}

func (*fakeContainer) Inspect(context.Context) (*dockercontainer.InspectResponse, error) {
	return &dockercontainer.InspectResponse{HostConfig: &dockercontainer.HostConfig{ExtraHosts: []string{testcontainers.HostInternal + ":172.20.0.2"}}}, nil
}

func TestServerDependencyGatewayAndCleanupOrder(t *testing.T) {
	var hostAddress string
	api := service("api", "billing")
	api.Configure = func(ctx context.Context, env *Environment, r *testcontainers.ContainerRequest) error {
		var err error
		hostAddress, err = env.HostEndpoint(ctx, "billing")
		if err != nil {
			return err
		}
		networkAddress, err := env.NetworkEndpoint("billing")
		if err != nil {
			return err
		}
		if !strings.HasPrefix(networkAddress, serverGatewayHost+":") {
			return errors.New("wrong server gateway endpoint")
		}
		r.Env = map[string]string{"BILLING_URL": "http://" + networkAddress}
		return nil
	}
	env, driver := testEnvironment(Config{Servers: []ServerSpec{httpSpec("billing")}, Containers: []ContainerSpec{api}})
	startEnvironment(t, env)
	if len(driver.containers) != 2 {
		t.Fatalf("expected one shared gateway and one API, got %d", len(driver.containers))
	}
	config := &dockercontainer.HostConfig{}
	driver.requests[1].HostConfigModifier(config)
	if len(config.ExtraHosts) != 1 || config.ExtraHosts[0] != serverGatewayHost+":172.20.0.2" {
		t.Fatalf("gateway mapping: %v", config.ExtraHosts)
	}
	// The mock must still accept requests while its client container terminates.
	alive := false
	driver.containers[1].record = func(event string) {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+hostAddress, http.NoBody)
		if err == nil {
			response, requestErr := http.DefaultClient.Do(request)
			if requestErr == nil {
				alive = response.StatusCode == http.StatusNoContent
				_ = response.Body.Close()
			}
		}
		driver.addEvent(event)
	}
	if err := env.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !alive {
		t.Fatal("mock closed before dependent container")
	}
	if _, err := env.HostEndpoint(t.Context(), "billing"); err == nil {
		t.Fatal("closed mock resolved")
	}
}

func TestServerValidationBeforeDocker(t *testing.T) {
	cases := []Config{
		{Servers: []ServerSpec{httpSpec("same"), httpSpec("same")}},
		{Servers: []ServerSpec{httpSpec("same")}, Containers: []ContainerSpec{service("same")}},
		{Servers: []ServerSpec{{Name: "bad"}}},
		{Servers: []ServerSpec{httpSpec("billing")}, Containers: []ContainerSpec{service("api", "unknown")}},
	}
	for _, config := range cases {
		env, driver := testEnvironment(config)
		if err := env.Prepare(t.Context()); err == nil {
			t.Fatal("invalid server graph accepted")
		}
		if len(driver.events) != 0 {
			t.Fatal("invalid graph contacted Docker")
		}
	}
}

func TestUnexpectedServerExitCancelsEnvironment(t *testing.T) {
	env, _ := testEnvironment(Config{Servers: []ServerSpec{{Name: "broken", New: func(context.Context, ServerLogger) (Server, error) {
		return &localServer{serve: func(net.Listener) error { return errors.New("listener broken") }, shutdown: func(context.Context) error { return nil }}, nil
	}}}})
	if err := env.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	_ = env.Up(t.Context())
	select {
	case <-env.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("server exit ignored")
	}
	if !strings.Contains(context.Cause(env.Context()).Error(), "listener broken") {
		t.Fatal(context.Cause(env.Context()))
	}
	if err := env.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLateServerFactoryIsClosed(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var closed atomic.Int32
	env, _ := testEnvironment(Config{Servers: []ServerSpec{{Name: "late", New: func(context.Context, ServerLogger) (Server, error) {
		close(entered)
		<-release
		return &localServer{serve: func(net.Listener) error { t.Error("late server started"); return nil }, shutdown: func(context.Context) error { closed.Add(1); return nil }}, nil
	}}}})
	if err := env.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	upDone := make(chan error, 1)
	go func() { upDone <- env.Up(context.Background()) }()
	<-entered
	downDone := make(chan error, 1)
	go func() { downDone <- env.Down(context.Background()) }()
	<-env.Context().Done()
	close(release)
	<-upDone
	if err := <-downDone; err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatal("late server leaked")
	}
}
