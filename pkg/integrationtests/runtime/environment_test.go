package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
)

const (
	jobName       = "job"
	removeNetwork = "remove network"
)

type fakeContainer struct {
	testcontainers.Container
	mu           sync.Mutex
	running      bool
	job          bool
	code         int
	removed      int
	terminateErr error
	record       func(string)
	name         string
}

func (f *fakeContainer) GetContainerID() string { return f.name }
func (f *fakeContainer) Start(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = !f.job
	return nil
}
func (f *fakeContainer) State(context.Context) (*dockercontainer.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &dockercontainer.State{Running: f.running, ExitCode: f.code}, nil
}
func (f *fakeContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed++
	f.running = false
	if f.record != nil {
		f.record("remove " + f.name)
	}
	return f.terminateErr
}
func (f *fakeContainer) Logs(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("output from " + f.name)), nil
}
func (*fakeContainer) Host(context.Context) (string, error) { return "::1", nil }
func (*fakeContainer) MappedPort(context.Context, string) (dockernetwork.Port, error) {
	return dockernetwork.MustParsePort("49153/tcp"), nil
}

type fakeDriver struct {
	mu         sync.Mutex
	events     []string
	requests   []testcontainers.ContainerRequest
	containers []*fakeContainer
	createHook func()
	createErr  error
	prepareErr error
}

func (d *fakeDriver) addEvent(event string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, event)
}
func (d *fakeDriver) prepare(_ context.Context, r testcontainers.ContainerRequest, _ *logBuffer) (testcontainers.ContainerRequest, error) {
	d.addEvent("prepare " + r.Image)
	return r, d.prepareErr
}
func (d *fakeDriver) network(context.Context) (networkResource, error) {
	d.addEvent("network")
	return networkResource{"test-network", func(context.Context) error { d.addEvent(removeNetwork); return nil }}, nil
}
func (d *fakeDriver) create(ctx context.Context, r testcontainers.ContainerRequest, logs *logBuffer) (testcontainers.Container, error) {
	if d.createHook != nil {
		d.createHook()
	}
	d.mu.Lock()
	d.requests = append(d.requests, cloneRequest(r))
	d.events = append(d.events, "create "+r.Image)
	logs.Printf("output from %s", r.Image)
	c := &fakeContainer{name: r.Image, job: r.Image == jobName, record: d.addEvent}
	d.containers = append(d.containers, c)
	d.mu.Unlock()
	for _, hooks := range r.LifecycleHooks {
		for _, hook := range hooks.PostCreates {
			if err := hook(ctx, c); err != nil {
				return c, err
			}
		}
	}
	return c, d.createErr
}

func testEnvironment(config Config) (*Environment, *fakeDriver) {
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	env := New(config)
	driver := &fakeDriver{}
	env.driver = driver
	return env, driver
}
func service(name string, deps ...string) ContainerSpec {
	return ContainerSpec{Name: name, DependsOn: deps, Request: testcontainers.ContainerRequest{Image: name}}
}
func startEnvironment(t *testing.T, env *Environment) {
	t.Helper()
	if err := env.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := env.Up(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Down(context.Background()); err != nil {
			t.Error(err)
		}
	})
}

func TestDependencyLifecycleAndEndpoints(t *testing.T) {
	mysql := service("mysql")
	api := service("api", "mysql")
	env, driver := testEnvironment(Config{Containers: []ContainerSpec{api, mysql}})
	mysql.AfterReady = func(context.Context, *Environment, testcontainers.Container) error {
		driver.addEvent("init mysql")
		return nil
	}
	env.config.Containers[1] = mysql
	startEnvironment(t, env)
	got, err := env.HostEndpoint(t.Context(), "mysql", "3306/tcp")
	if err != nil || got != "[::1]:49153" {
		t.Fatalf("endpoint %q: %v", got, err)
	}
	got, err = env.NetworkEndpoint("mysql", "3306/tcp")
	if err != nil || got != "mysql:3306" {
		t.Fatalf("endpoint %q: %v", got, err)
	}
	if _, err := env.NetworkEndpoint("missing", "3306"); err == nil {
		t.Fatal("missing service accepted")
	}
	if _, err := env.NetworkEndpoint("mysql", "99999"); err == nil {
		t.Fatal("invalid port accepted")
	}
	if err := env.AddCleanup("client", func(context.Context) error { driver.addEvent("close client"); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := env.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	expected := []string{"prepare mysql", "prepare api", "network", "create mysql", "init mysql", "create api", "close client", "remove api", "remove mysql", removeNetwork}
	if !reflect.DeepEqual(driver.events, expected) {
		t.Fatalf("events: %v", driver.events)
	}
	if err := env.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(driver.events, expected) {
		t.Fatal("cleanup repeated")
	}
}

func TestInvalidGraphDoesNotContactDocker(t *testing.T) {
	tests := []struct {
		name  string
		specs []ContainerSpec
	}{
		{"duplicate", []ContainerSpec{service("a"), service("a")}},
		{"unknown", []ContainerSpec{service("a", "b")}},
		{"cycle", []ContainerSpec{service("a", "b"), service("b", "a")}},
		{"manual dependency", []ContainerSpec{service("a", jobName), {Name: jobName, Manual: true, Request: testcontainers.ContainerRequest{Image: jobName}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, driver := testEnvironment(Config{Containers: tt.specs})
			if err := env.Prepare(t.Context()); err == nil {
				t.Fatal("invalid graph accepted")
			}
			if len(driver.events) != 0 {
				t.Fatalf("contacted driver: %v", driver.events)
			}
		})
	}
}

func TestJobsUseFreshRequestsAndAreNotAutomaticallyStarted(t *testing.T) {
	job := service(jobName, "mysql")
	job.Manual = true
	job.Request.Env = map[string]string{"BASE": "original"}
	job.Request.Cmd = []string{"default"}
	job.Configure = func(_ context.Context, env *Environment, r *testcontainers.ContainerRequest) error {
		endpoint, err := env.NetworkEndpoint("mysql", "3306/tcp")
		r.Env["MYSQL"] = endpoint
		return err
	}
	env, driver := testEnvironment(Config{Containers: []ContainerSpec{service("mysql"), job}})
	startEnvironment(t, env)
	if len(driver.containers) != 1 {
		t.Fatal("job started in Up")
	}
	for i := 0; i < 2; i++ {
		result, err := env.RunJob(t.Context(), jobName, WithCommand("run"), WithEnv(map[string]string{"BASE": "override"}))
		if err != nil || result.ExitCode != 0 || result.ID == "" || !strings.Contains(result.Logs, "output from job") {
			t.Fatalf("result %+v: %v", result, err)
		}
	}
	if job.Request.Env["BASE"] != "original" || job.Request.Cmd[0] != "default" {
		t.Fatal("template mutated")
	}
	for _, r := range driver.requests[1:] {
		if r.Env["MYSQL"] != "mysql:3306" || r.Env["BASE"] != "override" {
			t.Fatalf("request %+v", r)
		}
	}
	for _, c := range driver.containers[1:] {
		if c.removed != 1 {
			t.Fatal("job was not removed")
		}
	}
}

func TestPartialCreationAndCleanupErrors(t *testing.T) {
	env, driver := testEnvironment(Config{Containers: []ContainerSpec{service("mysql")}})
	driver.createErr = errors.New("create hook failed")
	if err := env.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := env.Up(t.Context()); err == nil {
		t.Fatal("expected startup failure")
	}
	driver.containers[0].terminateErr = errors.New("remove failed")
	clientClosed := false
	if err := env.AddCleanup("client", func(context.Context) error { clientClosed = true; return errors.New("close failed") }); err != nil {
		t.Fatal(err)
	}
	err := env.Down(context.Background())
	if err == nil || !strings.Contains(err.Error(), "remove failed") || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("cleanup: %v", err)
	}
	if !clientClosed || driver.events[len(driver.events)-1] != removeNetwork {
		t.Fatalf("cleanup incomplete: %v", driver.events)
	}
}

func TestLateCreationIsRemoved(t *testing.T) {
	env, driver := testEnvironment(Config{Containers: []ContainerSpec{service("mysql")}})
	created, release := make(chan struct{}), make(chan struct{})
	driver.createHook = func() { close(created); <-release }
	if err := env.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	upDone := make(chan error, 1)
	go func() { upDone <- env.Up(context.Background()) }()
	<-created
	downDone := make(chan error, 1)
	go func() { downDone <- env.Down(context.Background()) }()
	<-env.Context().Done()
	close(release)
	if err := <-upDone; err == nil {
		t.Fatal("Up succeeded during shutdown")
	}
	if err := <-downDone; err != nil {
		t.Fatal(err)
	}
	if driver.containers[0].removed != 1 {
		t.Fatalf("late resources: %v", driver.events)
	}
}

func TestBindIsIdempotentAndContextIsCanceled(t *testing.T) {
	env, _ := testEnvironment(Config{})
	var ctx context.Context
	t.Run("bound", func(t *testing.T) {
		ctx = env.Bind(t)
		if ctx != env.Bind(t) {
			t.Fatal("duplicate binding")
		}
	})
	if ctx.Err() == nil {
		t.Fatal("test completion did not cancel context")
	}
	env.Abort(errors.New("after test")) // Must not call Fail on the completed test.
}

func TestPhaseTimeoutAndPanic(t *testing.T) {
	env, _ := testEnvironment(Config{})
	err := env.Phase(t.Context(), "timeout", 10*time.Millisecond, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	err = env.Phase(t.Context(), "panic", time.Second, func(context.Context) error { panic("broken hook") })
	if err == nil || !strings.Contains(err.Error(), "broken hook") {
		t.Fatalf("panic: %v", err)
	}
}

func TestLogBufferIntervalsAndTruncation(t *testing.T) {
	buffer := &logBuffer{limit: 8}
	before := time.Now()
	after := before.Add(time.Second)
	buffer.write(before, "old")
	buffer.write(after, "0123456789")
	got := buffer.text(before)
	if !strings.Contains(got, "truncated") || !strings.HasSuffix(got, "23456789") {
		t.Fatalf("buffer: %q", got)
	}
	if got := buffer.text(after.Add(time.Second)); got != "" {
		t.Fatalf("old output leaked: %q", got)
	}
}

func TestUnexpectedServiceExitAbortsEnvironment(t *testing.T) {
	env, driver := testEnvironment(Config{Containers: []ContainerSpec{service("mysql")}})
	startEnvironment(t, env)
	c := driver.containers[0]
	c.mu.Lock()
	c.running = false
	c.mu.Unlock()
	select {
	case <-env.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("service exit not detected")
	}
	if !strings.Contains(context.Cause(env.Context()).Error(), "stopped unexpectedly") {
		t.Fatal(context.Cause(env.Context()))
	}
}

func TestCleanupRemovesKnownResourcesBeforeWaitingForHook(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	spec := service("mysql")
	spec.AfterReady = func(context.Context, *Environment, testcontainers.Container) error {
		close(entered)
		<-release
		return nil
	}
	env, driver := testEnvironment(Config{Containers: []ContainerSpec{spec}})
	if err := env.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	upDone := make(chan error, 1)
	go func() { upDone <- env.Up(context.Background()) }()
	<-entered
	removed := make(chan struct{})
	env.mu.Lock()
	original := env.network.remove
	env.network.remove = func(ctx context.Context) error { err := original(ctx); close(removed); return err }
	env.mu.Unlock()
	downDone := make(chan error, 1)
	go func() { downDone <- env.Down(context.Background()) }()
	select {
	case <-removed:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("cleanup waited for uncooperative hook before removing resources")
	}
	c := driver.containers[0]
	c.mu.Lock()
	count := c.removed
	c.mu.Unlock()
	if count != 1 {
		t.Error("known container not removed")
	}
	close(release)
	<-upDone
	if err := <-downDone; err != nil {
		t.Fatal(err)
	}
}

func TestPostCreatePanicStillTracksContainer(t *testing.T) {
	spec := service("mysql")
	spec.Request.LifecycleHooks = []testcontainers.ContainerLifecycleHooks{{PostCreates: []testcontainers.ContainerHook{
		func(context.Context, testcontainers.Container) error { panic("post-create panic") },
	}}}
	env, driver := testEnvironment(Config{Containers: []ContainerSpec{spec}})
	if err := env.Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := env.Up(t.Context()); err == nil || !strings.Contains(err.Error(), "post-create panic") {
		t.Fatalf("panic: %v", err)
	}
	if err := env.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.containers[0].removed != 1 {
		t.Fatal("partially-created container leaked")
	}
}

func TestJobHookPanicReturnsErrorAndRemovesContainer(t *testing.T) {
	spec := service(jobName)
	spec.Manual = true
	spec.AfterReady = func(context.Context, *Environment, testcontainers.Container) error { panic("job hook failed") }
	env, driver := testEnvironment(Config{Containers: []ContainerSpec{spec}})
	startEnvironment(t, env)
	if _, err := env.RunJob(t.Context(), jobName); err == nil || !strings.Contains(err.Error(), "job hook failed") {
		t.Fatalf("job hook: %v", err)
	}
	if driver.containers[0].removed != 1 {
		t.Fatal("failed job leaked")
	}
	if env.Context().Err() != nil {
		t.Fatal("job hook panic aborted environment")
	}
}
