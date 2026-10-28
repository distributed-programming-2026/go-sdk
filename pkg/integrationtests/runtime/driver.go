package runtime

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"

	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
)

type networkResource struct {
	name   string
	remove func(context.Context) error
}

type driver interface {
	prepare(context.Context, testcontainers.ContainerRequest, *logBuffer) (testcontainers.ContainerRequest, error)
	network(context.Context) (networkResource, error)
	create(context.Context, testcontainers.ContainerRequest, *logBuffer) (testcontainers.Container, error)
}

type dockerDriver struct{}

func (dockerDriver) prepare(ctx context.Context, request testcontainers.ContainerRequest, logs *logBuffer) (testcontainers.ContainerRequest, error) {
	provider, err := testcontainers.NewDockerProvider(testcontainers.WithLogger(logs))
	if err != nil {
		return request, err
	}
	defer provider.Close() // Provider clients only own idle connections.
	if request.ShouldBuildImage() {
		request, err = buildImage(ctx, provider, request, logs)
		if err != nil {
			return request, err
		}
	} else if err := pullImage(ctx, provider, request); err != nil {
		return request, err
	}

	request.AlwaysPullImage = false
	return request, nil
}

func buildImage(ctx context.Context, provider *testcontainers.DockerProvider, request testcontainers.ContainerRequest, logs *logBuffer) (testcontainers.ContainerRequest, error) {
	for _, hooks := range request.LifecycleHooks {
		for _, hook := range hooks.PreBuilds {
			if err := hook(ctx, request); err != nil {
				return request, err
			}
		}
	}
	request.FromDockerfile.BuildLogWriter = logs
	request.KeepImage = true
	image, err := provider.BuildImage(ctx, &request)
	if err != nil {
		return request, err
	}
	request.Image = image
	for _, hooks := range request.LifecycleHooks {
		for _, hook := range hooks.PostBuilds {
			if err := hook(ctx, request); err != nil {
				return request, err
			}
		}
	}
	// Prepared images are reusable Docker cache, not owned by a job instance.
	request.FromDockerfile = testcontainers.FromDockerfile{}
	return request, nil
}

func pullImage(ctx context.Context, provider *testcontainers.DockerProvider, request testcontainers.ContainerRequest) error {
	client, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	_, inspectErr := client.ImageInspect(ctx, request.Image)
	if inspectErr != nil && !errdefs.IsNotFound(inspectErr) {
		return inspectErr
	}
	if inspectErr != nil || request.AlwaysPullImage || request.ImagePlatform != "" {
		var options []testcontainers.PullImageOption
		if request.ImagePlatform != "" {
			platform, err := platforms.Parse(request.ImagePlatform)
			if err != nil {
				return err
			}
			options = append(options, testcontainers.PullDockerImageWithPlatform(platform))
		}
		if err := provider.PullImageWithOpts(ctx, request.Image, options...); err != nil {
			return fmt.Errorf("pull %s: %w", request.Image, err)
		}
	}
	return nil
}

func (dockerDriver) network(ctx context.Context) (networkResource, error) {
	n, err := tcnetwork.New(ctx)
	if err != nil {
		return networkResource{}, err
	}
	return networkResource{name: n.Name, remove: n.Remove}, nil
}

func (dockerDriver) create(ctx context.Context, request testcontainers.ContainerRequest, logs *logBuffer) (testcontainers.Container, error) {
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: request, Logger: logs})
}
