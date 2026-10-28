package mockhttp_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests/mockhttp"
	"github.com/stretchr/testify/require"
)

func start(t *testing.T, handler http.Handler) (environment *integrationtests.Environment, url string) {
	t.Helper()
	env := integrationtests.New(integrationtests.Config{Servers: []integrationtests.ServerSpec{mockhttp.New("billing", handler)}})
	require.NoError(t, env.Prepare(t.Context()))
	t.Cleanup(func() { require.NoError(t, env.Down(context.Background())) })
	require.NoError(t, env.Up(t.Context()))
	address, err := env.HostEndpoint(t.Context(), "billing")
	require.NoError(t, err)
	return env, "http://" + address
}

func TestHTTPMockUsesRealRequestsAndCanChangeResponses(t *testing.T) {
	var calls atomic.Int32
	var unavailable atomic.Bool
	env, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if unavailable.Load() {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{"method": r.Method, "path": r.URL.Path}); err != nil {
			t.Error(err)
		}
	}))
	ctx := env.Bind(t)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/charge", strings.NewReader(`{"amount":42}`))
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	var body map[string]string
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "POST", body["method"])
	require.Equal(t, "/charge", body["path"])
	unavailable.Store(true)
	request, err = http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	require.NoError(t, err)
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	require.EqualValues(t, 2, calls.Load())
	require.NoError(t, env.Down(context.Background()))
	client := &http.Client{Timeout: time.Second}
	response, err = client.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	require.Error(t, err)
}

func TestHTTPMockSupportsConcurrentRequests(t *testing.T) {
	var calls atomic.Int32
	env, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(http.StatusNoContent) }))
	ctx := env.Bind(t)
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
			if err != nil {
				t.Error(err)
				return
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := io.Copy(io.Discard, response.Body); err != nil {
				t.Error(err)
			}
			if err := response.Body.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	require.EqualValues(t, 16, calls.Load())
}

func TestHTTPMockPreservesStreaming(t *testing.T) {
	_, url := start(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := io.WriteString(w, "chunk"); err != nil {
			t.Error(err)
			return
		}
		requireFlusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("missing Flusher")
			return
		}
		requireFlusher.Flush()
	}))
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "chunk", string(body))
}
