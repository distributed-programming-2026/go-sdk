// Package mockhttp runs real HTTP dependencies using ordinary Go handlers.
package mockhttp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
)

// New declares an HTTP server. Configure routing, responses and assertions in a
// standard http.Handler; no outgoing requests are intercepted or replaced.
func New(name string, handler http.Handler) integrationtests.ServerSpec {
	return integrationtests.ServerSpec{Name: name, New: func(_ context.Context, logs integrationtests.ServerLogger) (integrationtests.Server, error) {
		if handler == nil {
			return nil, errors.New("nil HTTP mock handler")
		}
		server := &http.Server{Handler: trace(handler, logs), ReadHeaderTimeout: 5 * time.Second, ErrorLog: log.New(logWriter{logs}, "", 0)}
		return &httpServer{Server: server}, nil
	}}
}

type httpServer struct{ *http.Server }

func (s *httpServer) Shutdown(ctx context.Context) error {
	err := s.Server.Shutdown(ctx)
	if err != nil {
		return errors.Join(err, s.Close())
	}
	return nil
}

type logWriter struct{ logs integrationtests.ServerLogger }

func (w logWriter) Write(value []byte) (int, error) {
	w.logs.Printf("%s", value)
	return len(value), nil
}

func trace(handler http.Handler, logs integrationtests.ServerLogger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		response := &recorder{ResponseWriter: w}
		completed := false
		defer func() {
			outcome := fmt.Sprint(response.status)
			if !completed {
				outcome = "panic"
			}
			logs.Printf("HTTP %s %s status=%s bytes=%d duration=%s", r.Method, r.URL.RequestURI(), outcome, response.bytes, time.Since(started))
		}()
		handler.ServeHTTP(response, r)
		if response.status == 0 {
			response.status = http.StatusOK
		}
		completed = true
	})
}
