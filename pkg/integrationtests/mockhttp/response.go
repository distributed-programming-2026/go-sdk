package mockhttp

import (
	"bufio"
	"net"
	"net/http"
)

type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *recorder) WriteHeader(code int) {
	if r.status != 0 {
		return
	}
	if code >= 200 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}
func (r *recorder) Write(value []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	count, err := r.ResponseWriter.Write(value)
	r.bytes += count
	return count, err
}

// Unwrap preserves ResponseController access to optional writer capabilities.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
func (r *recorder) Flush() {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(r.ResponseWriter).Flush()
}
func (r *recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(r.ResponseWriter).Hijack()
}
func (r *recorder) Push(target string, options *http.PushOptions) error {
	if pusher, ok := r.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, options)
	}
	return http.ErrNotSupported
}
