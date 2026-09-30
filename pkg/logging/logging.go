// Package logging provides a consistently configured JSON slog logger and
// attributes for values that need a little more structure than slog's built-in
// attributes provide.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"
)

const (
	appIDKey      = "app_id"
	timestampKey  = "@timestamp"
	timeKey       = "time"
	errorKey      = "error"
	stackKey      = "stack"
	durationKey   = "duration"
	durationNSKey = "duration_ns"
	targetKey     = "target"
)

// Config configures a Logger. Output defaults to os.Stderr.
type Config struct {
	AppID     string
	Debug     bool
	Output    io.Writer
	AddSource bool
}

// New constructs a JSON logger. Every record contains app_id, @timestamp in
// Unix milliseconds, and time in RFC3339Nano format.
func New(config Config) *slog.Logger {
	output := config.Output
	if output == nil {
		output = os.Stderr
	}

	level := slog.LevelInfo
	if config.Debug {
		level = slog.LevelDebug
	}

	jsonHandler := slog.NewJSONHandler(output, &slog.HandlerOptions{
		AddSource: config.AddSource,
		Level:     level,
		// Time is supplied by handler.Handle in the two required formats.
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey && attr.Value.Kind() == slog.KindTime {
				return slog.Attr{}
			}
			return attr
		},
	})

	return slog.New(&handler{
		next:  jsonHandler,
		appID: config.AppID,
	})
}

// NewJSONLogger is a compatibility-friendly spelling of New.
func NewJSONLogger(config *Config) *slog.Logger {
	if config == nil {
		return New(Config{})
	}
	return New(*config)
}

// Error returns an attribute that renders err as a string. If err or any error
// contained in it carries a stack trace, the handler also emits a stack field.
func Error(err error) slog.Attr {
	return slog.Any(errorKey, errorAttr{err: err})
}

// DurationNs renders a duration as its integer number of nanoseconds.
func DurationNs(duration time.Duration) slog.Attr {
	return slog.Int64(durationNSKey, duration.Nanoseconds())
}

// Duration renders a duration in Go's human-readable duration format.
func Duration(duration time.Duration) slog.Attr {
	return slog.String(durationKey, duration.String())
}

// Target identifies the subsystem that emitted a log record.
func Target(target string) slog.Attr {
	return slog.String(targetKey, target)
}

type errorAttr struct {
	err error
}

type handler struct {
	next   slog.Handler
	appID  string
	attrs  []slog.Attr
	groups []string
}

func (h *handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *handler) Handle(ctx context.Context, record slog.Record) error {
	logRecord := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	logRecord.AddAttrs(
		slog.String(appIDKey, h.appID),
		slog.Int64(timestampKey, record.Time.UnixMilli()),
		slog.String(timeKey, record.Time.Format(time.RFC3339Nano)),
	)
	logRecord.AddAttrs(h.attrs...)
	var recordAttrs []slog.Attr
	record.Attrs(func(attr slog.Attr) bool {
		recordAttrs = append(recordAttrs, expandAttr(attr)...)
		return true
	})
	logRecord.AddAttrs(inGroups(recordAttrs, h.groups)...)
	return h.next.Handle(ctx, logRecord)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	expanded := make([]slog.Attr, 0, len(attrs))
	for _, attr := range attrs {
		expanded = append(expanded, expandAttr(attr)...)
	}
	return &handler{
		next:   h.next,
		appID:  h.appID,
		attrs:  appendAttrs(h.attrs, inGroups(expanded, h.groups)),
		groups: appendStrings(nil, h.groups),
	}
}

func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &handler{
		next:   h.next,
		appID:  h.appID,
		attrs:  appendAttrs(nil, h.attrs),
		groups: appendStrings(h.groups, []string{name}),
	}
}

func inGroups(attrs []slog.Attr, groups []string) []slog.Attr {
	if len(attrs) == 0 {
		return nil
	}
	for _, group := range slices.Backward(groups) {
		attrs = []slog.Attr{slog.Group(group, attrsToAny(attrs)...)}
	}
	return attrs
}

func attrsToAny(attrs []slog.Attr) []any {
	values := make([]any, len(attrs))
	for index := range attrs {
		values[index] = attrs[index]
	}
	return values
}

func appendAttrs(base, extra []slog.Attr) []slog.Attr {
	return append(append([]slog.Attr(nil), base...), extra...)
}

func appendStrings(base, extra []string) []string {
	return append(append([]string(nil), base...), extra...)
}

func expandAttr(attr slog.Attr) []slog.Attr {
	attr.Value = attr.Value.Resolve()
	if value, ok := attr.Value.Any().(errorAttr); ok {
		if value.err == nil {
			return []slog.Attr{slog.Any(attr.Key, nil)}
		}
		attrs := []slog.Attr{slog.String(attr.Key, value.err.Error())}
		if stack := errorStack(value.err); stack != "" {
			attrs = append(attrs, slog.String(stackKey, stack))
		}
		return attrs
	}
	if attr.Value.Kind() == slog.KindGroup {
		children := attr.Value.Group()
		expanded := make([]slog.Attr, 0, len(children))
		for _, child := range children {
			expanded = append(expanded, expandAttr(child)...)
		}
		attr.Value = slog.GroupValue(expanded...)
	}
	return []slog.Attr{attr}
}

func errorStack(err error) string {
	var stacks []string
	seenErrors := make(map[error]struct{})
	seenStacks := make(map[string]struct{})
	var visit func(error)
	visit = func(current error) {
		if current == nil {
			return
		}
		// Some error implementations are not comparable.
		value := reflect.ValueOf(current)
		if value.IsValid() && value.Type().Comparable() {
			if _, exists := seenErrors[current]; exists {
				return
			}
			seenErrors[current] = struct{}{}
		}
		if stack := ownStack(current); stack != "" {
			if _, exists := seenStacks[stack]; !exists {
				seenStacks[stack] = struct{}{}
				stacks = append(stacks, stack)
			}
		}
		switch wrapped := current.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return strings.Join(stacks, "\n\n")
}

func ownStack(err error) string {
	// github.com/pkg/errors and compatible implementations expose their stack
	// through the %+v formatter.
	verbose := strings.TrimSpace(fmt.Sprintf("%+v", err))
	plain := strings.TrimSpace(err.Error())
	if verbose != plain && strings.Contains(verbose, "\n") {
		return verbose
	}

	// Also support errors whose StackTrace return type is library-specific.
	method := reflect.ValueOf(err).MethodByName("StackTrace")
	if method.IsValid() && method.Type().NumIn() == 0 && method.Type().NumOut() == 1 {
		result := method.Call(nil)[0]
		if result.IsValid() {
			stack := strings.TrimSpace(fmt.Sprintf("%+v", result.Interface()))
			if stack != "" {
				return stack
			}
		}
	}
	return ""
}
