package runtime

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type logChunk struct {
	at   time.Time
	text string
}

type logBuffer struct {
	mu        sync.Mutex
	limit     int
	size      int
	droppedAt time.Time
	chunks    []logChunk
}

func (b *logBuffer) Accept(log testcontainers.Log) {
	b.write(time.Now(), "["+log.LogType+"] "+string(log.Content))
}

func (b *logBuffer) Printf(format string, args ...any) {
	b.write(time.Now(), fmt.Sprintf(format, args...)+"\n")
}

func (b *logBuffer) write(at time.Time, value string) {
	if value == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit <= 0 {
		return
	}
	if len(value) > b.limit {
		value = strings.Clone(value[len(value)-b.limit:])
		b.droppedAt = at
	}
	b.chunks = append(b.chunks, logChunk{at, value})
	b.size += len(value)
	for b.size > b.limit {
		extra := b.size - b.limit
		first := &b.chunks[0]
		if first.at.After(b.droppedAt) {
			b.droppedAt = first.at
		}
		if len(first.text) <= extra {
			b.size -= len(first.text)
			b.chunks[0] = logChunk{}
			b.chunks = b.chunks[1:]
		} else {
			first.text = first.text[extra:]
			b.size -= extra
		}
	}
}

func (b *logBuffer) text(since time.Time) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var result strings.Builder
	if !b.droppedAt.IsZero() && !b.droppedAt.Before(since) {
		result.WriteString("[log buffer truncated]\n")
	}
	for _, chunk := range b.chunks {
		if !chunk.at.Before(since) {
			result.WriteString(chunk.text)
		}
	}
	return result.String()
}

func (b *logBuffer) Write(value []byte) (int, error) {
	b.write(time.Now(), string(value))
	return len(value), nil
}

// replace uses an authoritative final snapshot after a job's log producer stops.
func (b *logBuffer) replace(snapshot *logBuffer) {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.chunks = snapshot.chunks
	b.size = snapshot.size
	b.droppedAt = snapshot.droppedAt
}
