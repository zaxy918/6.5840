// Configuration for slog package to use a custom minimalist log handler with a specific output format and log level filtering.
package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

var programStart time.Time

// SimpleHandler minimalist custom slog handler
type SimpleHandler struct {
	level slog.Level
	out   *os.File
	attrs []slog.Attr
}

// NewSimpleHandler creates a new SimpleHandler instance
func NewSimpleHandler(out *os.File, lvl slog.Level) *SimpleHandler {
	return &SimpleHandler{
		out:   out,
		level: lvl,
	}
}

// Enabled checks whether the log record should be output
func (h *SimpleHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level
}

// Handle formats and writes the log record
func (h *SimpleHandler) Handle(_ context.Context, r slog.Record) error {
	uptimeMs := time.Since(programStart).Milliseconds()

	var levelTag string
	switch r.Level {
	case slog.LevelDebug:
		levelTag = "[D]"
	case slog.LevelInfo:
		levelTag = "[I]"
	case slog.LevelWarn:
		levelTag = "[W]"
	case slog.LevelError:
		levelTag = "[E]"
	default:
		levelTag = "[?]"
	}

	var fields []string
	// Append static handler attributes
	for _, attr := range h.attrs {
		fields = append(fields, fmt.Sprintf("%s=%v", attr.Key, attr.Value.Any()))
	}
	// Append dynamic record attributes
	r.Attrs(func(a slog.Attr) bool {
		fields = append(fields, fmt.Sprintf("%s=%v", a.Key, a.Value.Any()))
		return true
	})

	if len(fields) > 0 {
		_, err := fmt.Fprintf(h.out, "[%dms] %s %s%c", uptimeMs, r.Message, strings.Join(fields, " "), 10)
		return err
	}
	_, err := fmt.Fprintf(h.out, "[%dms] %s %s\n", uptimeMs, levelTag, r.Message)
	return err
}

// WithAttrs returns a new Handler with additional attributes
func (h *SimpleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &SimpleHandler{
		level: h.level,
		out:   h.out,
		attrs: newAttrs,
	}
}

// WithGroup unused, comply with slog.Handler interface
func (h *SimpleHandler) WithGroup(_ string) slog.Handler {
	return h
}

func init() {
	programStart = time.Now()
	handler := NewSimpleHandler(os.Stdout, slog.LevelDebug)
	slog.SetDefault(slog.New(handler))
}
