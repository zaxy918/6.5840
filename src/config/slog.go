package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

// SimpleHandler custom minimalist log handler
type SimpleHandler struct {
	level slog.Level
	out   *os.File
}

// NewSimpleHandler create new SimpleHandler instance
func NewSimpleHandler(out *os.File, lvl slog.Level) *SimpleHandler {
	return &SimpleHandler{out: out, level: lvl}
}

// Enabled judge whether current log level should be printed
func (h *SimpleHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level
}

// Handle core log formatting and output logic
func (h *SimpleHandler) Handle(_ context.Context, r slog.Record) error {
	// 1. Short level tag
	var l string
	switch r.Level {
	case slog.LevelDebug:
		l = "[D]"
	case slog.LevelInfo:
		l = "[I]"
	case slog.LevelWarn:
		l = "[W]"
	case slog.LevelError:
		l = "[E]"
	}
	// 2. Short time format: HH:mm:ss
	t := r.Time.Format(time.TimeOnly)
	// 3. Concatenate all key-value attributes in a more readable layout
	fields := []string{}
	r.Attrs(func(a slog.Attr) bool {
		fields = append(fields, fmt.Sprintf("%s=%v", a.Key, a.Value.Any()))
		return true
	})
	// Custom output format: [Time][Level] Message | key=value | key=value
	if len(fields) > 0 {
		_, err := fmt.Fprintf(h.out, "%s %s %s | %s\n", t, l, r.Message, strings.Join(fields, " | "))
		return err
	}
	_, err := fmt.Fprintf(h.out, "%s %s %s\n", t, l, r.Message)
	return err
}

// WithAttrs implement slog.Handler interface
func (h *SimpleHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

// WithGroup implement slog.Handler interface
func (h *SimpleHandler) WithGroup(string) slog.Handler { return h }

// init set global default slog handler on package load
func init() {
	// Replace default handler with custom minimalist processor
	handler := NewSimpleHandler(os.Stdout, slog.LevelInfo)
	slog.SetDefault(slog.New(handler))
}
