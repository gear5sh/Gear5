package logger

import "strings"

type Level int

const (
	unknown Level = iota
	debug
	info
	warn
	errorlevel
	fatal
)

func (l Level) String() string {
	switch l {
	case unknown:
		return "unknown"
	case debug:
		return "debug"
	case info:
		return "info"
	case warn:
		return "warn"
	case errorlevel:
		return "error"
	case fatal:
		return "fatal"
	default:
		return ""
	}
}

func ToLevel(levelStr string) Level {
	switch strings.TrimSpace(strings.ToLower(levelStr)) {
	case "debug":
		return debug
	case "info":
		return info
	case "warn":
		return warn
	case "error":
		return errorlevel
	case "fatal":
		return fatal
	default:
		return unknown
	}
}
