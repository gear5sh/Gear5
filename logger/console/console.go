package console

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/piyushsingariya/shift/types"
)

var (
	writer      io.Writer
	errorWriter io.Writer
)

func SetupWriter(w io.Writer, err io.Writer) {
	writer = w
	errorWriter = err
}

type Level int

const (
	UNKNOWN Level = iota
	DEBUG
	INFO
	WARN
	ERROR
	FATAL
)

func (l Level) String() string {
	switch l {
	case UNKNOWN:
		return "unknown"
	case DEBUG:
		return "debug"
	case INFO:
		return "info"
	case WARN:
		return "warn"
	case ERROR:
		return "error"
	case FATAL:
		return "fatal"
	default:
		return ""
	}
}

func ToLevel(levelStr string) Level {
	switch strings.TrimSpace(strings.ToLower(levelStr)) {
	case "debug":
		return DEBUG
	case "info":
		return INFO
	case "warn":
		return WARN
	case "error":
		return ERROR
	case "fatal":
		return FATAL
	default:
		return UNKNOWN
	}
}

func Log(format string, level Level, v ...interface{}) error {
	message := ""
	if format == "" {
		formatted := []string{}
		for _, elem := range v {
			formatted = append(formatted, fmt.Sprint(elem))
		}
		message = strings.Join(formatted, ", ")
	} else {
		message = fmt.Sprintf(format, v...)
	}
	shiftMessage := types.Message{
		Type: types.LogMessage,
		Log: &types.Log{
			Level:   level.String(),
			Message: message,
		},
	}

	return Print(level, shiftMessage)
}

func Print(level Level, value any) error {
	if level == ERROR {
		return json.NewEncoder(errorWriter).Encode(value)

	}
	return json.NewEncoder(writer).Encode(value)
}
