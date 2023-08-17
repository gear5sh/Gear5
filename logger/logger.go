package logger

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/goccy/go-json"

	"github.com/piyushsingariya/kaku/models"
	"github.com/piyushsingariya/kaku/types"
)

var (
	writer      io.Writer
	errorWriter io.Writer
)

func SetupWriter(w io.Writer, err io.Writer) {
	writer = w
	errorWriter = err
}

// Info writes record into os.stdout with log level INFO
func Info(v ...interface{}) {
	Log("", info, v...)
}

// Info writes record into os.stdout with log level INFO
func Infof(format string, v ...interface{}) {
	Log(format, info, v...)
}

// Info writes record into os.stdout with log level INFO
func Debug(v ...interface{}) {
	Log("", debug, v...)
}

// Info writes record into os.stdout with log level INFO
func Debugf(format string, v ...interface{}) {
	Log(format, debug, v...)
}

// Error writes record into os.stdout with log level ERROR
func Error(v ...interface{}) {
	Log("", errorlevel, v...)
}

// Fatal writes record into os.stdout with log level ERROR and exits
func Fatal(v ...interface{}) {
	Log("", errorlevel, v...)
	os.Exit(1)
}

// Fatal writes record into os.stdout with log level ERROR
func Fatalf(format string, v ...interface{}) {
	Log(format, errorlevel, v...)
	os.Exit(1)
}

// Error writes record into os.stdout with log level ERROR
func Errorf(format string, v ...interface{}) {
	Log(format, errorlevel, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warn(v ...interface{}) {
	Log("", warn, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warnf(format string, v ...interface{}) {
	Log(format, warn, v...)
}

func Log(format string, level Level, v ...interface{}) {
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
	kakuMessage := models.Message{
		Type: types.LogType,
		Log: &models.Log{
			Level:   level.String(),
			Message: message,
		},
	}

	if level == errorlevel {
		json.NewEncoder(errorWriter).Encode(kakuMessage)
		return
	}
	json.NewEncoder(writer).Encode(kakuMessage)
}
