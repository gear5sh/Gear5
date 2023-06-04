package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/types"
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
	Log("", INFO, v...)
}

// Info writes record into os.stdout with log level INFO
func Infof(format string, v ...interface{}) {
	Log(format, INFO, v...)
}

// Info writes record into os.stdout with log level INFO
func Debug(v ...interface{}) {
	Log("", DEBUG, v...)
}

// Info writes record into os.stdout with log level INFO
func Debugf(format string, v ...interface{}) {
	Log(format, DEBUG, v...)
}

// Error writes record into os.stdout with log level ERROR
func Error(v ...interface{}) {
	Log("", ERROR, v...)
}

// Fatal writes record into os.stdout with log level ERROR and exits
func Fatal(v ...interface{}) {
	Log("", ERROR, v...)
	os.Exit(1)
}

// Fatal writes record into os.stdout with log level ERROR
func Fatalf(format string, v ...interface{}) {
	Log(format, ERROR, v...)
	os.Exit(1)
}

// Error writes record into os.stdout with log level ERROR
func Errorf(format string, v ...interface{}) {
	Log(format, ERROR, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warn(v ...interface{}) {
	Log("", WARN, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warnf(format string, v ...interface{}) {
	Log(format, WARN, v...)
}

func Log(format string, level Level, v ...interface{}) {
	message := ""
	if format == "" {
		message = fmt.Sprint(v...)
	} else {
		message = fmt.Sprintf(format, v...)
	}
	syndicateMessage := models.Message{
		Type: types.LogType,
		Log: &models.Log{
			Level:   level.String(),
			Message: message,
		},
	}

	if level == ERROR {
		json.NewEncoder(errorWriter).Encode(syndicateMessage)
		return
	}
	json.NewEncoder(writer).Encode(syndicateMessage)
}
