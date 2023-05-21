package logger

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/piyushsingariya/syndicate/models"
)

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
	syndicateMessage := models.Message{
		Log: &models.Log{
			Level:   level.String(),
			Message: fmt.Sprintf(format, v...),
		},
	}

	json.NewEncoder(os.Stdout).Encode(syndicateMessage)
}
