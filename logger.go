package redis_ease

import "log"

// LogLevel represents the level of logging.
type LogLevel int

const (
	// LogLevelNone disables logging.
	LogLevelNone LogLevel = iota
	// LogLevelError logs only errors.
	LogLevelError
	// LogLevelWarn logs warnings and errors.
	LogLevelWarn
	// LogLevelInfo logs info, warnings, and errors.
	LogLevelInfo
	// LogLevelDebug logs all messages including debug.
	LogLevelDebug
)

// Logger defines the interface for logging. This allows users to use their own logger.
type Logger interface {
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

// leveledLogger is a simple internal logger that writes to stdout.
type leveledLogger struct {
	level LogLevel
}

func (l *leveledLogger) Errorf(format string, v ...interface{}) {
	if l.level >= LogLevelError {
		log.Printf("[ERROR] "+format, v...)
	}
}

func (l *leveledLogger) Warnf(format string, v ...interface{}) {
	if l.level >= LogLevelWarn {
		log.Printf("[WARN] "+format, v...)
	}
}

func (l *leveledLogger) Infof(format string, v ...interface{}) {
	if l.level >= LogLevelInfo {
		log.Printf("[INFO] "+format, v...)
	}
}

func (l *leveledLogger) Debugf(format string, v ...interface{}) {
	if l.level >= LogLevelDebug {
		log.Printf("[DEBUG] "+format, v...)
	}
}

// discardLogger is a logger that outputs nothing.
type discardLogger struct{}

func (l *discardLogger) Errorf(format string, v ...interface{}) {}
func (l *discardLogger) Warnf(format string, v ...interface{})  {}
func (l *discardLogger) Infof(format string, v ...interface{})  {}
func (l *discardLogger) Debugf(format string, v ...interface{}) {}
