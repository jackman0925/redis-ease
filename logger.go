package redis_ease

import "log"

// LogLevel represents the level of logging.
type LogLevel int

const (
	// LogLevelDefault resolves to LogLevelInfo.
	LogLevelDefault LogLevel = iota
	// LogLevelNone disables logging.
	LogLevelNone
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

type panicSafeLogger struct {
	logger Logger
}

func (l *panicSafeLogger) Errorf(format string, v ...interface{}) {
	l.call(l.logger.Errorf, format, v...)
}
func (l *panicSafeLogger) Warnf(format string, v ...interface{}) {
	l.call(l.logger.Warnf, format, v...)
}
func (l *panicSafeLogger) Infof(format string, v ...interface{}) {
	l.call(l.logger.Infof, format, v...)
}
func (l *panicSafeLogger) Debugf(format string, v ...interface{}) {
	l.call(l.logger.Debugf, format, v...)
}

func (l *panicSafeLogger) call(fn func(string, ...interface{}), format string, v ...interface{}) {
	defer func() { _ = recover() }()
	fn(format, v...)
}

func buildLogger(cfg Config) Logger {
	if cfg.Logger != nil {
		return &panicSafeLogger{logger: cfg.Logger}
	}
	level := cfg.LogLevel
	if level == LogLevelDefault {
		level = LogLevelInfo
	}
	if level == LogLevelNone {
		return &discardLogger{}
	}
	if level < LogLevelError || level > LogLevelDebug {
		level = LogLevelInfo
	}
	return &leveledLogger{level: level}
}
