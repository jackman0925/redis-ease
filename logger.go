package redis_ease

import "log"

// LogLevel 表示日志级别。
type LogLevel int

const (
	// LogLevelDefault 最终按 LogLevelInfo 处理。
	LogLevelDefault LogLevel = iota
	// LogLevelNone 禁用日志。
	LogLevelNone
	// LogLevelError 仅记录错误日志。
	LogLevelError
	// LogLevelWarn 记录警告和错误日志。
	LogLevelWarn
	// LogLevelInfo 记录信息、警告和错误日志。
	LogLevelInfo
	// LogLevelDebug 记录包括调试信息在内的全部日志。
	LogLevelDebug
)

// Logger 定义日志接口，调用方可以接入自定义日志实现。
type Logger interface {
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

// leveledLogger 是向标准输出写入日志的内部简单实现。
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

// discardLogger 是不产生任何输出的日志实现。
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
