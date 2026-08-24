package redis_ease

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuiltInLoggers(t *testing.T) {
	levels := []LogLevel{LogLevelError, LogLevelWarn, LogLevelInfo, LogLevelDebug}
	for _, level := range levels {
		logger := &leveledLogger{level: level}
		require.NotPanics(t, func() {
			logger.Errorf("error")
			logger.Warnf("warn")
			logger.Infof("info")
			logger.Debugf("debug")
		})
	}
	discard := &discardLogger{}
	discard.Errorf("error")
	discard.Warnf("warn")
	discard.Infof("info")
	discard.Debugf("debug")
}

func TestInvalidLogLevelFallsBackToInfo(t *testing.T) {
	logger := buildLogger(Config{LogLevel: LogLevel(999)})
	levelled, ok := logger.(*leveledLogger)
	require.True(t, ok)
	require.Equal(t, LogLevelInfo, levelled.level)
}
