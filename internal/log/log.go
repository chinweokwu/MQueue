package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.SugaredLogger
	core *zap.Logger
}

// NewLogger creates a default production logger (INFO, JSON)
func NewLogger() *Logger {
	return NewFromConfig("info", "json")
}

// NewFromConfig creates a logger with specified level and encoding
func NewFromConfig(level, encoding string) *Logger {
	atom := zap.NewAtomicLevel()

	switch level {
	case "debug":
		atom.SetLevel(zap.DebugLevel)
	case "info":
		atom.SetLevel(zap.InfoLevel)
	case "warn":
		atom.SetLevel(zap.WarnLevel)
	case "error":
		atom.SetLevel(zap.ErrorLevel)
	default:
		atom.SetLevel(zap.InfoLevel)
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	cfg := zap.Config{
		Level:            atom,
		Encoding:         encoding, // "json" or "console"
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := cfg.Build()
	if err != nil {
		// Fallback to basic logger if config fails
		l, _ := zap.NewProduction()
		return &Logger{l.Sugar(), l}
	}

	return &Logger{logger.Sugar(), logger}
}

func (l *Logger) With(args ...interface{}) *Logger {
	return &Logger{l.SugaredLogger.With(args...), l.core}
}

func (l *Logger) Sync() error {
	return l.SugaredLogger.Sync()
}
