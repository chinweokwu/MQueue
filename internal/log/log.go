package log

import (
	"go.uber.org/zap"
)

type Logger struct {
	*zap.SugaredLogger
}

func NewLogger() *Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	return &Logger{logger.Sugar()}
}
