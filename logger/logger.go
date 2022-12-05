package logger

import "go.uber.org/zap"

type Logger struct {
	*zap.SugaredLogger
}

func NewLogger() Logger {
	config := zap.NewProductionConfig()
	logger, err := config.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	return Logger{
		logger.WithOptions(zap.AddCallerSkip(1)).Sugar(),
	}
}
