package logger

import (
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DISABLE_TIME_STAMP = true
	ENABLE_LOG_LEVEL   = logrus.InfoLevel
)

func newCustomFormatter() *customFormatter {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		panic(err)
	}
	return &customFormatter{
		Formatter: &logrus.TextFormatter{
			TimestampFormat:  "15:04:05.000",
			FullTimestamp:    true,
			DisableTimestamp: DISABLE_TIME_STAMP,
		},
		TimeZone: location,
	}
}

type customFormatter struct {
	logrus.Formatter
	TimestampFormat string
	TimeZone        *time.Location
}

func (f *customFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	entry.Time = entry.Time.In(f.TimeZone)
	return f.Formatter.Format(entry)
}

var Log *logrus.Logger

func init() {
	Log = logrus.New()
	Log.SetFormatter(newCustomFormatter())
	Log.SetLevel(ENABLE_LOG_LEVEL)
}
