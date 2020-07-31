package qup

type Logger interface {
	Log(args ...interface{})

	Logf(format string, args ...interface{})

	Error(args ...interface{})

	Errorf(format string, args ...interface{})
}

type emptyLogger struct {
}

func (*emptyLogger) Log(args ...interface{}) {

}

func (*emptyLogger) Logf(format string, args ...interface{}) {

}

func (*emptyLogger) Error(args ...interface{}) {

}

func (*emptyLogger) Errorf(format string, args ...interface{}) {

}
