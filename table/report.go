package table

type PartiQLCommand struct {
	Statement string
	Tokens    []interface{}
}

type WriteReport struct {
	Status         int
	UnwrittenItems []interface{}
	Errors         []error
}

type ExecutionReport struct {
	Status           int
	FailedStatements []PartiQLCommand
	Errors           []error
}
