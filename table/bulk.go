package table

import (
	"context"
)

func Write(ctx context.Context, t Table, items []interface{}, inputOptions InputOptions) (WriteReport, error) {
	manager := &WriteManager{
		tableName: t.TableName,
		keySchema: t.KeySchema,
	}

	return manager.Write(ctx, items, inputOptions)
}

func Execute(ctx context.Context, t Table, stmts []PartiQLCommand, inputOptions InputOptions) (ExecutionReport, error) {
	manager := &WriteManager{
		tableName: t.TableName,
	}

	return manager.Execute(ctx, stmts, inputOptions)
}
