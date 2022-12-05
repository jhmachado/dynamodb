package table

import (
	"context"
	"github.com/jhmachado/dynamodb/paginator"
)

type InputOptionsFunc func(kvs map[string]interface{}) error

type ItemsCollection interface {
	Put(ctx context.Context, item interface{}, optFns ...InputOptionsFunc) error
	Get(ctx context.Context, primaryKey PrimaryKey, optFns ...InputOptionsFunc) (interface{}, error)
	Query(ctx context.Context, partitionKey string, optFns ...InputOptionsFunc) ([]interface{}, error)
	QueryPaginator(partitionKey string, optFns ...InputOptionsFunc) (paginator.Paginator, error)
	Scan(ctx context.Context, optFns ...InputOptionsFunc) ([]interface{}, error)
	ScanPaginator(optFns ...InputOptionsFunc) (paginator.Paginator, error)
	UpdateItem(ctx context.Context, primaryKey PrimaryKey, optFns ...InputOptionsFunc) error
	BulkWrite(ctx context.Context, items []interface{}, optFns ...InputOptionsFunc) (WriteReport, error)
	BulkExecute(ctx context.Context, partiQLs []PartiQLCommand, optFns ...InputOptionsFunc) (ExecutionReport, error)
	GetEntityResolver() EntityResolver
	SetEntityResolver(er EntityResolver)
}
