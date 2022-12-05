package table

import (
	"context"
	"github.com/jhmachado/dynamodb/logger"
	"github.com/jhmachado/dynamodb/paginator"
)

const DefaultPageSize = 50

var log logger.Logger

func init() {
	log = logger.NewLogger()
}

type Table struct {
	TableName      string
	IndexName      *string
	PrimaryKey     PrimaryKey
	KeySchema      KeySchema
	EntityResolver EntityResolver
}

func (t Table) GetEntityResolver() EntityResolver {
	return t.EntityResolver
}

func (t *Table) SetEntityResolver(er EntityResolver) {
	t.EntityResolver = er
}

func (t Table) CollectionName() string {
	if t.IndexName != nil {
		return *t.IndexName
	}
	return t.TableName
}

func (t Table) Get(ctx context.Context, inputOptions InputOptions) (interface{}, error) {
	return get(ctx, t, inputOptions)
}

func (t Table) Put(ctx context.Context, item interface{}, inputOptions InputOptions) error {
	return put(ctx, t, item, inputOptions)
}

func (t Table) Query(ctx context.Context, partitionKey string, inputOptions InputOptions) ([]interface{}, error) {
	return query(ctx, t, partitionKey, inputOptions)
}

func (t Table) PaginatedQuery(partitionKey string, inputOptions InputOptions) (paginator.Paginator, error) {
	return paginatedQuery(t, partitionKey, inputOptions)
}

func (t Table) Update(ctx context.Context, inputOptions InputOptions) error {
	return updateItem(ctx, t, inputOptions)
}

func (t Table) Scan(ctx context.Context, inputOptions InputOptions) ([]interface{}, error) {
	return scan(ctx, t, inputOptions)
}

func (t Table) PaginatedScan(inputOptions InputOptions) (paginator.Paginator, error) {
	return paginatedScan(t, inputOptions)
}
