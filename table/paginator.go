package table

import "context"

type Paginator interface {
	HasMorePages() bool
	NextPage(ctx context.Context) ([]interface{}, error)
}
