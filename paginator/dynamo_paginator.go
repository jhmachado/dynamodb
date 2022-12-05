package paginator

import (
	"context"
	"errors"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jhmachado/dynamodb"
	"github.com/jhmachado/dynamodb/table"
	"github.com/jhmachado/dynamodb/util"
)

type DynamoPaginator struct {
	queryPaginator  *ddb.QueryPaginator
	scanPaginator   *ddb.ScanPaginator
	entitiesDecoder table.EntitiesDecoder
	logOpts         []string
}

func (p *DynamoPaginator) HasMorePages() bool {
	if p.queryPaginator != nil {
		return p.queryPaginator.HasMorePages()
	}

	return p.scanPaginator.HasMorePages()
}

func (p *DynamoPaginator) NextPage(ctx context.Context) ([]interface{}, error) {
	client, err := dynamodb.Client()
	if err != nil {
		return nil, err
	}

	dbCtx, cancel := util.BuildDBContext(ctx, client.TimeoutsMs)
	if cancel != nil {
		defer cancel()
	}

	items, err := p.extractItems(dbCtx)
	if err != nil {
		return nil, err
	}

	return p.entitiesDecoder.AttributeMapsToEntities(items)
}

func (p DynamoPaginator) extractItems(dbCtx context.Context) ([]map[string]types.AttributeValue, error) {
	if p.queryPaginator != nil {
		queryOutput, err := p.queryPaginator.NextPage(dbCtx)
		if err != nil {
			return nil, errors.New(util.FormatErrorMessage("pagination failed for query", p.logOpts))
		}

		return queryOutput.Items, nil
	}

	if p.scanPaginator != nil {
		scanOutput, err := p.scanPaginator.NextPage(dbCtx)
		if err != nil {
			return nil, errors.New(util.FormatErrorMessage("pagination failed for scan", p.logOpts))
		}

		return scanOutput.Items, nil
	}

	return nil, errors.New("no paginator provided")
}

func NewQueryPaginator(
	client *ddb.Client,
	resolver table.EntityResolver,
	keySchema table.KeySchema,
	opts *ddb.QueryInput,
	logOpts []string,
) Paginator {
	return &DynamoPaginator{
		queryPaginator: ddb.NewQueryPaginator(client, opts),
		entitiesDecoder: table.EntitiesDecoder{
			EntityResolver: resolver,
			KeySchema:      keySchema,
		},
		logOpts: logOpts,
	}
}

func NewScanPaginator(
	client *ddb.Client,
	resolver table.EntityResolver,
	keySchema table.KeySchema,
	opts *ddb.ScanInput,
	logOpts []string,
) Paginator {
	return &DynamoPaginator{
		scanPaginator: ddb.NewScanPaginator(client, opts),
		entitiesDecoder: table.EntitiesDecoder{
			EntityResolver: resolver,
			KeySchema:      keySchema,
		},
		logOpts: logOpts,
	}
}
