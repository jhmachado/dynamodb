package table

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	db "github.com/jhmachado/dynamodb/client"
	"github.com/jhmachado/dynamodb/util"
)

func scan(ctx context.Context, table Table, inputOptions InputOptions) ([]interface{}, error) {
	client, err := db.GetClient()
	if err != nil {
		return nil, err
	}

	scanInput, err := buildScanInput(table, inputOptions)
	if err != nil {
		return nil, err
	}

	scanQuery := "SELECT *"
	if scanInput.FilterExpression != nil {
		scanQuery = *scanInput.FilterExpression
	}
	log.Debugf("[%s] DynamoDB scan query: %s", table.CollectionName(), scanQuery)

	dbCtx, cancel := util.BuildDBContext(ctx, client.TimeoutsMs)
	if cancel != nil {
		defer cancel()
	}

	scanOutput, err := client.AWSClient.Scan(dbCtx, scanInput)
	if err != nil {
		return nil, errors.New("scan failed on collection, " + table.CollectionName())
	}

	log.Debugf("Result set size: %d", len(scanOutput.Items))

	decoder := EntitiesDecoder{
		EntityResolver: table.EntityResolver,
		KeySchema:      table.KeySchema,
	}

	return decoder.AttributeMapsToEntities(scanOutput.Items)
}

func paginatedScan(table Table, inputOptions InputOptions) (Paginator, error) {
	client, err := db.GetClient()
	if err != nil {
		return nil, err
	}

	scanInput, err := buildScanInput(table, inputOptions)
	if err != nil {
		return nil, err
	}

	if scanInput.Limit == nil {
		val := int32(DefaultPageSize)
		scanInput.Limit = &val
	}

	scanPaginator := NewScanPaginator(
		client.AWSClient,
		table.EntityResolver,
		table.KeySchema,
		scanInput,
		[]string{table.CollectionName()},
	)

	return scanPaginator, nil
}

func buildScanInput(table Table, inputOptions InputOptions) (*dynamodb.ScanInput, error) {
	scanInput := &dynamodb.ScanInput{
		TableName: &table.TableName,
	}

	if table.IndexName != nil {
		scanInput.IndexName = table.IndexName
	}

	if filter, ok := inputOptions[attributesFilter]; ok {
		expr := filter.(string)
		scanInput.FilterExpression = &expr
	}

	if tvs, ok := inputOptions[optTokenValues]; ok {
		avs, err := attributevalue.MarshalMap(tvs.(map[string]interface{}))
		if err != nil {
			return nil, err
		}

		scanInput.ExpressionAttributeValues = avs
	}

	if substitutions, ok := inputOptions[optTokenNameSubstitutions]; ok {
		scanInput.ExpressionAttributeNames = substitutions.(map[string]string)
	}

	if projections, ok := inputOptions[projections]; ok {
		scanInput.ProjectionExpression = projections.(*string)
	}

	if limit, ok := inputOptions[limit]; ok {
		val := int32(limit.(int))
		scanInput.Limit = &val
	}

	return scanInput, nil
}
