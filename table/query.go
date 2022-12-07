package table

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	db "github.com/jhmachado/dynamodb/client"
	"github.com/jhmachado/dynamodb/util"
)

func query(ctx context.Context, table Table, partitionKey string, inputOptions InputOptions) ([]interface{}, error) {
	client, err := db.GetClient()
	if err != nil {
		return nil, err
	}

	queryInput, err := buildQueryInput(table, partitionKey, inputOptions)
	if err != nil {
		return nil, err
	}

	log.Debugf("[%s] DynamoDB Query: %s", table.CollectionName(), *queryInput.KeyConditionExpression)

	dbCtx, cancel := util.BuildDBContext(ctx, client.TimeoutsMs)
	if cancel != nil {
		defer cancel()
	}

	queryOutput, err := client.AWSClient.Query(dbCtx, queryInput)
	if err != nil {
		return nil, errors.New("query failed: " + err.Error())
	}

	log.Debugf("result set size: %d", len(queryOutput.Items))

	decoder := EntitiesDecoder{
		EntityResolver: table.EntityResolver,
		KeySchema:      table.KeySchema,
	}

	return decoder.AttributeMapsToEntities(queryOutput.Items)
}

func paginatedQuery(table Table, partitionKey string, inputOptions InputOptions) (Paginator, error) {
	client, err := db.GetClient()
	if err != nil {
		return nil, err
	}

	queryInput, err := buildQueryInput(table, partitionKey, inputOptions)
	if err != nil {
		return nil, err
	}

	if queryInput.Limit == nil {
		val := int32(DefaultPageSize)
		queryInput.Limit = &val
	}

	queryPaginator := NewQueryPaginator(
		client.AWSClient,
		table.EntityResolver,
		table.KeySchema,
		queryInput,
		[]string{table.CollectionName(), partitionKey},
	)

	return queryPaginator, nil
}

func buildQueryInput(table Table, partitionKey string, inputOptions InputOptions) (*dynamodb.QueryInput, error) {
	if partitionKey == "" {
		return nil, errors.New("partition key is empty")
	}

	queryInput := &dynamodb.QueryInput{
		TableName: &table.TableName,
	}

	if table.IndexName != nil {
		queryInput.IndexName = table.IndexName
	}

	fn := WithExpression(optPartitionKeyFilter, table.KeySchema.PkName+" = ?", partitionKey)
	if err := fn(inputOptions); err != nil {
		return nil, err
	}

	expression := inputOptions[optPartitionKeyFilter].(string)
	if filter, ok := inputOptions[sortKeyFilter]; ok {
		expression = fmt.Sprintf("%s AND %s", expression, filter.(string))
	}

	queryInput.KeyConditionExpression = &expression
	if tvs, ok := inputOptions[optTokenValues]; ok {
		avs, err := attributevalue.MarshalMap(tvs.(map[string]interface{}))
		if err != nil {
			return nil, errors.New("failed to marshal token values")
		}
		queryInput.ExpressionAttributeValues = avs
	}

	if substitutions, ok := inputOptions[optTokenNameSubstitutions]; ok {
		queryInput.ExpressionAttributeNames = substitutions.(map[string]string)
	}

	if projections, ok := inputOptions[projections]; ok {
		queryInput.ProjectionExpression = projections.(*string)
	}

	if desc, ok := inputOptions[descSortOrder]; ok {
		if desc.(bool) {
			b := false
			queryInput.ScanIndexForward = &b
		}
	}

	if limit, ok := inputOptions[limit]; ok {
		val := int32(limit.(int))
		queryInput.Limit = &val
	}

	if tf, ok := inputOptions[consistentRead]; ok {
		queryInput.ConsistentRead = aws.Bool(tf.(bool))
	}

	return queryInput, nil
}
