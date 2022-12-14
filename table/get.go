package table

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	db "github.com/jhmachado/dynamodb/client"
	"github.com/jhmachado/dynamodb/util"
	"reflect"
)

func get(ctx context.Context, table Table, primaryKey PrimaryKey, inputOptions InputOptions) (interface{}, error) {
	client, err := db.GetClient()
	if err != nil {
		return nil, err
	}

	log.Debugf("[%s] DynamoDB GET with primary key, %s", table.CollectionName(), FormatPrimaryKey(primaryKey, &table.KeySchema))

	getItemInput, err := buildGetItemInput(table, primaryKey, inputOptions)
	if err != nil {
		return nil, err
	}

	dbCtx, cancel := util.BuildDBContext(ctx, client.TimeoutsMs)
	if cancel != nil {
		defer cancel()
	}

	output, err := client.AWSClient.GetItem(dbCtx, getItemInput)
	if err != nil {
		return nil, errors.New("failed to retrieve item")
	}

	if output.Item == nil {
		return nil, nil
	}

	decoder := EntitiesDecoder{
		EntityResolver: table.EntityResolver,
		KeySchema:      table.KeySchema,
	}
	entity, _ := decoder.ResolveZeroEntity(primaryKey)
	log.Debugf("resolved entity type: %s", reflect.TypeOf(entity).String())

	err = attributevalue.UnmarshalMap(output.Item, entity)
	if err != nil {
		logOptions := []string{table.CollectionName(), FormatPrimaryKey(primaryKey, &table.KeySchema)}
		message := util.FormatErrorMessage("failed to deserialize db item", logOptions)
		return nil, errors.New(message)
	}

	return entity, nil
}

func buildGetItemInput(table Table, primaryKey PrimaryKey, inputOptions InputOptions) (*dynamodb.GetItemInput, error) {
	avs, err := attributevalue.MarshalMap(primaryKey)
	if err != nil {
		return nil, errors.New("failed to marshal primary key")
	}

	getItemInput := &dynamodb.GetItemInput{
		TableName: &table.TableName,
		Key:       avs,
	}

	if tf, ok := inputOptions[consistentRead]; ok {
		getItemInput.ConsistentRead = aws.Bool(tf.(bool))
	}

	return getItemInput, nil
}
