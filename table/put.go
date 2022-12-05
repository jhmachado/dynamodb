package table

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	db "github.com/jhmachado/dynamodb"
	"github.com/jhmachado/dynamodb/util"
)

func put(ctx context.Context, table Table, item interface{}, inputOptions InputOptions) error {
	client, err := db.Client()
	if err != nil {
		return err
	}

	itemValues, err := attributevalue.MarshalMap(item)
	if err != nil {
		return errors.New("failed to marshal item")
	}

	key, _ := util.GetPrimaryKeyFromAvMap(itemValues, table.KeySchema)
	if key != nil {
		log.Debugf("[%s] DynamoDB PUT with primay key, %s", table.CollectionName(), util.FormatPrimaryKey(key, &table.KeySchema))
	}

	putItemInput, err := buildPutInput(table, itemValues, inputOptions)
	if err != nil {
		return err
	}

	dbCtx, cancel := util.BuildDBContext(ctx, client.TimeoutsMs)
	if cancel != nil {
		defer cancel()
	}

	_, err = client.AWSClient.PutItem(dbCtx, putItemInput)
	if err != nil {
		return errors.New("failed to put item into table: " + table.TableName)
	}

	return nil
}

func buildPutInput(table Table, itemValues map[string]types.AttributeValue, inputOptions InputOptions) (*dynamodb.PutItemInput, error) {
	putItemInput := &dynamodb.PutItemInput{
		TableName: &table.TableName,
		Item:      itemValues,
	}

	if filter, ok := inputOptions[conditionExpression]; ok {
		expr := filter.(string)
		putItemInput.ConditionExpression = &expr
	}

	if tvs, ok := inputOptions[optTokenValues]; ok {
		avs, err := attributevalue.MarshalMap(tvs.(map[string]interface{}))
		if err != nil {
			return nil, errors.New("failed to marshal token values")
		}
		putItemInput.ExpressionAttributeValues = avs
	}

	if substitutions, ok := inputOptions[optTokenNameSubstitutions]; ok {
		putItemInput.ExpressionAttributeNames = substitutions.(map[string]string)
	}

	return putItemInput, nil
}
