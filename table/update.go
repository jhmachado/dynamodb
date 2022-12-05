package table

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	db "github.com/jhmachado/dynamodb"
	"github.com/jhmachado/dynamodb/util"
)

func updateItem(ctx context.Context, table Table, inputOptions InputOptions) error {
	client, err := db.Client()
	if err != nil {
		return err
	}

	formattedPk := util.FormatPrimaryKey(table.PrimaryKey, &table.KeySchema)
	log.Debugf("[%s] DynamoDB PATH with primary key, %s.", table.CollectionName(), formattedPk)

	updateItemInput, err := buildUpdateItemInput(table, inputOptions)
	if err != nil {
		return err
	}

	dbCtx, cancel := util.BuildDBContext(ctx, client.TimeoutsMs)
	if cancel != nil {
		defer cancel()
	}

	_, err = client.AWSClient.UpdateItem(dbCtx, updateItemInput)
	if err != nil {
		logOptions := []string{table.CollectionName(), formattedPk}
		message := util.FormatErrorMessage("failed to update item", logOptions)
		return errors.New(message)
	}

	return nil
}

func buildUpdateItemInput(table Table, inputOptions InputOptions) (*dynamodb.UpdateItemInput, error) {
	updateItemInput := &dynamodb.UpdateItemInput{
		TableName: &table.TableName,
	}

	avs, err := attributevalue.MarshalMap(table.PrimaryKey)
	if err != nil {
		return nil, errors.New("failed to marshal primary key, " + err.Error())
	}

	updateItemInput.Key = avs

	if filter, ok := inputOptions[updateExpression]; ok {
		expr := filter.(string)
		updateItemInput.UpdateExpression = &expr
	}

	if filter, ok := inputOptions[conditionExpression]; ok {
		expr := filter.(string)
		updateItemInput.ConditionExpression = &expr
	}

	if tvs, ok := inputOptions[optTokenValues]; ok {
		avs, err := attributevalue.MarshalMap(tvs.(map[string]interface{}))
		if err != nil {
			return nil, errors.New("failed to marshal token values, " + err.Error())
		}
		updateItemInput.ExpressionAttributeValues = avs
	}

	if substitutions, ok := inputOptions[optTokenNameSubstitutions]; ok {
		updateItemInput.ExpressionAttributeNames = substitutions.(map[string]string)
	}

	return updateItemInput, nil
}
