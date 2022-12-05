package table

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jhmachado/dynamodb/util"
)

type EntitiesDecoder struct {
	EntityResolver EntityResolver
	KeySchema      KeySchema
}

func (d *EntitiesDecoder) AttributeMapsToEntities(items []map[string]types.AttributeValue) ([]interface{}, error) {
	entities, err := d.ResolveZeroEntities(items)
	if err != nil {
		return nil, err
	}

	if len(entities) == 0 {
		return entities, nil
	}

	err = attributevalue.UnmarshalListOfMaps(items, &entities)
	if err != nil {
		return nil, errors.New("failed to deserialize dynamodb items")
	}

	return entities, nil
}

func (d *EntitiesDecoder) ResolveZeroEntities(items []map[string]types.AttributeValue) ([]interface{}, error) {
	zeroEntities := make([]interface{}, 0, len(items))
	if len(items) == 0 {
		return zeroEntities, nil
	}

	pkEntityIndices := make([]int, 0)
	for _, item := range items {
		pk := item[d.KeySchema.PkName].(*types.AttributeValueMemberS).Value
		var sk interface{}
		var err error

		if d.KeySchema.SkName != nil {
			av := item[*d.KeySchema.SkName]
			sk, err = util.CastAttributeValue(av)
			if err != nil {
				return nil, err
			}
		}

		primaryKey := util.MemoryOnlyPrimaryKey{
			Pk: pk,
			Sk: sk,
		}

		zeroEntity, isPkEntity := d.ResolveZeroEntity(primaryKey)
		if isPkEntity {
			pkEntityIndices = append(pkEntityIndices, len(zeroEntities))
		}

		zeroEntities = append(zeroEntities, zeroEntity)
	}

	return zeroEntities, nil
}

func (d *EntitiesDecoder) ResolveZeroEntity(primaryKey PrimaryKey) (interface{}, bool) {
	var zeroEntity interface{}
	isPkEntity := false
	var err error

	if d.EntityResolver != nil {
		zeroEntity, isPkEntity, err = d.EntityResolver.CreateZeroEntity(primaryKey)
	}

	if d.EntityResolver == nil || err != nil {
		zeroEntity = make(map[string]interface{})
	}

	return zeroEntity, isPkEntity
}
