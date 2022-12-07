package table

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jhmachado/dynamodb/util"
)

type PrimaryKey interface {
	PK() string
	SK() interface{}
}

func FormatPrimaryKey(key PrimaryKey, schema *KeySchema) string {
	if schema != nil {
		if schema.SkName == nil {
			return key.PK()
		}
		return fmt.Sprintf("[%s: %s, %s: %s]", schema.PkName, key.PK(), *schema.SkName, key.SK())
	}

	if key.SK() == nil {
		return key.PK()
	}

	return fmt.Sprintf("[%s, %s]", key.PK(), key.SK())
}

func GetPrimaryKeyFromAvMap(avs map[string]types.AttributeValue, schema KeySchema) (PrimaryKey, error) {
	av, ok := avs[schema.PkName]
	if !ok {
		return nil, errors.New("partition key not found in attribute values map")
	}

	pk := av.(*types.AttributeValueMemberS).Value
	var sk interface{}

	if schema.SkName != nil {
		av, ok = avs[*schema.SkName]
		if !ok {
			return nil, errors.New("sort key not found in attribute values map")
		}

		var err error
		sk, err = util.CastAttributeValue(av)
		if err != nil {
			return nil, err
		}
	}

	return &util.MemoryOnlyPrimaryKey{
		Pk: pk,
		Sk: sk,
	}, nil
}
