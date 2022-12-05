package util

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
)

const StringAttributeType = "*types.AttributeValueMemberS"
const NumberAttributeType = "*types.AttributeValueMemberN"
const BooleanAttributeType = "*types.AttributeValueMemberB"

func CastAttributeValue(value types.AttributeValue) (interface{}, error) {
	switch reflect.TypeOf(value).String() {
	case StringAttributeType:
		return value.(*types.AttributeValueMemberS).Value, nil
	case NumberAttributeType:
		return value.(*types.AttributeValueMemberN).Value, nil
	case BooleanAttributeType:
		return value.(*types.AttributeValueMemberB).Value, nil
	default:
		return nil, errors.New("attribute-value type is not supported")
	}
}
