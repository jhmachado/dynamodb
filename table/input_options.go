package table

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const writeConcurrency = "WriteConcurrency"
const batchWriteInitialBackofficeDelayMs = "BatchWriteInitialBackofficeDelayMs"
const batchWriteMaxRetries = "BatchWriteMaxRetries"
const sortKeyFilter = "SortKeyFilter"
const descSortOrder = "DescSortOrder"
const consistentRead = "ConsistentRead"
const attributesFilter = "AttributesFilter"
const conditionExpression = "ConditionExpression"
const updateExpression = "UpdateExpression"
const optPartitionKeyFilter = "PartitionKeyFilter"
const optTokenValues = "TokenValues"
const optTokenNameSubstitutions = "TokenNameSubstitutions"
const projections = "Projections"
const limit = "Limit"

type InputOptionsFunc func(kvs map[string]interface{}) error

type InputOptions map[string]interface{}

func WithExpression(expressionType, expression string, tokenValues ...interface{}) InputOptionsFunc {
	return func(kvs map[string]interface{}) error {
		expr, numTokens := replaceCharTokensWithCounters(expressionType, expression)
		kvs[expressionType] = expr

		if numTokens == 0 {
			return nil
		}

		if numTokens > 0 && (tokenValues == nil || numTokens != len(tokenValues)) {
			return errors.New("all token values not provided for expression: " + expr)
		}

		if _, ok := kvs[optTokenValues]; !ok {
			kvs[optTokenValues] = make(map[string]interface{})
		}

		tvMap := kvs[optTokenValues].(map[string]interface{})
		i := 1

		if expressionType == optPartitionKeyFilter {
			i = 0
		}

		for _, val := range tokenValues {
			tvMap[":"+strconv.Itoa(i)] = val
			i++
		}

		hashPrefixedTokens := findHashPrefixedTokens(expression)
		if len(hashPrefixedTokens) > 0 {
			if _, ok := kvs[optTokenNameSubstitutions]; !ok {
				kvs[optTokenNameSubstitutions] = make(map[string]string)
			}

			substMap := kvs[optTokenNameSubstitutions].(map[string]string)
			for _, token := range hashPrefixedTokens {
				substMap[token] = token[1:]
			}
		}

		return nil
	}
}

func replaceCharTokensWithCounters(expressionType, expression string) (string, int) {
	numTokens := 0
	counter := 0

	if expressionType == optPartitionKeyFilter {
		counter = -1
	}

	start := 0

	for true {
		if start >= len(expression) {
			break
		}

		index := strings.Index(expression, "?")
		if index < 0 {
			break
		}

		counter++
		numTokens++
		expression = fmt.Sprintf("%s:%d%s", expression[0:index], counter, expression[index+1:])
		start = index + 2
	}

	return expression, numTokens
}

func findHashPrefixedTokens(expression string) []string {
	tokens := make([]string, 0)
	start := 0

	for true {
		if start >= len(expression) {
			break
		}

		index := strings.Index(expression, "#")
		if index < 0 {
			break
		}

		spaceCharIndex := strings.Index(expression[index:], " ")
		if spaceCharIndex < 0 {
			tokens = append(tokens, expression[index:])
			break
		}

		tokens = append(tokens, expression[index:index+spaceCharIndex])
		expression = fmt.Sprintf("%s_%s", expression[0:index], expression[index+1:])
	}

	return tokens
}
