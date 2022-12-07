package table

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jhmachado/dynamodb"
	"github.com/jhmachado/dynamodb/client"
	"github.com/jhmachado/dynamodb/util"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const DefaultConcurrency = 5
const MaxItemsPerBatch = 25
const DefaultInitialBackoffDelayMs = 100
const DefaultMaxRetries = 2

type WriteManager struct {
	tableName string
	keySchema KeySchema
}

func (m *WriteManager) Write(ctx context.Context, items []interface{}, inputOptions InputOptions) (WriteReport, error) {
	report := WriteReport{}

	if len(items) == 0 {
		report.Status = dynamodb.Success
		return report, nil
	}

	log.Debugf("[%s] Bulk write item count: %d", m.tableName, len(items))

	if len(report.Errors) > 0 {
		return report, errors.New("bulk write failed")
	}

	maxThreads, initBackoffDelay, maxRetries := m.parseInputOptions(inputOptions)

	ch := make(chan *WriteReport)
	start := 0
	threadCount := 0

	for true {
		if start >= len(items) {
			break
		}

		end := start + MaxItemsPerBatch
		if end > len(items) {
			end = len(items)
		}

		log.Debugf("Lauching thread to write batch of %d items.", end-start)
		go writeBatch(ctx, items[start:end], m.tableName, m.keySchema, ch, initBackoffDelay, maxRetries)
		start += MaxItemsPerBatch

		threadCount++
		if threadCount >= maxThreads {
			break
		}
	}

	for batchReport := range ch {
		log.Debug("Finished writting a batch.")
		threadCount--

		report.Errors = append(report.Errors, batchReport.Errors...)
		report.UnwrittenItems = append(report.UnwrittenItems, batchReport.UnwrittenItems...)

		if start < len(items) {
			end := start + MaxItemsPerBatch
			if end < len(items) {
				end = len(items)
			}
			log.Debugf("Lauching thread to write batch of %d items.", end-start)

			go writeBatch(ctx, items[start:end], m.tableName, m.keySchema, ch, initBackoffDelay, maxRetries)
			threadCount++
			start += MaxItemsPerBatch
		}

		if threadCount == 0 {
			break
		}
	}

	switch {
	case len(report.Errors) > 0:
		report.Status = dynamodb.Err
	case len(report.UnwrittenItems) > 0:
		report.Status = dynamodb.Throttled
	default:
		report.Status = dynamodb.Success
	}

	var err error
	if report.Status != dynamodb.Success {
		err = createBuildWriteError(&report, len(items))
	}

	return report, err
}

func (m *WriteManager) Execute(ctx context.Context, stmts []PartiQLCommand, inputOptions InputOptions) (ExecutionReport, error) {
	report := ExecutionReport{}

	if len(stmts) == 0 {
		report.Status = dynamodb.Success
		return report, nil
	}

	log.Debugf("[%s] Bulk update statement count: %d", m.tableName, len(stmts))

	if len(report.Errors) > 0 {
		report.Status = dynamodb.Err
		return report, errors.New("bulk update failed")
	}

	maxThreads, _, _ := m.parseInputOptions(inputOptions)

	ch := make(chan *ExecutionReport)
	start := 0
	threadCount := 0

	for true {
		if start >= len(stmts) {
			break
		}

		end := start + MaxItemsPerBatch
		if end > len(stmts) {
			end = len(stmts)
		}

		log.Debugf("Lauching thread to write batch of %d items.", end-start)
		go updateBatch(ctx, stmts[start:end], m.tableName, m.keySchema, ch)
		start += MaxItemsPerBatch

		threadCount++
		if threadCount > maxThreads {
			break
		}
	}

	for batchReport := range ch {
		log.Debug("Finished executing a batch.")

		report.Errors = append(report.Errors, batchReport.Errors...)
		report.FailedStatements = append(report.FailedStatements, batchReport.FailedStatements...)

		if start < len(stmts) {
			end := start + MaxItemsPerBatch
			if end > len(stmts) {
				end = len(stmts)
			}
			log.Debugf("Lauching thread to write batch of %d items.", end-start)

			go updateBatch(ctx, stmts[start:end], m.tableName, m.keySchema, ch)
			threadCount++
			start += MaxItemsPerBatch
		}

		if threadCount == 0 {
			break
		}
	}

	var err error
	if len(report.Errors) > 0 {
		report.Status = dynamodb.Err
		err = createBulkExecutionError(&report, len(stmts))
	} else {
		report.Status = dynamodb.Success
	}

	return report, err
}

func createBulkExecutionError(report *ExecutionReport, totalStmts int) error {
	var arr []string
	arr = append(arr, "{")
	arr = append(arr, "bulk execution had errors: ")
	arr = append(arr, fmt.Sprintf("failed statements: %d/%d ", len(report.FailedStatements), totalStmts))
	arr = append(arr, "errors: [")

	for i, err := range report.Errors {
		arr = append(arr, err.Error())
		if i < len(report.Errors)-1 {
			arr = append(arr, ", ")
		}
	}

	arr = append(arr, "]}")
	return errors.New(strings.Join(arr, ""))
}

func updateBatch(ctx context.Context, stmts []PartiQLCommand, tableName string, ks KeySchema, outCh chan *ExecutionReport) {
	var cancelFns []context.CancelFunc
	defer func() {
		for _, cancel := range cancelFns {
			cancel()
		}
	}()

	report := &ExecutionReport{}

	client, err := client.GetClient()
	if err != nil {
		report.Errors = append(report.Errors, err)
		outCh <- report
		return
	}

	var requests []types.BatchStatementRequest
	for _, stmt := range stmts {
		req, err := createBatchStatementRequest(stmt)
		if err != nil {
			report.Errors = append(report.Errors, err)
			outCh <- report
			return
		}

		requests = append(requests, *req)
	}

	out, err := client.AWSClient.BatchExecuteStatement(ctx, &ddb.BatchExecuteStatementInput{Statements: requests})
	if err != nil {
		report.Errors = append(report.Errors, errors.New("bulk update table failed"))
		outCh <- report
		return
	}

	for i, response := range out.Responses {
		if response.Error != nil {
			report.FailedStatements = append(report.FailedStatements, stmts[i])

			var msg string
			if response.Error.Message != nil {
				msg = *(response.Error.Message)
			} else {
				msg = "execution failed"
			}

			msg = fmt.Sprintf("%s; parti-QL: %s", msg, stmts[i])
			report.Errors = append(report.Errors, errors.New(msg))
		}
	}

	outCh <- report
}

func createBatchStatementRequest(partiQL PartiQLCommand) (*types.BatchStatementRequest, error) {
	req := &types.BatchStatementRequest{
		Statement: &partiQL.Statement,
	}

	for _, token := range partiQL.Tokens {
		if token == nil {
			req.Parameters = append(req.Parameters, &types.AttributeValueMemberNULL{Value: true})
			continue
		}

		switch reflect.TypeOf(token).String() {
		case "string":
			req.Parameters = append(req.Parameters, &types.AttributeValueMemberS{Value: token.(string)})
		case "int":
			req.Parameters = append(req.Parameters, &types.AttributeValueMemberN{Value: strconv.Itoa(token.(int))})
		case "bool":
			req.Parameters = append(req.Parameters, &types.AttributeValueMemberBOOL{Value: token.(bool)})
		default:
			return nil, errors.New("unsupported type for token `{}`; supported types are: string, int bool and nil")
		}
	}

	return req, nil
}

func createBuildWriteError(report *WriteReport, totalItems int) error {
	var arr []string
	arr = append(arr, "{")
	arr = append(arr, "bulk write partially succeeded or had error(s): ")
	arr = append(arr, fmt.Sprintf("unwritten items: %d/%d", len(report.UnwrittenItems), totalItems))
	arr = append(arr, "errors: [")

	for i, err := range report.Errors {
		arr = append(arr, err.Error())
		if i < len(report.Errors)-1 {
			arr = append(arr, ", ")
		}
	}

	arr = append(arr, "]}")
	return errors.New(strings.Join(arr, ""))
}

func (m *WriteManager) parseInputOptions(inputOptions InputOptions) (int, int, int) {
	maxThreads := DefaultConcurrency
	if concurrency, ok := inputOptions[writeConcurrency]; ok {
		maxThreads = concurrency.(int)
	}

	initBackoffDelay := DefaultInitialBackoffDelayMs
	if delay, ok := inputOptions[batchWriteInitialBackofficeDelayMs]; ok {
		initBackoffDelay = delay.(int)
	}

	maxRetries := DefaultMaxRetries
	if val, ok := inputOptions[batchWriteMaxRetries]; ok {
		maxRetries = val.(int)
	}

	return maxThreads, initBackoffDelay, maxRetries
}

func writeBatch(
	ctx context.Context,
	items []interface{},
	tableName string,
	ks KeySchema,
	outCh chan *WriteReport,
	initBackoffDelay int,
	maxRetries int,
) {
	var cancelFns []context.CancelFunc
	defer func() {
		for _, cancel := range cancelFns {
			cancel()
		}
	}()

	report := &WriteReport{}
	keyToItem := make(map[string]interface{})

	client, err := client.GetClient()
	if err != nil {
		report.Errors = append(report.Errors, err)
		outCh <- report
		return
	}

	writeReqs := make([]types.WriteRequest, 0, len(items))
	for _, item := range items {
		avs, err := attributevalue.MarshalMap(item)
		if err != nil {
			report.Errors = append(report.Errors, err)
		}

		key, err := GetPrimaryKeyFromAvMap(avs, ks)
		if err != nil {
			report.Errors = append(report.Errors, err)
			continue
		}
		keyToItem[FormatPrimaryKey(key, nil)] = item

		writeReqs = append(writeReqs, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: avs},
		})
	}

	if len(writeReqs) == 0 {
		outCh <- report
		return
	}

	retryCount := 0
	backoffDelay := initBackoffDelay

	for true {
		dbCtx, cancel := util.BuildDBContext(ctx, client.TimeoutsMs)
		if cancel != nil {
			cancelFns = append(cancelFns, cancel)
		}

		out, err := getBatchWriteFunc()(dbCtx, client.AWSClient, tableName, writeReqs)
		if err != nil {
			report.Errors = append(report.Errors, err)
			if out == nil {
				break
			}
		}

		writeReqs = nil
		if len(out.UnprocessedItems) > 0 {
			writeReqs = append(writeReqs, out.UnprocessedItems[tableName]...)
			if retryCount >= maxRetries {
				break
			}

			retryCount++
			time.Sleep(time.Duration(backoffDelay) * time.Millisecond)
			backoffDelay *= 2
			continue
		}

		break
	}

	if len(writeReqs) > 0 {
		for _, writeReq := range writeReqs {
			avs := writeReq.PutRequest.Item
			key, _ := GetPrimaryKeyFromAvMap(avs, ks)
			item := keyToItem[FormatPrimaryKey(key, nil)]
			report.UnwrittenItems = append(report.UnwrittenItems, item)
		}
	}

	outCh <- report
}

type batchWriteFunc func(context.Context, *ddb.Client, string, []types.WriteRequest) (*ddb.BatchWriteItemOutput, error)

var batchWriter batchWriteFunc

func getBatchWriteFunc() batchWriteFunc {
	if batchWriter == nil {
		batchWriter = makeDynamoDBBatchWriteCall
	}
	return batchWriter
}

func setBatchWriteFunc(fn batchWriteFunc) {
	batchWriter = fn
}

func makeDynamoDBBatchWriteCall(ctx context.Context, client *ddb.Client, tableName string, writeReqs []types.WriteRequest) (*ddb.BatchWriteItemOutput, error) {
	return client.BatchWriteItem(ctx, &ddb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeReqs,
		},
	})
}
