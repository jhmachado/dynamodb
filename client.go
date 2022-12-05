package dynamodb

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jhmachado/dynamodb/logger"
	"sync"
)

var log logger.Logger
var once sync.Once
var singleton *ClientWrapper

func Init(cfg aws.Config) {
	once.Do(func() {
		singleton = &ClientWrapper{
			AWSClient: ddb.NewFromConfig(cfg),
		}
	})
	log = logger.NewLogger()
}

func InitWithClientTimeout(cfg aws.Config, timeoutMs int) {
	Init(cfg)
	singleton.TimeoutsMs = &timeoutMs
}

type ClientWrapper struct {
	AWSClient  *ddb.Client
	TimeoutsMs *int
}

func Client() (*ClientWrapper, error) {
	if singleton == nil {
		return nil, errors.New("dynamodb Client not initialized")
	}
	return singleton, nil
}
