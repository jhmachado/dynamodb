package client

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sync"
)

var once sync.Once
var singleton *Wrapper

func Init(cfg aws.Config) {
	once.Do(func() {
		singleton = &Wrapper{
			AWSClient: ddb.NewFromConfig(cfg),
		}
	})
}

func InitWithClientTimeout(cfg aws.Config, timeoutMs int) {
	Init(cfg)
	singleton.TimeoutsMs = &timeoutMs
}

type Wrapper struct {
	AWSClient  *ddb.Client
	TimeoutsMs *int
}

func GetClient() (*Wrapper, error) {
	if singleton == nil {
		return nil, errors.New("dynamodb GetClient not initialized")
	}
	return singleton, nil
}
