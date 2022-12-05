package util

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"time"
)

func BuildClientConfig() (aws.Config, error) {
	region, url := GetDynamoDBServiceEnvs()
	var options []func(options *config.LoadOptions) error
	options = append(options, config.WithRegion(region))

	if IsLocalSetup() {
		endpointResolver := aws.EndpointResolverWithOptionsFunc(func(service string, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: url,
			}, nil
		})
		options = append(options, config.WithEndpointResolverWithOptions(endpointResolver))

		credentialsProvider := credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "TestKey",
				SecretAccessKey: "TestSecretKey",
			},
		}
		options = append(options, config.WithCredentialsProvider(credentialsProvider))
	}

	return config.LoadDefaultConfig(context.TODO(), options...)
}

func BuildDBContext(ctx context.Context, timeoutMs *int) (context.Context, context.CancelFunc) {
	if timeoutMs != nil {
		return context.WithTimeout(ctx, time.Millisecond*time.Duration(*timeoutMs))
	}
	return ctx, nil
}
