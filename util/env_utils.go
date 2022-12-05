package util

import (
	"os"
	"strings"
)

func SetDynamoDBEnvDefaults() {
	os.Setenv("DYNAMODB_REGION", "")
	os.Setenv("DYNAMODB_URL", "http://localhost:8000")
	os.Setenv("DYNAMODB_TABLE", "messages")
}

func GetDynamoDBServiceEnvs() (string, string) {
	return os.Getenv("DYNAMODB_REGION"), os.Getenv("DYNAMODB_URL")
}

func IsLocalSetup() bool {
	return strings.ToUpper(os.Getenv("IS_CLOUD_DEPLOY")) == "TRUE"
}
