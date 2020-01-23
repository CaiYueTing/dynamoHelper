package dynamoHelper

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func NewDynamodb(region string) *dynamodb.DynamoDB{
	return dynamodb.New(session.New(), &aws.Config{
		Region: &region,
	})
}

