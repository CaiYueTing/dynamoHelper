package dynamoHelper

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Dynamo struct {
	region    *string
	tablename *string
}

func NewDynamo(region string, tablename string) *Dynamo {
	return &Dynamo{
		region:    &region,
		tablename: &tablename,
	}
}

func (d *Dynamo) newSess() *dynamodb.DynamoDB {
	return dynamodb.New(session.New(), &aws.Config{
		Region: d.region,
	})
}

func (d *Dynamo) newQueryInput(tablename string, colume string, index string, key string, operator string) *dynamodb.QueryInput {
	return &dynamodb.QueryInput{
		KeyConditions: map[string]*dynamodb.Condition{
			colume: {
				AttributeValueList: []*dynamodb.AttributeValue{{S: &key}},
				ComparisonOperator: &operator,
			},
		},
		TableName: &tablename,
		IndexName: &index,
	}
}

func (d *Dynamo) QueryTable(tablename string, colume string, index string, key string, operator string) []map[string]*dynamodb.AttributeValue {
	input := d.newQueryInput(tablename, colume, index, key, operator)
	sess := d.newSess()
	result, err := sess.Query(input)
	if err != nil {
		fmt.Println(err)
	}
	return result.Items
}

func (d Dynamo) GetItemWithIndex() {}

func (d *Dynamo) GetItem() {}
