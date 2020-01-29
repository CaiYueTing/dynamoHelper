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

func (d *Dynamo) newQueryInput(colume string, index string, key string, operator string) *dynamodb.QueryInput {
	return &dynamodb.QueryInput{
		KeyConditions: map[string]*dynamodb.Condition{
			colume: {
				AttributeValueList: []*dynamodb.AttributeValue{{S: &key}},
				ComparisonOperator: &operator,
			},
		},
		TableName: d.tablename,
		IndexName: &index,
	}
}

func (d *Dynamo) QueryTableWithIndex(colume string, index string, key string, operator string) ([]map[string]*dynamodb.AttributeValue, error) {
	input := d.newQueryInput(colume, index, key, operator)
	sess := d.newSess()
	result, err := sess.Query(input)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return result.Items, nil
}

func (d *Dynamo) newGetItemInput(colume string, key string) *dynamodb.GetItemInput {
	return &dynamodb.GetItemInput{
		TableName: d.tablename,
		Key:       map[string]*dynamodb.AttributeValue{colume: {S: &key}},
	}
}

func (d *Dynamo) GetItem(colume string, key string) (*dynamodb.GetItemOutput, error) {
	input := d.newGetItemInput(colume, key)
	sess := d.newSess()
	result, err := sess.GetItem(input)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return result, nil
}

func (d *Dynamo) GetTableSize(table string) (int64, error) {
	input := dynamodb.DescribeTableInput{
		TableName: d.tablename,
	}
	sess := d.newSess()
	result, err := sess.DescribeTable(&input)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	return *result.Table.TableSizeBytes, nil
}
