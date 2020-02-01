package dynamoHelper

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
		checkerr(err)
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
		checkerr(err)
		return nil, err
	}
	return result, nil
}

func (d *Dynamo) GetTableSize() (int64, error) {
	input := dynamodb.DescribeTableInput{
		TableName: d.tablename,
	}
	sess := d.newSess()
	result, err := sess.DescribeTable(&input)
	if err != nil {
		checkerr(err)
		return 0, err
	}
	return *result.Table.TableSizeBytes, nil
}

func (d *Dynamo) newScanInput(totalSeg int64, colume string, key string) []*dynamodb.ScanInput {
	inputs := make([]*dynamodb.ScanInput, totalSeg)
	for i, _ := range inputs {
		inputs[i] = &dynamodb.ScanInput{
			ScanFilter: map[string]*dynamodb.Condition{
				colume: {
					ComparisonOperator: aws.String("CONTAINS"),
					AttributeValueList: []*dynamodb.AttributeValue{{S: &key}},
				},
			},
			TableName:     d.tablename,
			Segment:       aws.Int64(int64(i)),
			TotalSegments: &totalSeg,
		}
	}
	return inputs
}

func (d *Dynamo) ScanTable(seg int64, colume string, key string) []map[string]*dynamodb.AttributeValue {
	inputs := d.newScanInput(seg, colume, key)
	results := []map[string]*dynamodb.AttributeValue{}
	ch := make(chan dynamodb.ScanOutput, seg)
	for i := 0; i < int(seg); i++ {
		go func(i int) {
			sess := d.newSess()
			result, err := sess.Scan(inputs[i])
			fmt.Println("ch :", i, ":", err)
			defer func() {
				if err := recover(); err != nil {
					fmt.Println("recover:", err)
				}
			}()
			ch <- *result
		}(i)
	}
	for i := 0; i < int(seg); i++ {
		output := <-ch
		results = append(results, output.Items...)
	}

	return results
}

func checkerr(err error) {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case dynamodb.ErrCodeProvisionedThroughputExceededException:
			fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
		case dynamodb.ErrCodeResourceNotFoundException:
			fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
		case dynamodb.ErrCodeRequestLimitExceeded:
			fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
		case dynamodb.ErrCodeInternalServerError:
			fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
		default:
			fmt.Println(aerr.Error())
		}
	} else {

		fmt.Println(err.Error())
	}
	return
}
