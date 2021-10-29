package DistributedLockAWS

import (
	"fmt"
	"github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

func DynamoUpdateSingle(dynamoClient *dynamodb.DynamoDB, table string, id string, updateParams map[string]interface{}) error {
	var updateExpression expression.UpdateBuilder
	fmt.Printf("updating record: %s\n", id)
	initial := true
	for k, v := range updateParams {
		if initial {
			updateExpression = expression.Set(expression.Name(k), expression.Value(v))
			initial = false
		} else {
			updateExpression = updateExpression.Set(expression.Name(k), expression.Value(v))
		}
	}

	updateInput, err := expression.NewBuilder().WithUpdate(updateExpression).Build()
	if err != nil {
		return err
	}

	_, err = dynamoClient.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
		},
		UpdateExpression:          updateInput.Update(),
		ExpressionAttributeNames:  updateInput.Names(),
		ExpressionAttributeValues: updateInput.Values(),
	})

	return err
}

func DaxUpdateSingle(daxClient *dax.Dax, table string, id string, updateParams map[string]interface{}) error {
	var updateExpression expression.UpdateBuilder
	fmt.Printf("updating record: %s\n", id)
	fmt.Println(updateParams)
	initial := true
	for k, v := range updateParams {
		if initial {
			updateExpression = expression.Set(expression.Name(k), expression.Value(v))
			initial = false
		} else {
			updateExpression = updateExpression.Set(expression.Name(k), expression.Value(v))
		}
	}

	updateInput, err := expression.NewBuilder().WithUpdate(updateExpression).Build()
	if err != nil {
		return err
	}

	_, err = daxClient.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
		},
		UpdateExpression:          updateInput.Update(),
		ExpressionAttributeNames:  updateInput.Names(),
		ExpressionAttributeValues: updateInput.Values(),
	})

	return err
}
