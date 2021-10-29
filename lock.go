package DistributedLockAWS

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const timeFormat = "2006-01-02T15:04:05-07:00"

func LoaderLockHandlerDax(daxClient *dax.Dax, lockInput []LoaderLockInput, analyticTable string, id string) (bool, error) {
	fmt.Println("Loader lock handler initiated")
	cleanup := []string{}
	dirty := []string{}

	for _, x := range lockInput {
		if x.Lock.InvocationTimestamp != "" {
			timeParsed, err := time.Parse(timeFormat, x.Lock.InvocationTimestamp)
			if err != nil {
				return false, err
			}
			if timeParsed.Before(time.Now().Add(-30*time.Second)) && x.Lock.RecordID != id {
				fmt.Printf("Lock timestamp: %s  Current timestamp: %s  lockID: %s  currentID: %s", x.Lock.InvocationTimestamp, time.Now().Format(timeFormat), x.Lock.RecordID, id)
				return false, nil
			}
		}
	}

	fmt.Println("Records are not currently locked, attempting to gain locks")
	for _, x := range lockInput {
		err := DaxUpdateSingle(daxClient, analyticTable, x.ID, map[string]interface{}{
			"lock.invocationRecordID":  id,
			"lock.invocationTimestamp": time.Now().Format(timeFormat),
		})
		if err != nil {
			fmt.Println(err.Error())
			err2 := lockCleanupFunctionDax(daxClient, cleanup, analyticTable)
			if err2 != nil {
				return false, err2
			}
			return false, err
		}
		cleanup = append(cleanup, x.ID)
	}

	fmt.Println("Lock writes successful, checking all locks are owned")
	var lock LoaderLockInput
	for _, x := range lockInput {
		res, err := daxClient.GetItem(&dynamodb.GetItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				"id": {
					S: aws.String(x.ID),
				},
			},
			TableName: aws.String(analyticTable),
		})
		if err != nil {
			fmt.Println(err.Error())
			err2 := lockCleanupFunctionDax(daxClient, cleanup, analyticTable)
			if err2 != nil {
				return false, err2
			}
			return false, err
		}
		if res.Item == nil {
			err2 := lockCleanupFunctionDax(daxClient, cleanup, analyticTable)
			if err2 != nil {
				return false, err2
			}
			fmt.Println("Could not find record??")
			return false, errors.New(fmt.Sprintf("could not find record by ID %s\n", x.ID))
		}
		err = dynamodbattribute.UnmarshalMap(res.Item, &lock)
		if err != nil {
			fmt.Println(err.Error())
			err2 := lockCleanupFunctionDax(daxClient, cleanup, analyticTable)
			if err2 != nil {
				return false, err2
			}
			return false, err
		}

		if lock.Lock.RecordID != id {
			fmt.Println("lock record ID" + lock.Lock.RecordID)
			fmt.Println("ID" + id)
			dirty = append(dirty, lock.Lock.RecordID)
		}
	}

	if len(dirty) != 0 {
		fmt.Println("Dirty lock found")
		temp := []string{}
		for i := 0; i < len(cleanup); i++ {
			found := false
			for ii := 0; ii < len(dirty); ii++ {
				if cleanup[i] == dirty[ii] {
					found = true
					break
				}
			}
			if !found {
				temp = append(temp, cleanup[i])
			}
		}
		err := lockCleanupFunctionDax(daxClient, temp, analyticTable)
		if err != nil {
			return false, err
		}
		return false, errors.New("partial overlap on lock from another record, dirty read is possible")
	}

	return true, nil
}

func LoaderUnlockHandlerDax(daxClient *dax.Dax, lockInput []LoaderLockInput, analyticTable string, handlerFunc errorHandler) {
	fmt.Println("Loader unlock handler initiated")
	var failed []string
	for _, x := range lockInput {
		err := DaxUpdateSingle(daxClient, analyticTable, x.ID, map[string]interface{}{
			"lock.invocationRecordID":  "",
			"lock.invocationTimestamp": "",
		})
		if err != nil {
			failed = append(failed, x.ID)
		}
	}
	if len(failed) > 0 {
		handlerFunc(daxClient, failed)
	}
}

func lockCleanupFunctionDax(daxClient *dax.Dax, records []string, analyticTable string) error {
	fmt.Println("Lock cleanup function initiated")
	var failed []string
	for _, x := range records {
		err := DaxUpdateSingle(daxClient, analyticTable, x, map[string]interface{}{
			"lock.invocationRecordID":  "",
			"lock.invocationTimestamp": "",
		})
		if err != nil {
			failed = append(failed, x)
		}
	}
	if len(failed) > 0 {
		appendedIds := ""
		for _, s := range failed {
			appendedIds = appendedIds + "  " + s
		}
		return errors.New(fmt.Sprintf("unable to cleanup - locks failed: %s", appendedIds))
	}

	return nil
}
