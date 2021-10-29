package DistributedLockAWS

import "github.com/aws/aws-dax-go/dax"

type LoaderLock struct {
	RecordID            string `json:"invocationRecordID"`
	InvocationTimestamp string `json:"invocationTimestamp"`
}

type LoaderLockInput struct {
	Lock LoaderLock `json:"lock"`
	ID   string     `json:"id"`
}

type errorHandler func(*dax.Dax, []string)
