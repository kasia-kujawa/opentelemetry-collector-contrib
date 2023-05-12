// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

package observability

import (
	"context"
	"fmt"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

func init() {
	err := view.Register(
		viewAcceptedLogs,
		viewErrorCount,
	)
	if err != nil {
		fmt.Printf("Failed to register sqlquery receiver's views: %v\n", err)
	}
}

const (
	StartError   = "start_error"
	CollectError = "collect_error"
)

var (
	mAcceptedLogs = stats.Int64("receiver/accepted/log/records", "Number of log record pushed into the pipeline.", "")
	mErrorCount   = stats.Int64("receiver/error/count", "Number of errors", "")

	receiverKey, _  = tag.NewKey("receiver")   // nolint:errcheck
	queryKey, _     = tag.NewKey("query")      // nolint:errcheck
	errorTypeKey, _ = tag.NewKey("error_type") // nolint:errcheck
)

var viewAcceptedLogs = &view.View{
	Name:        mAcceptedLogs.Name(),
	Description: mAcceptedLogs.Description(),
	Measure:     mAcceptedLogs,
	TagKeys:     []tag.Key{receiverKey, queryKey},
	Aggregation: view.Sum(),
}

var viewErrorCount = &view.View{
	Name:        mErrorCount.Name(),
	Description: mErrorCount.Description(),
	Measure:     mErrorCount,
	TagKeys:     []tag.Key{receiverKey, queryKey, errorTypeKey},
	Aggregation: view.Sum(),
}

func RecordAcceptedLogs(acceptedLogs int64, receiver string, query string) error {
	return stats.RecordWithTags(
		context.Background(),
		[]tag.Mutator{
			tag.Insert(receiverKey, receiver),
			tag.Insert(queryKey, query),
		},
		mAcceptedLogs.M(acceptedLogs),
	)
}

func RecordErrorCount(errorType string, receiver string, query string) error {
	return stats.RecordWithTags(
		context.Background(),
		[]tag.Mutator{
			tag.Insert(receiverKey, receiver),
			tag.Insert(queryKey, query),
			tag.Insert(errorTypeKey, errorType),
		},
		mErrorCount.M(int64(1)),
	)
}

func RecordNoErrorCount(errorType string, receiver string, query string) error {
	return stats.RecordWithTags(
		context.Background(),
		[]tag.Mutator{
			tag.Insert(receiverKey, receiver),
			tag.Insert(queryKey, query),
			tag.Insert(errorTypeKey, errorType),
		},
		mErrorCount.M(int64(0)),
	)
}
