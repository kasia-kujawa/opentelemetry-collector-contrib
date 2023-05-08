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
		viewCollectedLogs,
		viewCollectedAccumulated,
	)
	if err != nil {
		fmt.Printf("Failed to register sqlquery receiver's views: %v\n", err)
	}
}

var (
	mLogsCollected                = stats.Int64("receiver/logs/collected", "Number of logs collected in the collection interval", "")
	mLogsCollectedLogsAccumulated = stats.Int64("receiver/logs/collected_accumulated", "Number of logs collected", "")
	receiverKey, _                = tag.NewKey("receiver") // nolint:errcheck
)

var viewCollectedLogs = &view.View{
	Name:        mLogsCollected.Name(),
	Description: mLogsCollected.Description(),
	Measure:     mLogsCollected,
	TagKeys:     []tag.Key{receiverKey},
	Aggregation: view.LastValue(),
}

var viewCollectedAccumulated = &view.View{
	Name:        mLogsCollectedLogsAccumulated.Name(),
	Description: mLogsCollectedLogsAccumulated.Description(),
	Measure:     mLogsCollectedLogsAccumulated,
	TagKeys:     []tag.Key{receiverKey},
	Aggregation: view.Sum(),
}

func RecordCollectedLogs(collectedLogs int64, receiver string) error {
	return stats.RecordWithTags(
		context.Background(),
		[]tag.Mutator{
			tag.Insert(receiverKey, receiver),
		},
		mLogsCollected.M(collectedLogs),
	)
}

func RecordCollectedLogsAccumulated(collectedLogs int64, receiver string) error {
	return stats.RecordWithTags(
		context.Background(),
		[]tag.Mutator{
			tag.Insert(receiverKey, receiver),
		},
		mLogsCollectedLogsAccumulated.M(collectedLogs),
	)
}
