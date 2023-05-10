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
		viewCollectedTotal,
	)
	if err != nil {
		fmt.Printf("Failed to register sqlquery receiver's views: %v\n", err)
	}
}

var (
	mLogsCollectedLogs = stats.Int64("receiver/collected/log/records", "Number of collected logs", "")
	receiverKey, _     = tag.NewKey("receiver") // nolint:errcheck
)

var viewCollectedTotal = &view.View{
	Name:        mLogsCollectedLogs.Name(),
	Description: mLogsCollectedLogs.Description(),
	Measure:     mLogsCollectedLogs,
	TagKeys:     []tag.Key{receiverKey},
	Aggregation: view.Sum(),
}

func RecordCollectedLogs(collectedLogs int64, receiver string) error {
	return stats.RecordWithTags(
		context.Background(),
		[]tag.Mutator{
			tag.Insert(receiverKey, receiver),
		},
		mLogsCollectedLogs.M(collectedLogs),
	)
}
