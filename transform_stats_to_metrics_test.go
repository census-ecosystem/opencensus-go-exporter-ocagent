// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ocagent

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"

	"github.com/golang/protobuf/ptypes/timestamp"
)

var (
	keyField, _ = tag.NewKey("field")
	keyName, _  = tag.NewKey("name")

	mSprinterLatencyMs = stats.Float64("sprint_latency", "The time in which a sprinter completes the course", "ms")
)

func TestViewDataToMetrics(t *testing.T) {
	startTime := time.Date(2018, 11, 25, 15, 38, 18, 997, time.UTC)
	endTime := startTime.Add(100 * time.Millisecond)

	tests := []struct {
		in      *view.Data
		want    *metricspb.Metric
		wantErr string
	}{
		{in: nil, wantErr: "expecting non-nil a view.Data"},
		{in: &view.Data{}, wantErr: "expecting non-nil a view.View"},
		{in: &view.Data{View: &view.View{}}, wantErr: "expecting a non-nil stats.Measure"},
		{
			in: &view.Data{
				Start: startTime,
				End:   endTime,
				View: &view.View{
					Name:        "ocagent.io/latency",
					Description: "latency of runners for a 100m dash",
					Aggregation: view.Distribution(0, 10, 20, 30, 40),
					TagKeys:     []tag.Key{keyField, keyName},
					Measure:     mSprinterLatencyMs,
				},
				Rows: []*view.Row{
					{
						Tags: []tag.Tag{
							{Key: keyField, Value: "main-field"},
							{Key: keyName, Value: "sprinter-#10"},
						},
						Data: &view.DistributionData{
							// Points: [11.9]
							Count:           1,
							Min:             11.9,
							Max:             11.9,
							Mean:            11.9,
							CountPerBucket:  []int64{0, 1, 0, 0, 0},
							SumOfSquaredDev: 0,
						},
					},
					{
						Tags: []tag.Tag{
							{Key: keyField, Value: "small-field"},
							{Key: keyName, Value: ""},
						},
						Data: &view.DistributionData{
							// Points: [20.2]
							Count:           1,
							Min:             20.2,
							Max:             20.2,
							Mean:            20.2,
							CountPerBucket:  []int64{0, 0, 1, 0, 0},
							SumOfSquaredDev: 0,
						},
					},
					{
						Tags: []tag.Tag{
							{Key: keyField, Value: "small-field"},
							{Key: keyName, Value: "sprinter-#yp"},
						},
						Data: &view.DistributionData{
							// Points: [28.9]
							Count:           1,
							Min:             28.9,
							Max:             28.9,
							Mean:            28.9,
							CountPerBucket:  []int64{0, 0, 1, 0, 0},
							SumOfSquaredDev: 0,
						},
					},
				},
			},
			want: &metricspb.Metric{
				Descriptor_: &metricspb.Metric_MetricDescriptor{
					MetricDescriptor: &metricspb.MetricDescriptor{
						Name:        "ocagent.io/latency",
						Description: "latency of runners for a 100m dash",
						Unit:        "ms", // Derived from the measure
						Type:        metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
						LabelKeys: []*metricspb.LabelKey{
							{Key: "field"},
							{Key: "name"},
						},
					},
				},
				Timeseries: []*metricspb.TimeSeries{
					{
						StartTimestamp: &timestamp.Timestamp{
							Seconds: 1543160298,
							Nanos:   997,
						},
						LabelValues: []*metricspb.LabelValue{
							{Value: "main-field", HasValue: true},
							{Value: "sprinter-#10", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 1543160298,
									Nanos:   100000997,
								},
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Count:                 1,
										Sum:                   11.9,
										SumOfSquaredDeviation: 0,
									}},
							},
						},
					},
					{
						StartTimestamp: &timestamp.Timestamp{
							Seconds: 1543160298,
							Nanos:   997,
						},
						LabelValues: []*metricspb.LabelValue{
							{Value: "small-field", HasValue: true},
							{Value: "", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 1543160298,
									Nanos:   100000997,
								},
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Count:                 1,
										Sum:                   20.2,
										SumOfSquaredDeviation: 0,
									}},
							},
						},
					},
					{
						StartTimestamp: &timestamp.Timestamp{
							Seconds: 1543160298,
							Nanos:   997,
						},
						LabelValues: []*metricspb.LabelValue{
							{Value: "small-field", HasValue: true},
							{Value: "sprinter-#yp", HasValue: true},
						},
						Points: []*metricspb.Point{
							{
								Timestamp: &timestamp.Timestamp{
									Seconds: 1543160298,
									Nanos:   100000997,
								},
								Value: &metricspb.Point_DistributionValue{
									DistributionValue: &metricspb.DistributionValue{
										Count:                 1,
										Sum:                   28.9,
										SumOfSquaredDeviation: 0,
									}},
							},
						},
					},
				},
				Resource: nil,
			},
		},
	}

	for i, tt := range tests {
		got, err := viewDataToMetrics(tt.in)
		if tt.wantErr != "" {
			continue
		}

		// Otherwise we shouldn't get any error.
		if err != nil {
			if got != nil {
				t.Errorf("#%d: unexpected error %v with inconsistency (non-nil result): %v", i, err, got)
			} else {
				t.Errorf("#%d: unexpected error: %v", i, err)
			}
			continue
		}

		if !reflect.DeepEqual(got, tt.want) {
			gj := serializeAsJSON(got)
			wj := serializeAsJSON(tt.want)
			if gj != wj {
				t.Errorf("#%d: Unmatched JSON\nGot:\n\t%s\nWant:\n\t%s", i, gj, wj)
			}
		}
	}
}

func serializeAsJSON(v interface{}) string {
	blob, _ := json.MarshalIndent(v, "", "  ")
	return string(blob)
}
