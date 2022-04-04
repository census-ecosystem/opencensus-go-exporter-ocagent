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
	"errors"
	"fmt"
	"time"

	"go.opencensus.io/metric/metricdata"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	"github.com/golang/protobuf/ptypes/timestamp"


	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	errNilMeasure  = errors.New("expecting a non-nil stats.Measure")
	errNilView     = errors.New("expecting a non-nil view.View")
	errNilViewData = errors.New("expecting a non-nil view.Data")
)

func viewDataToMetric(vd *view.Data, metricNamePrefix string) (*metricspb.Metric, error) {
	if vd == nil {
		return nil, errNilViewData
	}

	descriptor, err := viewToMetricDescriptor(vd.View, metricNamePrefix)
	if err != nil {
		return nil, err
	}

	timeseries, err := viewDataToTimeseries(vd)
	if err != nil {
		return nil, err
	}

	metric := &metricspb.Metric{
		MetricDescriptor: descriptor,
		Timeseries:       timeseries,
	}
	return metric, nil
}

func viewToMetricDescriptor(v *view.View, metricNamePrefix string) (*metricspb.MetricDescriptor, error) {
	if v == nil {
		return nil, errNilView
	}
	if v.Measure == nil {
		return nil, errNilMeasure
	}

	name := stringOrCall(v.Name, v.Measure.Name)
	if len(metricNamePrefix) > 0 {
		name = metricNamePrefix + "/" + name
	}
	desc := &metricspb.MetricDescriptor{
		Name:        name,
		Description: stringOrCall(v.Description, v.Measure.Description),
		Unit:        v.Measure.Unit(),
		Type:        aggregationToMetricDescriptorType(v),
		LabelKeys:   tagKeysToLabelKeys(v.TagKeys),
	}
	return desc, nil
}

func stringOrCall(first string, call func() string) string {
	if first != "" {
		return first
	}
	return call()
}

type measureType uint

const (
	measureUnknown measureType = iota
	measureInt64
	measureFloat64
)

func measureTypeFromMeasure(m stats.Measure) measureType {
	switch m.(type) {
	default:
		return measureUnknown
	case *stats.Float64Measure:
		return measureFloat64
	case *stats.Int64Measure:
		return measureInt64
	}
}

func aggregationToMetricDescriptorType(v *view.View) metricspb.MetricDescriptor_Type {
	if v == nil || v.Aggregation == nil {
		return metricspb.MetricDescriptor_UNSPECIFIED
	}
	if v.Measure == nil {
		return metricspb.MetricDescriptor_UNSPECIFIED
	}

	switch v.Aggregation.Type {
	case view.AggTypeCount:
		// Cumulative on int64
		return metricspb.MetricDescriptor_CUMULATIVE_INT64

	case view.AggTypeDistribution:
		// Cumulative types
		return metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION

	case view.AggTypeLastValue:
		// Gauge types
		switch measureTypeFromMeasure(v.Measure) {
		case measureFloat64:
			return metricspb.MetricDescriptor_GAUGE_DOUBLE
		case measureInt64:
			return metricspb.MetricDescriptor_GAUGE_INT64
		}

	case view.AggTypeSum:
		// Cumulative types
		switch measureTypeFromMeasure(v.Measure) {
		case measureFloat64:
			return metricspb.MetricDescriptor_CUMULATIVE_DOUBLE
		case measureInt64:
			return metricspb.MetricDescriptor_CUMULATIVE_INT64
		}
	}

	// For all other cases, return unspecified.
	return metricspb.MetricDescriptor_UNSPECIFIED
}

func tagKeysToLabelKeys(tagKeys []tag.Key) []*metricspb.LabelKey {
	labelKeys := make([]*metricspb.LabelKey, 0, len(tagKeys))
	for _, tagKey := range tagKeys {
		labelKeys = append(labelKeys, &metricspb.LabelKey{
			Key: tagKey.Name(),
		})
	}
	return labelKeys
}

func viewDataToTimeseries(vd *view.Data) ([]*metricspb.TimeSeries, error) {
	if vd == nil || len(vd.Rows) == 0 {
		return nil, nil
	}

	// Given that view.Data only contains Start, End
	// the timestamps for all the row data will be the exact same
	// per aggregation. However, the values will differ.
	// Each row has its own tags.
	startTimestamp := timeToProtoTimestamp(vd.Start)
	endTimestamp := timeToProtoTimestamp(vd.End)

	mType := measureTypeFromMeasure(vd.View.Measure)
	timeseries := make([]*metricspb.TimeSeries, 0, len(vd.Rows))
	// It is imperative that the ordering of "LabelValues" matches those
	// of the Label keys in the metric descriptor.
	for _, row := range vd.Rows {
		labelValues := labelValuesFromTags(row.Tags)
		point := rowToPoint(vd.View, row, endTimestamp, mType)
		timeseries = append(timeseries, &metricspb.TimeSeries{
			StartTimestamp: startTimestamp,
			LabelValues:    labelValues,
			Points:         []*metricspb.Point{point},
		})
	}

	if len(timeseries) == 0 {
		return nil, nil
	}

	return timeseries, nil
}

func timeToProtoTimestamp(t time.Time) *timestamp.Timestamp {
	unixNano := t.UnixNano()
	return &timestamp.Timestamp{
		Seconds: int64(unixNano / 1e9),
		Nanos:   int32(unixNano % 1e9),
	}
}

func rowToPoint(v *view.View, row *view.Row, endTimestamp *timestamp.Timestamp, mType measureType) *metricspb.Point {
	pt := &metricspb.Point{
		Timestamp: endTimestamp,
	}

	switch data := row.Data.(type) {
	case *view.CountData:
		pt.Value = &metricspb.Point_Int64Value{Int64Value: data.Value}

	case *view.DistributionData:
		pt.Value = &metricspb.Point_DistributionValue{
			DistributionValue: &metricspb.DistributionValue{
				Count: data.Count,
				Sum:   float64(data.Count) * data.Mean, // because Mean := Sum/Count
				// TODO: Add Exemplar
				Buckets: bucketsToProtoBuckets(data.CountPerBucket),
				BucketOptions: &metricspb.DistributionValue_BucketOptions{
					Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
						Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
							Bounds: v.Aggregation.Buckets,
						},
					},
				},
				SumOfSquaredDeviation: data.SumOfSquaredDev,
			}}

	case *view.LastValueData:
		setPointValue(pt, data.Value, mType)

	case *view.SumData:
		setPointValue(pt, data.Value, mType)
	}

	return pt
}

// Not returning anything from this function because metricspb.Point.is_Value is an unexported
// interface hence we just have to set its value by pointer.
func setPointValue(pt *metricspb.Point, value float64, mType measureType) {
	if mType == measureInt64 {
		pt.Value = &metricspb.Point_Int64Value{Int64Value: int64(value)}
	} else {
		pt.Value = &metricspb.Point_DoubleValue{DoubleValue: value}
	}
}

func bucketsToProtoBuckets(countPerBucket []int64) []*metricspb.DistributionValue_Bucket {
	distBuckets := make([]*metricspb.DistributionValue_Bucket, len(countPerBucket))
	for i := 0; i < len(countPerBucket); i++ {
		count := countPerBucket[i]

		distBuckets[i] = &metricspb.DistributionValue_Bucket{
			Count: count,
		}
	}

	return distBuckets
}

func labelValuesFromTags(tags []tag.Tag) []*metricspb.LabelValue {
	if len(tags) == 0 {
		return nil
	}

	labelValues := make([]*metricspb.LabelValue, 0, len(tags))
	for _, tag_ := range tags {
		labelValues = append(labelValues, &metricspb.LabelValue{
			Value: tag_.Value,

			// It is imperative that we set the "HasValue" attribute,
			// in order to distinguish missing a label from the empty string.
			// https://godoc.org/github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1#LabelValue.HasValue
			//
			// OpenCensus-Go uses non-pointers for tags as seen by this function's arguments,
			// so the best case that we can use to distinguish missing labels/tags from the
			// empty string is by checking if the Tag.Key.Name() != "" to indicate that we have
			// a value.
			HasValue: tag_.Key.Name() != "",
		})
	}
	return labelValues
}

func metricTypeToProtoEnum(t metricdata.Type) metricspb.MetricDescriptor_Type {
	switch t {
	case metricdata.TypeGaugeInt64:
		return metricspb.MetricDescriptor_GAUGE_INT64
	case metricdata.TypeGaugeFloat64:
		return metricspb.MetricDescriptor_GAUGE_DOUBLE
	case metricdata.TypeGaugeDistribution:
		return metricspb.MetricDescriptor_GAUGE_DISTRIBUTION
	case metricdata.TypeCumulativeInt64:
		return metricspb.MetricDescriptor_CUMULATIVE_INT64
	case metricdata.TypeCumulativeFloat64:
		return metricspb.MetricDescriptor_CUMULATIVE_DOUBLE
	case metricdata.TypeCumulativeDistribution:
		return metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION
	case metricdata.TypeSummary:
		return metricspb.MetricDescriptor_SUMMARY
	}
	return metricspb.MetricDescriptor_UNSPECIFIED
}

func metricLabelKeysToProtos(ks []metricdata.LabelKey) []*metricspb.LabelKey {
	keys := make([]*metricspb.LabelKey, len(ks))
	for i, k := range ks {
		keys[i] = &metricspb.LabelKey{
			Key:         k.Key,
			Description: k.Description,
		}
	}

	return keys
}

func metricDescriptorToProto(d metricdata.Descriptor) *metricspb.MetricDescriptor {
	return &metricspb.MetricDescriptor{
		Name:        d.Name,
		Description: d.Description,
		Unit:        string(d.Unit),
		Type:        metricTypeToProtoEnum(d.Type),
		LabelKeys:   metricLabelKeysToProtos(d.LabelKeys),
	}
}

func labelValuesToProto(vs []metricdata.LabelValue) []*metricspb.LabelValue {
	values := make([]*metricspb.LabelValue, len(vs))
	for i, v := range vs {
		values[i] = &metricspb.LabelValue{
			Value:    v.Value,
			HasValue: v.Present,
		}
	}
	return values
}

// pointValueVisitor populates value of the point proto.
type pointValueVisitor struct {
	point *metricspb.Point
}

func (vv *pointValueVisitor) VisitFloat64Value(v float64) {
	vv.point.Value = &metricspb.Point_DoubleValue{DoubleValue: v}
}
func (vv *pointValueVisitor) VisitInt64Value(v int64) {
	vv.point.Value = &metricspb.Point_Int64Value{Int64Value: v}
}

func (vv *pointValueVisitor) VisitDistributionValue(v *metricdata.Distribution) {
	var bucketOptions *metricspb.DistributionValue_BucketOptions
	if v.BucketOptions != nil {
		bucketOptions = &metricspb.DistributionValue_BucketOptions{
			Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
				Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
					Bounds: v.BucketOptions.Bounds,
				},
			},
		}
	}

	buckets := make([]*metricspb.DistributionValue_Bucket, len(v.Buckets))
	for i, b := range v.Buckets {
		var ex *metricspb.DistributionValue_Exemplar
		if b.Exemplar != nil {
			attachments := make(map[string]string)
			for k, v := range b.Exemplar.Attachments {
				switch v := v.(type) {
				case fmt.Stringer:
					attachments[k] = v.String()
				case string:
					attachments[k] = v
				default:
					attachments[k] = fmt.Sprintf("%v", v)
				}
			}
			ex = &metricspb.DistributionValue_Exemplar{
				Value:       b.Exemplar.Value,
				Timestamp:   timeToProtoTimestamp(b.Exemplar.Timestamp),
				Attachments: attachments,
			}
		}

		buckets[i] = &metricspb.DistributionValue_Bucket{
			Count:    b.Count,
			Exemplar: ex,
		}
	}
	d := &metricspb.DistributionValue{
		Count:                 v.Count,
		Sum:                   v.Sum,
		SumOfSquaredDeviation: v.SumOfSquaredDeviation,
		BucketOptions:         bucketOptions,
		Buckets:               buckets,
	}

	vv.point.Value = &metricspb.Point_DistributionValue{DistributionValue: d}
}

func (vv *pointValueVisitor) VisitSummaryValue(v *metricdata.Summary) {

	var count *wrapperspb.Int64Value
	var sum *wrapperspb.DoubleValue
	if v.HasCountAndSum {
		count = wrapperspb.Int64(v.Count)
		sum = wrapperspb.Double(v.Sum)
	}

	var ptiles []*metricspb.SummaryValue_Snapshot_ValueAtPercentile
	for k, v := range v.Snapshot.Percentiles {
		ptiles = append(ptiles, &metricspb.SummaryValue_Snapshot_ValueAtPercentile{
			Percentile: k,
			Value:      v,
		})
	}

	vv.point.Value = &metricspb.Point_SummaryValue{
		SummaryValue: &metricspb.SummaryValue{
			Count: count,
			Sum:   sum,
			Snapshot: &metricspb.SummaryValue_Snapshot{
				Count:            wrapperspb.Int64(v.Snapshot.Count),
				Sum:              wrapperspb.Double(v.Snapshot.Sum),
				PercentileValues: ptiles,
			},
		},
	}
}

func metricPointsToProto(ps []metricdata.Point) []*metricspb.Point {
	points := make([]*metricspb.Point, len(ps))
	for i, p := range ps {

		pt := &metricspb.Point{
			Timestamp: timeToProtoTimestamp(p.Time),
		}

		// pointValueVisitor populates point`s value in the point proto.
		vv := &pointValueVisitor{pt}
		p.ReadValue(vv)

		points[i] = pt
	}

	return points
}

func metricDataTimeseriesToProto(tss []*metricdata.TimeSeries) []*metricspb.TimeSeries {
	if tss == nil || len(tss) == 0 {
		return nil
	}

	timeseries := make([]*metricspb.TimeSeries, 0, len(tss))
	// It is imperative that the ordering of "LabelValues" matches those
	// of the Label keys in the metric descriptor.
	for _, ts := range tss {
		timeseries = append(timeseries, &metricspb.TimeSeries{
			StartTimestamp: timeToProtoTimestamp(ts.StartTime),
			LabelValues:    labelValuesToProto(ts.LabelValues),
			Points:         metricPointsToProto(ts.Points),
		})
	}

	return timeseries
}

func metricDataToMetric(md *metricdata.Metric) *metricspb.Metric {
	if md == nil {
		return nil
	}

	metric := &metricspb.Metric{
		MetricDescriptor: metricDescriptorToProto(md.Descriptor),
		Resource:         resourceToResourcePb(md.Resource),
		Timeseries:       metricDataTimeseriesToProto(md.TimeSeries),
	}
	return metric
}
