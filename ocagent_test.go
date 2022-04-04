// Copyright 2020, OpenCensus Authors
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

package ocagent_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"contrib.go.opencensus.io/exporter/ocagent"
	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	agenttracepb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/trace/v1"
	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	resourcepb "github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"

	opencensus "go.opencensus.io"
	"go.opencensus.io/resource"
	"go.opencensus.io/trace"
	"go.opencensus.io/stats"
	"go.opencensus.io/view"
	"go.opencensus.io/tag"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNewExporter_endToEnd(t *testing.T) {
	// Set up a custom resource for testing WithResourceDetector ExporterOption
	cr := &resource.Resource{
		Type: "foo",
		Labels: map[string]string{
			"test": "label",
		},
	}

	crd := func(context.Context) (*resource.Resource, error) {
		return cr, nil
	}

	tests := []struct {
		name             string
		additionalOpts   []ocagent.ExporterOption
		expectedResource *resource.Resource
	}{
		{
			name: "StandardExporter",
			expectedResource: &resource.Resource{
				Type:   "",
				Labels: nil,
			},
		}, {
			name: "WithResourceDetector",
			additionalOpts: []ocagent.ExporterOption{
				ocagent.WithResourceDetector(crd),
			},
			expectedResource: cr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			newExporterEndToEndTest(t, test.additionalOpts, test.expectedResource)
		})
	}
}

func newExporterEndToEndTest(t *testing.T, additionalOpts []ocagent.ExporterOption, expRes *resource.Resource) {
	ma := runMockAgent(t)
	defer ma.stop()

	serviceName := "endToEnd_test"
	opts := []ocagent.ExporterOption{
		ocagent.WithInsecure(),
		ocagent.WithAddress(ma.address),
		ocagent.WithReconnectionPeriod(50 * time.Millisecond),
		ocagent.WithServiceName(serviceName),
	}

	opts = append(opts, additionalOpts...)
	exp, err := ocagent.NewExporter(opts...)
	if err != nil {
		t.Fatalf("Failed to create a new agent exporter: %v", err)
	}
	defer exp.Stop()

	// Once we've register the exporter, we can then send over a bunch of spans.
	trace.RegisterExporter(exp)
	defer trace.UnregisterExporter(exp)

	// Let the agent push down a couple of configurations.
	// 1. Always sample
	ma.configsToSend <- &agenttracepb.UpdatedLibraryConfig{
		Config: &tracepb.TraceConfig{
			Sampler: &tracepb.TraceConfig_ConstantSampler{
				ConstantSampler: &tracepb.ConstantSampler{Decision: tracepb.ConstantSampler_ALWAYS_ON}, // Always sample
			},
		},
	}
	<-time.After(5 * time.Millisecond)

	// Now create a couple of spans
	for i := 0; i < 4; i++ {
		_, span := trace.StartSpan(context.Background(), "AlwaysSample")
		span.Annotatef([]trace.Attribute{trace.Int64Attribute("i", int64(i))}, "Annotation")
		span.End()
	}

	m := 4
	batchedSpans := make([]*tracepb.Span, 0, m)
	for i := 0; i < m; i++ {
		name := &tracepb.TruncatableString{Value: "AlwaysSample"}
		batchedSpans = append(batchedSpans, &tracepb.Span{Name: name})
	}
	_ = exp.ExportTraceServiceRequest(&agenttracepb.ExportTraceServiceRequest{Spans: batchedSpans})

	<-time.After(10 * time.Millisecond)
	exp.Flush()

	// 2. Never sample
	ma.configsToSend <- &agenttracepb.UpdatedLibraryConfig{
		Config: &tracepb.TraceConfig{
			Sampler: &tracepb.TraceConfig_ConstantSampler{
				ConstantSampler: &tracepb.ConstantSampler{Decision: tracepb.ConstantSampler_ALWAYS_OFF}, // Never sample
			},
		},
	}
	<-time.After(5 * time.Millisecond)
	exp.Flush()

	// Now create a couple of spans
	for i, n := 0, 2; i < n; i++ {
		_, span := trace.StartSpan(context.Background(), "NeverSample")
		span.Annotatef([]trace.Attribute{trace.Int64Attribute("i", int64(n-i))}, "Annotation")
		span.End()
	}
	<-time.After(10 * time.Millisecond)
	exp.Flush()

	// 3. Probability sampler (100%)
	ma.configsToSend <- &agenttracepb.UpdatedLibraryConfig{
		Config: &tracepb.TraceConfig{
			Sampler: &tracepb.TraceConfig_ProbabilitySampler{
				ProbabilitySampler: &tracepb.ProbabilitySampler{SamplingProbability: 1.0}, // 100% probability
			},
		},
	}
	<-time.After(5 * time.Millisecond)
	exp.Flush()

	// Now create a couple of spans
	for i := 0; i < 3; i++ {
		_, span := trace.StartSpan(context.Background(), "ProbabilitySampler-100%")
		span.Annotatef([]trace.Attribute{trace.BoolAttribute("odd", i&1 == 1)}, "Annotation")
		span.End()
	}

	m = 3
	batchedSpans = make([]*tracepb.Span, 0, m)
	for i := 0; i < m; i++ {
		name := &tracepb.TruncatableString{Value: "ProbabilitySampler-100%"}
		batchedSpans = append(batchedSpans, &tracepb.Span{Name: name})
	}
	_ = exp.ExportTraceServiceRequest(&agenttracepb.ExportTraceServiceRequest{Spans: batchedSpans})

	<-time.After(10 * time.Millisecond)
	exp.Flush()

	// 4. Probability sampler (0%)
	ma.configsToSend <- &agenttracepb.UpdatedLibraryConfig{
		Config: &tracepb.TraceConfig{
			Sampler: &tracepb.TraceConfig_ProbabilitySampler{
				ProbabilitySampler: &tracepb.ProbabilitySampler{SamplingProbability: 0.0}, // 0% probability
			},
		},
	}
	<-time.After(5 * time.Millisecond)
	exp.Flush()

	for i := 0; i < 3; i++ {
		_, span := trace.StartSpan(context.Background(), "ProbabilitySampler-0%")
		span.Annotatef([]trace.Attribute{trace.BoolAttribute("even", i&1 == 0)}, "Annotation")
		span.End()
	}
	// Give the traces some time to be exported or dropped by the core library
	<-time.After(5 * time.Millisecond)

	ma.transitionToReceivingClientConfigs()
	<-time.After(5 * time.Millisecond)

	// Now invoke Flush on the exporter.
	exp.Flush()
	<-time.After(5 * time.Millisecond)

	// Now shutdown the exporter
	if err := exp.Stop(); err != nil {
		t.Errorf("Failed to stop the exporter: %v", err)
	}

	// Shutdown the agent too so that we can begin
	// verification checks of expected data back.
	ma.stop()

	// Expecting 5 receivedConfigs: the first one with the nodeInfo
	// and the rest with {AlwaysSample, NeverSample, 100%, 0%}
	spans := ma.getSpans()
	traceNodes := ma.getTraceNodes()
	receivedConfigs := ma.getReceivedConfigs()
	resource := ma.getResource()

	if g, w := len(receivedConfigs), 5; g != w {
		t.Errorf("ReceivedConfigs: got %d want %d", g, w)
	}

	// Expecting 14 spanData that were sampled, given that
	// two of the trace configs pushed down to the client
	// were {NeverSample, ProbabilitySampler(0.0)}
	if g, w := len(spans), 14; g != w {
		t.Errorf("Spans: got %d want %d", g, w)
	}

	// Now check that the responses received by the agent properly
	// contain the node identifier that we expect the exporter to have.
	wantIdentifier := &commonpb.ProcessIdentifier{
		HostName: os.Getenv("HOSTNAME"),
		Pid:      uint32(os.Getpid()),
	}
	wantLibraryInfo := &commonpb.LibraryInfo{
		Language:           commonpb.LibraryInfo_GO_LANG,
		ExporterVersion:    ocagent.Version,
		CoreLibraryVersion: opencensus.Version(),
	}
	wantServiceInfo := &commonpb.ServiceInfo{
		Name: serviceName,
	}

	var firstNodeInConfig, firstNodeInTraceExport *commonpb.Node
	if len(receivedConfigs) > 0 {
		firstNodeInConfig = receivedConfigs[0].Node
	}
	if len(traceNodes) > 0 {
		firstNodeInTraceExport = traceNodes[0]
	}
	nodeComparisons := []struct {
		name string
		node *commonpb.Node
	}{
		// Expecting the first config message that the agent got to contain the nodeInfo
		{name: "Config", node: firstNodeInConfig},
		// Expecting the first span message that the agent got to contain the nodeInfo
		{name: "Trace", node: firstNodeInTraceExport},
	}

	for _, tt := range nodeComparisons {
		node := tt.node
		if node == nil {
			t.Errorf("%q: first message should contain a non-nil Node", tt.name)
		} else if g, w := node.Identifier, wantIdentifier; !sameProcessIdentifier(g, w) {
			t.Errorf("%q: ProcessIdentifier mismatch\nGot  %#v\nWant %#v", tt.name, g, w)
		} else if g, w := node.LibraryInfo, wantLibraryInfo; !sameLibraryInfo(g, w) {
			t.Errorf("%q: LibraryInfo mismatch\nGot  %#v\nWant %#v", tt.name, g, w)
		} else if g, w := node.ServiceInfo, wantServiceInfo; !sameServiceInfo(g, w) {
			t.Errorf("%q: ServiceInfo mismatch\nGot  %#v\nWant %#v", tt.name, g, w)
		}
	}

	wantResource := &resourcepb.Resource{
		Type:   expRes.Type,
		Labels: expRes.Labels,
	}

	if !reflect.DeepEqual(resource, wantResource) {
		t.Errorf("Resource mismatch\nGot  %#v\nWant %#v", resource, wantResource)
	}
}

func TestNewExporter_metrics_endToEnd(t *testing.T) {
	// Setup mock agent.
	ma := runMockAgent(t)
	defer ma.stop()

	// Setup exporter.
	exp, err := ocagent.NewExporter(ocagent.WithInsecure(),
		ocagent.WithAddress(ma.address),
		ocagent.WithReconnectionPeriod(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to create a new agent exporter: %v", err)
	}
	defer exp.Stop()
	exp.StartMetricsExport()
	defer exp.StopMetricsExport()

	// Setup some stats and views.
	var (
		testInt64Stat   = stats.Int64("test/int64", "a test int stat", "1")
		testFloat64Stat = stats.Float64("test/float", "a test float stat", "1")

		testTag = tag.MustNewKey("test_tag")

		distributionView = &view.View{
			Name:        "test/distribution",
			Measure:     testFloat64Stat,
			Description: "test distribution",
			Aggregation: view.Distribution(10, 100, 1000),
			TagKeys:     []tag.Key{testTag}}

		intGuageView = &view.View{
			Name:        "test/intGuage",
			Measure:     testInt64Stat,
			Description: "Test guage",
			Aggregation: view.LastValue(),
			TagKeys:     []tag.Key{testTag},
		}
	)
	if err := view.Register(distributionView, intGuageView); err != nil {
		t.Fatalf("Failed to register the views: %v", err)
	}

	// Publish some dummy stats.
	ctx, err := tag.New(context.Background(), tag.Insert(testTag, "test-tag-value"))
	if err != nil {
		t.Fatalf("failed to create tag (%v) err: %v", testTag, err)
	}
	for i := 0; i < 500; i++ {
		stats.Record(ctx, testFloat64Stat.M(0.5*float64(i)), testInt64Stat.M(int64(i)))
	}

	mock := ma.metricsServer

	// Wait for the data to to reach the mock agent.
	timeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	func() {
		for {
			select {
			case <-ticker.C:
				mock.mu.Lock()
				if len(mock.metrics) >= 2 {
					mock.mu.Unlock()
					return
				}
				mock.mu.Unlock()

			case <-ctx.Done():
				t.Fatalf("Timed out waiting for 2 metrics in the mock agent. Waited %v, got metrics: %v",
					timeout, mock.metrics)
			}
		}
	}()
	mock.mu.Lock()
	defer mock.mu.Unlock()

	// Verify that we got expected metrics.
	metricsByName := make(map[string]*metricspb.Metric)
	for _, m := range mock.metrics {
		metricsByName[m.GetMetricDescriptor().GetName()] = m
	}

	want := map[string]*metricspb.Metric{
		"test/distribution": {
			MetricDescriptor: &metricspb.MetricDescriptor{
				Name:        "test/distribution",
				Description: "test distribution",
				Unit:        "1",
				Type:        metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION,
				LabelKeys: []*metricspb.LabelKey{
					{Key: "test_tag"},
				},
			},
			Timeseries: []*metricspb.TimeSeries{
				{
					LabelValues: []*metricspb.LabelValue{
						{Value: "test-tag-value", HasValue: true},
					},
					Points: []*metricspb.Point{
						{
							Value: &metricspb.Point_DistributionValue{
								DistributionValue: &metricspb.DistributionValue{
									Count:                 500,
									Sum:                   62375,
									SumOfSquaredDeviation: 2.60415625e+06,
									Buckets: []*metricspb.DistributionValue_Bucket{
										{Count: 20}, {Count: 180}, {Count: 300}, {},
									},
									BucketOptions: &metricspb.DistributionValue_BucketOptions{
										Type: &metricspb.DistributionValue_BucketOptions_Explicit_{
											Explicit: &metricspb.DistributionValue_BucketOptions_Explicit{
												Bounds: []float64{10, 100, 1000},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			Resource: nil,
		},
		"test/intGuage": {
			MetricDescriptor: &metricspb.MetricDescriptor{
				Name:        "test/intGuage",
				Description: "Test guage",
				Unit:        "1",
				Type:        metricspb.MetricDescriptor_GAUGE_INT64,
				LabelKeys: []*metricspb.LabelKey{
					{Key: "test_tag"},
				},
			},
			Timeseries: []*metricspb.TimeSeries{
				{
					LabelValues: []*metricspb.LabelValue{
						{Value: "test-tag-value", HasValue: true},
					},
					Points: []*metricspb.Point{
						{
							Value: &metricspb.Point_Int64Value{
								Int64Value: 499,
							},
						},
					},
				},
			},
			Resource: nil,
		},
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&metricspb.TimeSeries{}, "start_timestamp"),
		protocmp.IgnoreFields(&metricspb.Point{}, "timestamp"),
	}
	if diff := cmp.Diff(want, metricsByName, opts...); diff != "" {
		t.Errorf("Unexpected diff for test_distribution MetricDescriptor (-want +got): %v", diff)
	}

}

func TestNewExporter_invokeStartThenStopManyTimes(t *testing.T) {
	ma := runMockAgent(t)
	defer ma.stop()

	exp, err := ocagent.NewExporter(ocagent.WithInsecure(),
		ocagent.WithReconnectionPeriod(50*time.Millisecond),
		ocagent.WithAddress(ma.address))
	if err != nil {
		t.Fatal("Surprisingly connected with a bad port")
	}
	defer exp.Stop()

	// Invoke Start numerous times, should return errAlreadyStarted
	for i := 0; i < 10; i++ {
		if err := exp.Start(); err == nil || !strings.Contains(err.Error(), "already started") {
			t.Errorf("#%d unexpected Start error: %v", i, err)
		}
	}

	exp.Stop()
	// Invoke Stop numerous times
	for i := 0; i < 10; i++ {
		if err := exp.Stop(); err == nil || !strings.Contains(err.Error(), "not started") {
			t.Errorf(`#%d got error (%v) expected a "not started error"`, i, err)
		}
	}
}

func TestNewExporter_agentConnectionDiesThenReconnects(t *testing.T) {
	ma := runMockAgent(t)

	reconnectionPeriod := 20 * time.Millisecond
	exp, err := ocagent.NewExporter(ocagent.WithInsecure(),
		ocagent.WithAddress(ma.address),
		ocagent.WithReconnectionPeriod(reconnectionPeriod))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer exp.Stop()

	// We'll now stop the agent right away to simulate a connection
	// dying in the midst of communication or even not existing before.
	ma.stop()

	// In the test below, we'll stop the agent many times,
	// while exporting traces and test to ensure that we can
	// reconnect.
	for j := 0; j < 3; j++ {

		exp.ExportSpan(&trace.SpanData{Name: "in the midst"})
		exp.Flush()
		<-time.After(reconnectionPeriod * 2)

		// Now resurrect the agent by making a new one but reusing the
		// old address, and the agent should reconnect automatically.
		nma := runMockAgentAtAddr(t, ma.address)

		// Give the exporter sometime to reconnect
		<-time.After(reconnectionPeriod * 4)

		n := 10
		for i := 0; i < n; i++ {
			exp.ExportSpan(&trace.SpanData{Name: "Resurrected"})
		}
		exp.Flush()
		m := 10
		batchedSpans := make([]*tracepb.Span, 0, m)
		for i := 0; i < m; i++ {
			name := &tracepb.TruncatableString{Value: "Resurrected"}
			batchedSpans = append(batchedSpans, &tracepb.Span{Name: name})
		}
		_ = exp.ExportTraceServiceRequest(&agenttracepb.ExportTraceServiceRequest{Spans: batchedSpans})

		<-time.After(reconnectionPeriod * 3)
		nmaSpans := nma.getSpans()
		// Expecting 10 spanData that were sampled, given that
		if g, w := len(nmaSpans), n+m; g != w {
			t.Errorf("Round #%d: Connected agent: spans: got %d want %d", j, g, w)
		}

		dSpans := ma.getSpans()
		// Expecting 0 spans to have been received by the original but now dead agent
		if g, w := len(dSpans), 0; g != w {
			t.Errorf("Round #%d: Disconnected agent: spans: got %d want %d", j, g, w)
		}
		nma.stop()
	}
}

// This test takes a long time to run: to skip it, run tests using: -short
func TestNewExporter_agentOnBadConnection(t *testing.T) {
	if testing.Short() {
		t.Skipf("Skipping this long running test")
	}

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to grab an available port: %v", err)
	}
	// Firstly close the "agent's" channel: optimistically this address won't get reused ASAP
	// However, our goal of closing it is to simulate an unavailable connection
	ln.Close()

	_, agentPortStr, _ := net.SplitHostPort(ln.Addr().String())

	address := fmt.Sprintf("localhost:%s", agentPortStr)
	exp, err := ocagent.NewExporter(ocagent.WithInsecure(),
		ocagent.WithReconnectionPeriod(50*time.Millisecond),
		ocagent.WithAddress(address))
	if err != nil {
		t.Fatalf("Despite an indefinite background reconnection, got error: %v", err)
	}
	defer exp.Stop()
}

func TestNewExporter_withAddress(t *testing.T) {
	ma := runMockAgent(t)
	defer ma.stop()

	exp, err := ocagent.NewUnstartedExporter(
		ocagent.WithInsecure(),
		ocagent.WithReconnectionPeriod(50*time.Millisecond),
		ocagent.WithAddress(ma.address))
	if err != nil {
		t.Fatal("Surprisingly connected with a bad port")
	}
	defer exp.Stop()

	if err := exp.Start(); err != nil {
		t.Fatalf("Unexpected Start error: %v", err)
	}
}

// Best case comparison for information that we can externally introspect
func sameProcessIdentifier(n1, n2 *commonpb.ProcessIdentifier) bool {
	if n1 == nil || n2 == nil {
		return n1 == n2
	}
	return n1.HostName == n2.HostName && n1.Pid == n2.Pid
}

func sameLibraryInfo(li1, li2 *commonpb.LibraryInfo) bool {
	if li1 == nil || li2 == nil {
		return li1 == li2
	}
	return li1.Language == li2.Language &&
		li1.ExporterVersion == li2.ExporterVersion &&
		li1.CoreLibraryVersion == li2.CoreLibraryVersion
}

func sameServiceInfo(si1, si2 *commonpb.ServiceInfo) bool {
	if si1 == nil || si2 == nil {
		return si1 == si2
	}
	return si1.Name == si2.Name
}
