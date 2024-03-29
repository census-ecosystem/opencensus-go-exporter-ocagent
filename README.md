> **Warning**
>
> OpenCensus and OpenTracing have merged to form [OpenTelemetry](https://opentelemetry.io), which serves as the next major version of OpenCensus and OpenTracing.
>
> OpenTelemetry has now reached feature parity with OpenCensus, with tracing and metrics SDKs available in .NET, Golang, Java, NodeJS, and Python. **All OpenCensus Github repositories, except [census-instrumentation/opencensus-python](https://github.com/census-instrumentation/opencensus-python), will be archived on July 31st, 2023**. We encourage users to migrate to OpenTelemetry by this date.
>
> To help you gradually migrate your instrumentation to OpenTelemetry, bridges are available in Java, Go, Python, and JS. [**Read the full blog post to learn more**](https://opentelemetry.io/blog/2023/sunsetting-opencensus/).

# OpenCensus Agent Go Exporter

[![Build Status][travis-image]][travis-url] [![GoDoc][godoc-image]][godoc-url]


This repository contains the Go implementation of the OpenCensus Agent (OC-Agent) Exporter.
OC-Agent is a deamon process running in a VM that can retrieve spans/stats/metrics from
OpenCensus Library, export them to other backends and possibly push configurations back to
Library. See more details on [OC-Agent Readme][OCAgentReadme].

Note: This is an experimental repository and is likely to get backwards-incompatible changes.
Ultimately we may want to move the OC-Agent Go Exporter to [OpenCensus Go core library][OpenCensusGo].

## Installation

```bash
$ go get -u contrib.go.opencensus.io/exporter/ocagent
```

## Usage

```go
import (
	"context"
	"fmt"
	"log"
	"time"

	"contrib.go.opencensus.io/exporter/ocagent"
	"go.opencensus.io/trace"
)

func Example() {
	exp, err := ocagent.NewExporter(ocagent.WithInsecure(), ocagent.WithServiceName("your-service-name"))
	if err != nil {
		log.Fatalf("Failed to create the agent exporter: %v", err)
	}
	defer exp.Stop()

	// Now register it as a trace exporter.
	trace.RegisterExporter(exp)

	// Then use the OpenCensus tracing library, like we normally would.
	ctx, span := trace.StartSpan(context.Background(), "AgentExporter-Example")
	defer span.End()

	for i := 0; i < 10; i++ {
		_, iSpan := trace.StartSpan(ctx, fmt.Sprintf("Sample-%d", i))
		<-time.After(6 * time.Millisecond)
		iSpan.End()
	}
}
```

[OCAgentReadme]: https://github.com/census-instrumentation/opencensus-proto/tree/master/opencensus/proto/agent#opencensus-agent-proto
[OpenCensusGo]: https://github.com/census-instrumentation/opencensus-go
[godoc-image]: https://godoc.org/contrib.go.opencensus.io/exporter/ocagent?status.svg
[godoc-url]: https://godoc.org/contrib.go.opencensus.io/exporter/ocagent
[travis-image]: https://travis-ci.org/census-ecosystem/opencensus-go-exporter-ocagent.svg?branch=master
[travis-url]: https://travis-ci.org/census-ecosystem/opencensus-go-exporter-ocagent

