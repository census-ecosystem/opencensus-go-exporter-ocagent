# OpenCensus Agent Go Exporter

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

[OCAgentReadme]: https://github.com/census-instrumentation/opencensus-proto/tree/master/opencensus/proto/agent#opencensus-agent-proto
[OpenCensusGo]: https://github.com/census-instrumentation/opencensus-go
