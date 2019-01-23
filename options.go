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

import "time"

const (
	DefaultAgentPort uint16 = 55678
	DefaultAgentHost string = "localhost"
)

type ExporterOption func(e *Exporter)

// WithInsecure disables client transport security for the exporter's gRPC connection
// just like grpc.WithInsecure() https://godoc.org/google.golang.org/grpc#WithInsecure
// does. Note, by default, client security is required unless WithInsecure is used.
func WithInsecure() ExporterOption {
	return func(e *Exporter) {
		e.canDialInsecure = true
	}
}

// WithAddress allows one to set the address that the exporter will
// connect to the agent on. If unset, it will instead try to use
// connect to DefaultAgentHost:DefaultAgentPort
func WithAddress(addr string) ExporterOption {
	return func(e *Exporter) {
		e.agentAddress = addr
	}
}

// WithServiceName allows one to set/override the service name
// that the exporter will report to the agent.
func WithServiceName(serviceName string) ExporterOption {
	return func(e *Exporter) {
		e.serviceName = serviceName
	}
}

// WithReconnectionPeriod sets the time after which the exporter will
// try to reconnect
func WithReconnectionPeriod(rp time.Duration) ExporterOption {
	return func(e *Exporter) {
		e.reconnectionPeriod = rp
	}
}

// UseCompressor will set the compressor for the gRPC client to use when sending requests.
// It is the responsibility of the caller to ensure that the compressor set has been registered
// with google.golang.org/grpc/encoding. This can be done by encoding.RegisterCompressor. Some
// compressors auto-register on import, such as gzip, which can be registered by calling
// `import _ "google.golang.org/grpc/encoding/gzip"`
func UseCompressor(compressorName string) ExporterOption {
	return func(e *Exporter) {
		e.compressor = &compressorName
	}
}
