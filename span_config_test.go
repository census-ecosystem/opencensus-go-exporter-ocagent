package ocagent

import "testing"

func TestSpanConfig_GetAnnotationEventsPerSpan(t *testing.T) {
	tests := [] struct{
		name string
		inputSpanConfig SpanConfig
		expectedMaxAnnotations int
	}{
		{
			name: "WhenConfigNotProvided",
			inputSpanConfig: SpanConfig{},
			expectedMaxAnnotations: 32,

		},
		{
			name: "WhenConfigProvided",
			inputSpanConfig: SpanConfig{AnnotationEventsPerSpan: 256},
			expectedMaxAnnotations: 256,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.inputSpanConfig.GetAnnotationEventsPerSpan()
			if got != test.expectedMaxAnnotations {
				t.Errorf("Got = %d; want %d", got, test.expectedMaxAnnotations)
			}
		})
	}
}

func TestSpanConfig_GetMessageEventsPerSpan(t *testing.T) {
	tests := [] struct{
		name string
		inputSpanConfig SpanConfig
		expectedMaxMessageEvents int
	}{
		{
			name: "WhenConfigNotProvided",
			inputSpanConfig: SpanConfig{},
			expectedMaxMessageEvents: 128,

		},
		{
			name: "WhenConfigProvided",
			inputSpanConfig: SpanConfig{MessageEventsPerSpan: 256},
			expectedMaxMessageEvents: 256,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.inputSpanConfig.GetMessageEventsPerSpan()
			if got != test.expectedMaxMessageEvents {
				t.Errorf("Got = %d; want %d", got, test.expectedMaxMessageEvents)
			}
		})
	}
}
