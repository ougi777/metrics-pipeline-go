package logging

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewWritesJSONWithServiceFields(t *testing.T) {
	var output bytes.Buffer
	logger := New(&output, "api", "test-1", "info")

	logger.Info("started")

	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("log output is not JSON: %v", err)
	}
	if entry["service"] != "api" {
		t.Errorf("service = %v, want api", entry["service"])
	}
	if entry["instance_id"] != "test-1" {
		t.Errorf("instance_id = %v, want test-1", entry["instance_id"])
	}
}
