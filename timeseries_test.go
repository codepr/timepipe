package main

import "testing"

func testTimeSeriesNew(t *testing.T) {
	ts := NewTestSeries("test-ts", 3000)
	if ts == nil || ts.Name != "test-ts" || ts.Retention != 3000 {
		t.Errorf("Failed to create a new TimeSeries")
	}
}

func testTimeSeriesAddPoint(t *testing.T) {
	ts := NewTestSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	if len(ts.Records) != 1 {
		t.Errorf("Failed to add new point to TimeSeries")
	}
	if ts.Records[0].value != 98.2 {
		t.Errorf("Failed to add new point to TimeSeries")
	}
}
