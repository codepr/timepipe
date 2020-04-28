package main

import (
	"testing"
	"time"
)

func TestTimeSeriesNew(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	if ts == nil || ts.Name != "test-ts" || ts.Retention != 3000 {
		t.Errorf("Failed to create a new TimeSeries")
	}
}

func TestTimeSeriesAddPoint(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	record := ts.AddPoint(98.2)
	if len(ts.Records) != 1 {
		t.Errorf("Failed to add new point to TimeSeries")
	}
	if ts.Records[0].value != 98.2 || record.value != 98.2 {
		t.Errorf("Failed to add new point to TimeSeries")
	}
}

func TestTimeSeriesAverage(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	ts.AddPoint(106.2)
	ts.AddPoint(98.22)
	ts.AddPoint(91.2)
	avg, _ := ts.Average()
	if avg != 98.455 {
		t.Errorf("Wrong average calculated, expected %v got %v", 98.455, avg)
	}
}

func TestTimeSeriesMax(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	ts.AddPoint(106.2)
	ts.AddPoint(98.22)
	ts.AddPoint(91.2)
	max, _ := ts.Max()
	if max.value != 106.2 {
		t.Errorf("Wrong maximum calculated, expected %v got %v", 106.2, max.value)
	}
}

func TestTimeSeriesMin(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	ts.AddPoint(106.2)
	ts.AddPoint(98.22)
	ts.AddPoint(91.2)
	min, _ := ts.Min()
	if min.value != 91.2 {
		t.Errorf("Wrong maximum calculated, expected %v got %v", 91.2, min.value)
	}
}

func TestTimeSeriesFirst(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	ts.AddPoint(106.2)
	ts.AddPoint(98.22)
	ts.AddPoint(91.2)
	first, _ := ts.First()
	if first.value != 98.2 {
		t.Errorf("Wrong maximum calculated, expected %v got %v", 91.2, first.value)
	}
}

func TestTimeSeriesLast(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	ts.AddPoint(106.2)
	ts.AddPoint(98.22)
	ts.AddPoint(91.2)
	last, _ := ts.Last()
	if last.value != 91.2 {
		t.Errorf("Wrong maximum calculated, expected %v got %v", 91.2, last)
	}
}

func TestTimeSeriesRange(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	time.Sleep(200 * time.Millisecond)
	start := ts.AddPoint(106.2)
	ts.AddPoint(97.5)
	ts.AddPoint(91.2)
	time.Sleep(200 * time.Millisecond)
	end := ts.AddPoint(65.98)
	ts.AddPoint(77.0)
	records, _ := ts.Range(start.Timestamp.UnixNano(), end.Timestamp.UnixNano())
	if len(records) != 4 {
		t.Errorf("Wrong slice size returned, expected %v got %v", 4, len(records))
	}
}
