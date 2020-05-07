// BSD 2-Clause License
//
// Copyright (c) 2020, Andrea Giacomo Baldan
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package timeseries

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
	if ts.Records[0].Value != 98.2 || record.Value != 98.2 {
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
	if max.Value != 106.2 {
		t.Errorf("Wrong maximum calculated, expected %v got %v", 106.2, max.Value)
	}
}

func TestTimeSeriesMin(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	ts.AddPoint(106.2)
	ts.AddPoint(98.22)
	ts.AddPoint(91.2)
	min, _ := ts.Min()
	if min.Value != 91.2 {
		t.Errorf("Wrong maximum calculated, expected %v got %v", 91.2, min.Value)
	}
}

func TestTimeSeriesFirst(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	ts.AddPoint(106.2)
	ts.AddPoint(98.22)
	ts.AddPoint(91.2)
	first, _ := ts.First()
	if first.Value != 98.2 {
		t.Errorf("Wrong maximum calculated, expected %v got %v",
			91.2, first.Value)
	}
}

func TestTimeSeriesLast(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	ts.AddPoint(106.2)
	ts.AddPoint(98.22)
	ts.AddPoint(91.2)
	last, _ := ts.Last()
	if last.Value != 91.2 {
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
	resTs, _ := ts.Range(start.Timestamp, end.Timestamp)
	if len(resTs.Records) != 4 {
		t.Errorf("Wrong slice size returned, expected %v got %v", 4, len(resTs.Records))
	}
}

func TestTimeSeriesFind(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	time.Sleep(200 * time.Millisecond)
	start := ts.AddPoint(106.2)
	ts.AddPoint(97.5)
	ts.AddPoint(91.2)
	time.Sleep(200 * time.Millisecond)
	ts.AddPoint(65.98)
	ts.AddPoint(77.0)
	record, _ := ts.Find(start.Timestamp)
	if record == nil || record.Value != 106.2 {
		t.Errorf("Find failed")
	}
}

func TestTimeSeriesAverageInterval(t *testing.T) {
	ts := NewTimeSeries("test-ts", 3000)
	ts.AddPoint(98.2)
	time.Sleep(200 * time.Millisecond)
	ts.AddPoint(106.2)
	ts.AddPoint(97.5)
	ts.AddPoint(91.2)
	time.Sleep(200 * time.Millisecond)
	ts.AddPoint(65.98)
	ts.AddPoint(77.0)
	records, _ := ts.AverageInterval(200)
	if len(records) != 2 {
		t.Errorf("AverageInterval return the wrong points, expected %v got %v",
			2, len(records))
	}
}
