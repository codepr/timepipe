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
	"errors"
	"time"
)

var (
	EmptyTimeSeriesErr = errors.New("no records in timeseries")
)

// Record represents a point in the timeseries, currently holds only a float
// value
type Record struct {
	Timestamp int64
	Value     float64
}

// TimeSeries represents a time series, essentially an append-only log of point
// values in time
type TimeSeries struct {
	Name      string
	Retention int64
	ctime     time.Time
	Records   []*Record
}

func newRecord(value float64) *Record {
	return &Record{time.Now().UnixNano(), value}
}

// NewTestSeries create a new TimeSeries by accepting a name and a retention value
func NewTimeSeries(name string, retention int64) *TimeSeries {
	return &TimeSeries{
		Name:      name,
		Retention: retention,
		ctime:     time.Now(),
		Records:   []*Record{},
	}
}

// AddPoint add a new point to an existing TimeSeries
func (ts *TimeSeries) AddPoint(value float64) Record {
	record := newRecord(value)
	ts.Records = append(ts.Records, record)
	return *record
}

func (ts *TimeSeries) AddRecord(record *Record) {
	ts.Records = append(ts.Records, record)
}

func (ts *TimeSeries) Average() (float64, error) {
	if len(ts.Records) == 0 {
		return 0.0, EmptyTimeSeriesErr
	}
	var sum float64 = 0.0
	for _, v := range ts.Records {
		sum += v.Value
	}
	return sum / float64(len(ts.Records)), nil
}

func (ts *TimeSeries) Max() (*Record, error) {
	if len(ts.Records) == 0 {
		return nil, EmptyTimeSeriesErr
	}
	max := ts.Records[0]
	for _, v := range ts.Records {
		if v.Value > max.Value {
			max = v
		}
	}
	return max, nil
}

func (ts *TimeSeries) Min() (*Record, error) {
	if len(ts.Records) == 0 {
		return nil, EmptyTimeSeriesErr
	}
	min := ts.Records[0]
	for _, v := range ts.Records {
		if v.Value < min.Value {
			min = v
		}
	}
	return min, nil
}

func (ts *TimeSeries) First() (*Record, error) {
	if len(ts.Records) == 0 {
		return nil, EmptyTimeSeriesErr
	}
	return ts.Records[0], nil
}

func (ts *TimeSeries) Last() (*Record, error) {
	if len(ts.Records) == 0 {
		return nil, EmptyTimeSeriesErr
	}
	last := len(ts.Records) - 1
	return ts.Records[last], nil
}

func (ts *TimeSeries) Range(lo, hi int64) ([]Record, error) {
	if len(ts.Records) == 0 {
		return nil, EmptyTimeSeriesErr
	}
	result := make([]Record, 0)
	for _, record := range ts.Records {
		if record.Timestamp >= lo && record.Timestamp <= hi {
			result = append(result, *record)
		}
	}
	return result, nil
}

func (ts *TimeSeries) Find(timestamp int64) (*Record, int) {
	if first, err := ts.First(); err != nil || first.Timestamp > timestamp {
		return nil, -1
	}
	if last, err := ts.Last(); err != nil || last.Timestamp < timestamp {
		return nil, -1
	}
	var mid, left, right int = 0, 0, len(ts.Records) - 1
	for left < right {
		mid = (left + right) / 2
		if ts.Records[mid].Timestamp < timestamp {
			left = mid
		} else if ts.Records[mid].Timestamp > timestamp {
			right = mid
		} else {
			return ts.Records[mid], mid
		}
	}
	return nil, -1
}

func (ts *TimeSeries) AverageInterval(interval_ms int64) ([]Record, error) {
	first, err := ts.First()
	if err != nil {
		return nil, err
	}
	last, err := ts.Last()
	if err != nil {
		return nil, err
	}
	interval := interval_ms * 1e6
	firstTs := (first.Timestamp / interval) * interval
	result := make([]Record, 0)
	var current int64 = firstTs + interval
	var sum float64 = 0.0
	var total int = 0
	for current < last.Timestamp {
		sum = 0.0
		total = 0
		for _, r := range ts.Records {
			if r.Timestamp > current-interval && r.Timestamp < current {
				sum += r.Value
				total += 1
			}
		}
		result = append(result, Record{current, sum / float64(total)})
		current += interval
	}
	return result, nil
}
