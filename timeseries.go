package main

import (
	"errors"
	"time"
)

var EmptyTimeSeriesErr = errors.New("no records in timeseries")

// Record struct
type Record struct {
	Timestamp time.Time
	value     float64
}

// TimeSeries struct
type TimeSeries struct {
	Name      string
	Retention int64
	ctime     time.Time
	Records   []*Record
}

func newRecord(value float64) *Record {
	return &Record{time.Now(), value}
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
func (ts *TimeSeries) AddPoint(value float64) {
	record := newRecord(value)
	ts.Records = append(ts.Records, record)
}

func (ts *TimeSeries) Average() (float64, error) {
	if len(ts.Records) == 0 {
		return 0.0, EmptyTimeSeriesErr
	}
	var sum float64 = 0.0
	for _, v := range ts.Records {
		sum += v.value
	}
	return sum / float64(len(ts.Records)), nil
}

func (ts *TimeSeries) Max() (*Record, error) {
	if len(ts.Records) == 0 {
		return nil, EmptyTimeSeriesErr
	}
	max := ts.Records[0]
	for _, v := range ts.Records {
		if v.value > max.value {
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
		if v.value < min.value {
			min = v
		}
	}
	return min, nil
}
