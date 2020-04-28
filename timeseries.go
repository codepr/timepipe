package main

import (
	"errors"
	"time"
)

var EmptyTimeSeriesErr = errors.New("no records in timeseries")

// Record represents a point in the timeseries, currently holds only a float
// value
type Record struct {
	Timestamp time.Time
	value     float64
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
func (ts *TimeSeries) AddPoint(value float64) Record {
	record := newRecord(value)
	ts.Records = append(ts.Records, record)
	return *record
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
		if record.Timestamp.UnixNano() >= lo && record.Timestamp.UnixNano() <= hi {
			result = append(result, *record)
		}
	}
	return result, nil
}
