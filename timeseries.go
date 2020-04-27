package main

import "time"

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

func (ts *TimeSeries) Average() float64 {
	var sum float64 = 0.0
	for _, v := range ts.Records {
		sum += v.value
	}
	return sum / float64(len(ts.Records))
}
