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

package protocol

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"github.com/codepr/timepipe/timeseries"
)

const (
	MIN   = 1
	MAX   = 2
	FIRST = 3
	LAST  = 4
)

type QueryPacket struct {
	Name  string
	Flags byte
	Range [2]int64
	Avg   int64
}

func (q *QueryPacket) Min() bool {
	return q.Flags>>1&0x03 == MIN
}

func (q *QueryPacket) Max() bool {
	return q.Flags>>1&0x03 == MAX
}

func (q *QueryPacket) First() bool {
	return q.Flags>>1&0x03 == FIRST
}

func (q *QueryPacket) Last() bool {
	return q.Flags>>1&0x03 == LAST
}

type QueryResponsePacket struct {
	Records []timeseries.Record
}

func (q *QueryPacket) UnmarshalBinary(buf []byte) error {
	reader := bytes.NewReader(buf)
	var nameLen uint16 = 0
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	name := make([]byte, nameLen)
	if err := binary.Read(reader, binary.BigEndian, &name); err != nil {
		return err
	}
	q.Name = string(name)
	if err := binary.Read(reader, binary.BigEndian, &q.Flags); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &q.Range); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &q.Avg); err != nil {
		return err
	}
	return nil
}

func (q *QueryPacket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, uint16(len(q.Name)))
	if err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, []byte(q.Name)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, q.Flags); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, q.Range); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, q.Avg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (q *QueryPacket) Apply(ts *timeseries.TimeSeries) (encoding.BinaryMarshaler, error) {
	qr := &QueryResponsePacket{}
	if q.Max() {
		qr.Records = make([]timeseries.Record, 1)
		r, err := ts.Max()
		if err != nil {
			return qr, nil
		}
		qr.Records[0] = *r
	} else if q.Min() {
		qr.Records = make([]timeseries.Record, 1)
		r, err := ts.Min()
		if err != nil {
			return qr, nil
		}
		qr.Records[0] = *r
	} else if q.First() {
		qr.Records = make([]timeseries.Record, 1)
		r, err := ts.First()
		if err != nil {
			return qr, nil
		}
		qr.Records[0] = *r
	} else if q.Last() {
		qr.Records = make([]timeseries.Record, 1)
		r, err := ts.Last()
		if err != nil {
			return qr, nil
		}
		qr.Records[0] = *r
	} else {
		var (
			tmp *timeseries.TimeSeries = nil
			err error                  = nil
		)
		if q.Range[0] != 0 && q.Range[1] != 0 {
			tmp, err = ts.Range(q.Range[0], q.Range[1])
			if err != nil {
				return qr, nil
			}
		} else if q.Range[0] != 0 {
			last, err := ts.Last()
			if err != nil {
				return qr, nil
			}
			tmp, err = ts.Range(q.Range[0], last.Timestamp)
			if err != nil {
				return qr, nil
			}
		} else if q.Range[1] != 0 {
			first, err := ts.First()
			if err != nil {
				return qr, nil
			}
			tmp, err = ts.Range(first.Timestamp, q.Range[1])
			if err != nil {
				return qr, nil
			}
		} else {
			tmp = ts
		}
		if q.Avg == 0 {
			val, err := tmp.Average()
			if err != nil {
				return qr, nil
			}
			qr.Records = make([]timeseries.Record, 1)
			qr.Records[0] = timeseries.Record{Timestamp: 0, Value: val}
		} else if q.Avg > 0 {
			records, err := tmp.AverageInterval(q.Avg)
			if err != nil {
				return qr, nil
			}
			qr.Records = make([]timeseries.Record, len(records))
			for i, v := range records {
				qr.Records[i] = v
			}
		} else {
			qr.Records = make([]timeseries.Record, tmp.Len())
			for i, v := range tmp.Records {
				qr.Records[i] = *v
			}
		}
	}
	header := Header{}
	header.SetOpcode(QUERYRESPONSE)
	header.SetStatus(OK)
	response := &Response{header, qr}
	return response, nil
}

func (qr *QueryResponsePacket) UnmarshalBinary(buf []byte) error {
	reader := bytes.NewReader(buf)
	var results uint64 = 0
	if err := binary.Read(reader, binary.BigEndian, &results); err != nil {
		return err
	}
	qr.Records = make([]timeseries.Record, results)
	var i uint64 = 0
	for ; i < results; i++ {
		err := binary.Read(reader, binary.BigEndian, &qr.Records[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (qr *QueryResponsePacket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, uint64(len(qr.Records)))
	if err != nil {
		return nil, err
	}
	for _, v := range qr.Records {
		err := binary.Write(buf, binary.BigEndian, v.Timestamp)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.BigEndian, v.Value)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
