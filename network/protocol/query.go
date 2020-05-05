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

type QueryPacket struct {
	Name  string
	Flags uint8
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
	return buf.Bytes(), nil
}

func (q *QueryPacket) Apply(ts *timeseries.TimeSeries) (encoding.BinaryMarshaler, error) {
	qr := &QueryResponsePacket{}
	qr.Records = make([]timeseries.Record, len(ts.Records))
	for i, v := range ts.Records {
		qr.Records[i] = *v
	}
	return qr, nil
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