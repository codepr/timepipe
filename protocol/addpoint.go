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

type AddPointPacket struct {
	Name          string
	HaveTimestamp bool
	Value         float64
	Timestamp     int64
}

func (a *AddPointPacket) UnmarshalBinary(buf []byte) error {
	r := bytes.NewReader(buf)
	var nameLen uint16 = 0
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	name := make([]byte, nameLen)
	if err := binary.Read(r, binary.BigEndian, &name); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &a.HaveTimestamp); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &a.Value); err != nil {
		return err
	}
	if a.HaveTimestamp == true {
		if err := binary.Read(r, binary.BigEndian, &a.Timestamp); err != nil {
			return err
		}
	}
	a.Name = string(name)
	return nil
}

func (a AddPointPacket) Apply(ts *timeseries.TimeSeries) (encoding.BinaryMarshaler, error) {
	record := &timeseries.Record{a.Timestamp, a.Value}
	ts.AddRecord(record)
	r := Header{}
	r.SetOpcode(ACK)
	r.SetStatus(OK)
	return r, nil
}
