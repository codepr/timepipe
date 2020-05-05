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
	"encoding/binary"
)

type CreatePacket struct {
	Name      string
	Retention int64
}

func (c *CreatePacket) UnmarshalBinary(buf []byte) error {
	reader := bytes.NewReader(buf)
	var nameLen uint16 = 0
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	name := make([]byte, nameLen)
	if err := binary.Read(reader, binary.BigEndian, &name); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &c.Retention); err != nil {
		return err
	}
	c.Name = string(name)
	return nil
}

func (c *CreatePacket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, uint16(len(c.Name))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, []byte(c.Name)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.Retention); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
