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

const (
	CREATE = iota
	DELETE
	ADDPOINT
	MADDPOINT
	QUERY
	QUERYRESPONSE
	ACK
)

const (
	OK = iota
	ACCEPTED
	TSNOTFOUND
	TSEXISTS
	UNKNOWNCMD
)

type AckResponse = Header

type Header struct {
	Value byte
	Size  uint64
}

func (h *Header) Len() uint64 {
	return h.Size
}

func (h *Header) Opcode() byte {
	return h.Value >> 4
}

func (h *Header) SetOpcode(opcode byte) {
	h.Value &= 0x0F
	h.Value |= ((opcode << 4) & 0xF0)
}

func (h *Header) Status() byte {
	return h.Value >> 1 & 0x03
}

func (h *Header) SetStatus(status byte) {
	h.Value |= status << 1
}

func (h Header) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, h); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *Header) UnmarshalBinary(buf []byte) error {
	// Read operation code
	b := bytes.NewReader(buf)
	var value uint8
	if err := binary.Read(b, binary.BigEndian, &value); err != nil {
		return err
	}
	// Read payload len in bytes
	var size uint64
	if err := binary.Read(b, binary.BigEndian, &size); err != nil {
		return err
	}
	h.Value = value
	h.Size = size
	return nil
}

func (header Header) String() string {
	var response string = ""
	switch header.Status() {
	case OK:
		response = "(ok)"
	case ACCEPTED:
		response = "(accepted)"
	case TSEXISTS:
		response = "(error) - timeseries already exists"
	case TSNOTFOUND:
		response = "(error) - timeseries not found"
	case UNKNOWNCMD:
		response = "(error) - unknown command"
	}
	return response
}
