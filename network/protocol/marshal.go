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

import "encoding"

type Response struct {
	header  Header
	payload encoding.BinaryMarshaler
}

func UnmarshalBinary(buf []byte, u encoding.BinaryUnmarshaler) error {
	return u.UnmarshalBinary(buf)
}

func MarshalBinary(m encoding.BinaryMarshaler) ([]byte, error) {
	return m.MarshalBinary()
}

func MarshalBinaryFull(opcode uint8, m encoding.BinaryMarshaler) ([]byte, error) {
	bytesarray, err := m.MarshalBinary()
	if err != nil {
		return nil, err
	}
	header := Header{opcode, uint64(len(bytesarray))}
	byteshdr, err := header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(byteshdr, bytesarray...), err
}

func (r *Response) MarshalBinary() ([]byte, error) {
	payloadBytes, err := r.payload.MarshalBinary()
	if err != nil {
		return nil, err
	}
	r.header.Size = uint64(len(payloadBytes))
	headerBytes, err := r.header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(headerBytes, payloadBytes...), nil
}
