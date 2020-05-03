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
	"testing"
)

func TestMarshalBinaryCreate(t *testing.T) {
	create := CreatePacket{"test-ts", 3000}
	b, err := MarshalBinary(&create)
	if err != nil {
		t.Errorf("Failed to marshal CREATE packet. Got error %v", err)
	}
	expected := []byte{0, 7, 116, 101, 115, 116, 45, 116, 115, 0, 0, 0, 0, 0, 0, 11, 184}
	res := bytes.Compare(b, expected)
	if res != 0 {
		t.Errorf("Failed to marshal CREATE. Expected %v got %v", expected, b)
	}
	test := CreatePacket{}
	UnmarshalBinary(b, &test)
	if test.Name != create.Name || test.Retention != create.Retention {
		t.Errorf("Failed to marshal CREATE packet. Expected %v got %v",
			create, test)
	}
}

func TestMarshalBinaryDelete(t *testing.T) {
	delete := DeletePacket{"test-ts"}
	b, err := MarshalBinary(&delete)
	if err != nil {
		t.Errorf("Failed to marshal DELETE packet. Got error %v", err)
	}
	expected := []byte{0, 7, 116, 101, 115, 116, 45, 116, 115}
	res := bytes.Compare(b, expected)
	if res != 0 {
		t.Errorf("Failed to marshal DELETE. Expected %v got %v", expected, b)
	}
	test := DeletePacket{}
	UnmarshalBinary(b, &test)
	if test.Name != delete.Name {
		t.Errorf("Failed to marshal DELETE packet. Expected %v got %v",
			delete, test)
	}
}

func TestMarshalBinaryAddPoint(t *testing.T) {
	add := AddPointPacket{"test-ts", false, 2.29, 0}
	b, err := MarshalBinary(&add)
	if err != nil {
		t.Errorf("Failed to marshal ADDPOINT packet. Got error %v", err)
	}
	expected := []byte{0, 7, 116, 101, 115, 116, 45, 116, 115, 0, 64, 2, 81, 235, 133, 30, 184, 82, 0, 0, 0, 0, 0, 0, 0, 0}
	res := bytes.Compare(b, expected)
	if res != 0 {
		t.Errorf("Failed to marshal ADDPOINT. Expected %v got %v", expected, b)
	}
	test := AddPointPacket{}
	UnmarshalBinary(b, &test)
	if test != add {
		t.Errorf("Failed to marshal ADDPOINT packet. Expected %v got %v",
			add, test)
	}
}

func TestMarshalBinaryQuery(t *testing.T) {
	query := QueryPacket{"test-ts", 0}
	b, err := MarshalBinary(&query)
	if err != nil {
		t.Errorf("Failed to marshal QUERY packet. Got error %v", err)
	}
	expected := []byte{0, 7, 116, 101, 115, 116, 45, 116, 115, 0}
	res := bytes.Compare(b, expected)
	if res != 0 {
		t.Errorf("Failed to marshal QUERY. Expected %v got %v", expected, b)
	}
	test := QueryPacket{}
	UnmarshalBinary(b, &test)
	if test != query {
		t.Errorf("Failed to marshal DELETE packet. Expected %v got %v",
			query, test)
	}
}
