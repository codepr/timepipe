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

import "testing"

func TestQueryFlagMin(t *testing.T) {
	q := QueryPacket{}
	q.Flags = 0
	if q.Min() == false && q.Last() != false &&
		q.Max() != false && q.First() != false {
		t.Errorf("Failed QUERY MIN flag check")
	}
}

func TestQueryFlagMax(t *testing.T) {
	q := QueryPacket{}
	q.Flags = MAX << 1
	if q.Max() == false && q.Last() != false &&
		q.Min() != false && q.First() != false {
		t.Errorf("Failed QUERY MAX flag check")
	}
}

func TestQueryFlagFirst(t *testing.T) {
	q := QueryPacket{}
	q.Flags = FIRST << 1
	if q.First() == false && q.Last() != false &&
		q.Max() != false && q.Min() != false {
		t.Errorf("Failed QUERY FIRST flag check")
	}
}

func TestQueryFlagLast(t *testing.T) {
	q := QueryPacket{}
	q.Flags = LAST << 1
	if q.Last() == false && q.First() != false &&
		q.Min() != false && q.Max() != false {
		t.Errorf("Failed QUERY LAST flag check")
	}
}
