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

package client

import (
	"strconv"
	"testing"
	"time"
)

func TestParseCreate(t *testing.T) {
	parser := NewParser("CREATE ts-test 3000")
	command, err := parser.Parse()
	if err != nil {
		t.Errorf("Failed to parse CREATE query")
	}
	if command.Type != CREATE || command.TimeSeries.Name != "ts-test" {
		t.Errorf("Failed to parse CREATE query")
	}
}

func TestParseCreateWithNoRetention(t *testing.T) {
	parser := NewParser("CREATE ts-test")
	command, err := parser.Parse()
	if err != nil {
		t.Errorf("Failed to parse CREATE query")
	}
	expected := Command{CREATE, timeseries{"ts-test", 0}, 0, 0, timerange{}}
	if command != expected {
		t.Errorf("Failed to parse CREATE query")
	}
}

func TestParseDelete(t *testing.T) {
	parser := NewParser("DELETE ts-test")
	command, err := parser.Parse()
	if err != nil {
		t.Errorf("Failed to parse DELETE query")
	}
	if command.Type != DELETE || command.TimeSeries.Name != "ts-test" {
		t.Errorf("Failed to parse DELETE query")
	}
}

func TestParseAdd(t *testing.T) {
	parser := NewParser("ADD ts-test * 12.2")
	command, err := parser.Parse()
	if err != nil {
		t.Errorf("Failed to parse ADD query")
	}
	expected := Command{ADD, timeseries{"ts-test", 0}, 0, 12.2, timerange{}}
	if command != expected {
		t.Errorf("Failed to parse ADD query")
	}
}

func TestParseAddWithTimestamp(t *testing.T) {
	now := time.Now().UnixNano()
	parser := NewParser("ADD ts-test " + strconv.FormatInt(now, 10) + " 12.2")
	command, err := parser.Parse()
	if err != nil {
		t.Errorf("Failed to parse ADD query")
	}
	expected := Command{ADD, timeseries{"ts-test", 0}, now, 12.2, timerange{}}
	if command != expected {
		t.Errorf("Failed to parse ADD query")
	}
}
