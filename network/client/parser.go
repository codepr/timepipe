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
	"errors"
	"github.com/codepr/timepipe/network/protocol"
	"strconv"
	"strings"
)

const (
	CREATE = iota
	DELETE
	ADD
	MADD
	QUERY
)

var (
	EmptyCommandErr      = errors.New("empty command string")
	UnknownCommandErr    = errors.New("unknown command")
	CommandEndReachedErr = errors.New("command reached end, no new tokens available")
)

type timerange struct {
	start, end int64
}

type timeseries struct {
	Name      string
	Retention int64
}

type Command struct {
	Type       int
	TimeSeries timeseries
	Timestamp  int64
	Value      float64
	Range      timerange
	Flag       byte
}

type parser struct {
	tokens []string
	index  int
}

func NewParser(cmd string) parser {
	p := parser{}
	p.tokens = strings.Fields(cmd)
	return p
}

func (p *parser) peek() (string, error) {
	if p.index >= len(p.tokens) {
		return "", CommandEndReachedErr
	}
	return p.tokens[p.index], nil
}

func (p *parser) pop() (string, error) {
	if _, err := p.peek(); err != nil {
		return "", err
	}
	token := p.tokens[p.index]
	p.index += 1
	return token, nil
}

func (p *parser) Parse() (Command, error) {
	command := Command{}
	if len(p.tokens) == 0 {
		return command, EmptyCommandErr
	}
	token, err := p.pop()
	if err != nil {
		return command, err
	}
	ts := timeseries{}
	switch strings.ToUpper(token) {
	case "CREATE":
		command.Type = CREATE
		ts.Name, err = p.pop()
		if err != nil {
			return command, nil
		}
		token, err = p.pop()
		if err != nil {
			ts.Retention = 0
		} else {
			if ts.Retention, err = strconv.ParseInt(token, 10, 64); err != nil {
				return command, err
			}
		}
		command.TimeSeries = ts
	case "DELETE":
		command.Type = DELETE
		ts.Name, err = p.pop()
		if err != nil {
			return command, nil
		}
		command.TimeSeries = ts
	case "ADD":
		command.Type = ADD
		ts.Name, err = p.pop()
		if err != nil {
			return command, nil
		}
		token, err := p.pop()
		if err != nil {
			return command, nil
		}
		if token == "*" {
			command.Timestamp = 0
		} else {
			command.Timestamp, err = strconv.ParseInt(token, 10, 64)
			if err != nil {
				return command, err
			}
		}
		token, err = p.pop()
		if err != nil {
			return command, err
		}
		if command.Value, err = strconv.ParseFloat(token, 64); err != nil {
			return command, err
		}
		command.TimeSeries = ts
	case "MADD":
		command.Type = MADD
	case "QUERY":
		command.Type = QUERY
		ts.Name, err = p.pop()
		if err != nil {
			return command, err
		}
		token, err = p.pop()
		if token != "*" {
			switch strings.ToUpper(token) {
			case "MAX":
				command.Flag = protocol.MAX << 1
			case "MIN":
				command.Flag = protocol.MIN << 1
			case "FIRST":
				command.Flag = protocol.FIRST << 1
			case "LAST":
				command.Flag = protocol.LAST << 1
			case "<":
				endTs, err := p.pop()
				if err != nil {
					return command, err
				}
				if command.Range.end, err = parseTimestamp(endTs); err != nil {
					return command, err
				}
			case ">":
				startTs, err := p.pop()
				if err != nil {
					return command, err
				}
				if command.Range.start, err = parseTimestamp(startTs); err != nil {
					return command, err
				}
			case "RANGE":
				startTs, err := p.pop()
				if err != nil {
					return command, err
				}
				endTs, err := p.pop()
				if err != nil {
					return command, err
				}
				if command.Range.start, err = parseTimestamp(startTs); err != nil {
					return command, err
				}
				if command.Range.end, err = parseTimestamp(endTs); err != nil {
					return command, err
				}
			default:
				return command, UnknownCommandErr
			}
		}
		command.TimeSeries = ts
	default:
		return command, UnknownCommandErr
	}
	return command, nil
}

func parseTimestamp(str string) (int64, error) {
	var mul int64 = 1
	if len(str) == 10 {
		mul = 1e9
	} else if len(str) == 13 {
		mul = 1e6
	}
	val, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return -1, err
	}
	return val * mul, nil
}
