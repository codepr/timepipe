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
	"bufio"
	"encoding"
	"fmt"
	"github.com/codepr/timepipe/network/protocol"
	"io"
	"net"
)

type Client struct {
	host, port string
	conn       net.Conn
	rw         *bufio.ReadWriter
}

func NewTimepipeClient(network, host, port string) (*Client, error) {
	conn, err := net.Dial(network, host+":"+port)
	if err != nil {
		return nil, err
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return &Client{host, port, conn, rw}, nil
}

func (c *Client) SendCommand(cmdString string) (string, error) {
	var response string = ""
	parser := NewParser(cmdString)
	command, err := parser.Parse()
	if err != nil {
		return "", err
	}
	header := protocol.Header{}
	header.SetOpcode(uint8(command.Type))
	var payload encoding.BinaryMarshaler
	switch command.Type {
	case CREATE:
		packet := protocol.CreatePacket{}
		packet.Name = command.TimeSeries.Name
		packet.Retention = command.TimeSeries.Retention
		payload = &packet
	case DELETE:
		packet := protocol.DeletePacket{}
		packet.Name = command.TimeSeries.Name
		payload = &packet
	case ADD:
		packet := protocol.AddPointPacket{}
		packet.Name = command.TimeSeries.Name
		packet.HaveTimestamp = command.Timestamp != 0
		packet.Value = command.Value
		payload = &packet
	case QUERY:
		packet := protocol.QueryPacket{}
		packet.Name = command.TimeSeries.Name
		packet.Flags = command.Flag
		packet.Range[0] = command.Range.start
		packet.Range[1] = command.Range.end
		packet.Avg = command.Avg
		payload = &packet
		// TODO
	}
	payloadBytes, err := payload.MarshalBinary()
	if err != nil {
		return "", err
	}
	header.Size = uint64(len(payloadBytes))
	headerBytes, err := header.MarshalBinary()
	if err != nil {
		return "", err
	}
	packetBytes := append(headerBytes, payloadBytes...)
	_, err = c.conn.Write(packetBytes)
	if err != nil {
		return "", err
	}
	buf := make([]byte, 9)
	if _, err := io.ReadAtLeast(c.rw, buf, 9); err != nil {
		return "", err
	}
	responseHeader := protocol.Header{}
	if err := responseHeader.UnmarshalBinary(buf); err != nil {
		return "", err
	}
	if responseHeader.Opcode() == protocol.ACK {
		switch responseHeader.Status() {
		case protocol.OK:
			response = "OK"
		case protocol.ACCEPTED:
			response = "ACCEPTED"
		case protocol.TSEXISTS:
			response = "Timeseries already exists"
		case protocol.TSNOTFOUND:
			response = "Timeseries not found"
		case protocol.UNKNOWNCMD:
			response = "Unknown command"
		}
	} else {
		payloadBuf := make([]byte, responseHeader.Len())
		if _, err := io.ReadAtLeast(c.rw, payloadBuf, len(payloadBuf)); err != nil {
			return "", err
		}
		res := protocol.QueryResponsePacket{}
		if err := res.UnmarshalBinary(payloadBuf); err != nil {
			return "", err
		}
		if len(res.Records) == 0 {
			response = "(empty)"
		} else {
			response += "\n"
			for i := 0; i < len(res.Records); i++ {
				response += fmt.Sprintf("%v %f\n",
					res.Records[i].Timestamp, res.Records[i].Value)
			}
		}
	}
	return response, nil
}

func (c *Client) Close() {
	c.conn.Close()
}
