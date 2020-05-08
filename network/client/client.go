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

type TpResponse struct {
	Header  protocol.Header
	Command Command
	Payload protocol.QueryResponsePacket
}

func NewTimepipeClient(network, host, port string) (*Client, error) {
	conn, err := net.Dial(network, host+":"+port)
	if err != nil {
		return nil, err
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return &Client{host, port, conn, rw}, nil
}

func (c *Client) SendCommand(cmdString string) (*TpResponse, error) {
	parser := NewParser(cmdString)
	command, err := parser.Parse()
	if err != nil {
		return nil, err
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
		return nil, err
	}
	header.Size = uint64(len(payloadBytes))
	headerBytes, err := header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	packetBytes := append(headerBytes, payloadBytes...)
	_, err = c.conn.Write(packetBytes)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 9)
	if _, err := io.ReadAtLeast(c.rw, buf, 9); err != nil {
		return nil, err
	}
	responseHeader := protocol.Header{}
	if err := responseHeader.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	r := &TpResponse{}
	r.Command = command
	r.Header = responseHeader
	if responseHeader.Opcode() == protocol.ACK {
		return r, nil
	}
	payloadBuf := make([]byte, responseHeader.Len())
	if _, err := io.ReadAtLeast(c.rw, payloadBuf, len(payloadBuf)); err != nil {
		return nil, err
	}
	if err := r.Payload.UnmarshalBinary(payloadBuf); err != nil {
		return nil, err
	}
	return r, nil
}

func (c *Client) Close() {
	c.conn.Close()
}

func (r TpResponse) String() string {
	var response string = ""
	if r.Header.Opcode() == protocol.ACK {
		response = r.Header.String()
		switch r.Header.Status() {
		case protocol.TSEXISTS:
			fallthrough
		case protocol.TSNOTFOUND:
			response += fmt.Sprintf(": %s", r.Command.TimeSeries.Name)
		}
	} else {
		if len(r.Payload.Records) > 0 {
			response = "\n"
			response += fmt.Sprintf("name: %s\nretention: %d\n",
				r.Command.TimeSeries.Name, r.Command.TimeSeries.Retention)
			response += "timestamp\t\tvalue\n"
			response += "---------\t\t-----\n"
		}
		response += r.Payload.String()
	}
	return response
}
