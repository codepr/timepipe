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

package network

import (
	"bufio"
	"encoding"
	. "github.com/codepr/timepipe/network/protocol"
	. "github.com/codepr/timepipe/timeseries"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type TimeSeriesOperation struct {
	Conn       *net.Conn
	TimeSeries *TimeSeries
	Operation  TimeSeriesApplicable
}

type ServerResponse struct {
	Conn    *net.Conn
	Payload encoding.BinaryMarshaler
}

type TimeSeriesApplicable interface {
	Apply(*TimeSeries) (encoding.BinaryMarshaler, error)
}

type Server struct {
	protocol string
	host     string
	port     string
	db       *sync.Map
	r        chan *TimeSeriesOperation
	w        chan *TimeSeriesOperation
	out      chan ServerResponse
}

func NewServer(protocol, host, port string) *Server {
	return &Server{
		protocol: protocol,
		host:     host,
		port:     port,
		db:       new(sync.Map),
		r:        make(chan *TimeSeriesOperation),
		w:        make(chan *TimeSeriesOperation),
		out:      make(chan ServerResponse),
	}
}

func (s *Server) Run() {
	l, err := net.Listen(s.protocol, s.host+":"+s.port)
	if err != nil {
		log.Fatal(err)
	}

	defer l.Close()

	log.Print("Listening on " + s.host + ":" + s.port)

	ch := make(chan net.Conn)

	// Start single goroutine responsible for timeseries management
	go s.processRequests()

	// Start goroutine for responses
	go func() {
		for {
			response := <-s.out
			data, err := response.Payload.MarshalBinary()
			if err != nil {
				log.Print(err)
				return
			}
			_, err = (*response.Conn).Write(data)
			if err != nil {
				log.Print("Error sending response")
			}
		}
	}()

	// Scale on accept
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			log.Print("Connection accepted")
			ch <- conn
		}
	}()

	// And on process as well
	for {
		go s.serveConn(<-ch)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	// Handle connection close
	defer func() {
		if err := conn.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		buf := make([]byte, 9)
		if _, err := io.ReadAtLeast(rw, buf, 9); err != nil {
			log.Print("Can't read header bytes:", err)
			return
		}
		header := &Header{}
		if err := header.UnmarshalBinary(buf); err != nil {
			log.Print("Can't unmarshal header:", err)
			return
		}
		s.handleRequest(&conn, rw, header)
	}
}

func (s *Server) handleRequest(conn *net.Conn,
	rw *bufio.ReadWriter, h *Header) {
	response := AckResponse{}
	response.SetOpcode(ACK)
	// Read the bytes left, a.k.a. payload of the request
	buf := make([]byte, h.Len())
	if _, err := io.ReadAtLeast(rw, buf, int(h.Len())); err != nil {
		log.Fatal("Can't read remaining bytes left", err)
	}
	switch h.Opcode() {
	case CREATE:
		create := CreatePacket{}
		if err := UnmarshalBinary(buf, &create); err != nil {
			log.Fatal("UnmarshalBinary:", err)
		}
		timeseries := NewTimeSeries(create.Name, create.Retention)
		if _, ok := s.db.LoadOrStore(create.Name, timeseries); ok {
			log.Println("Timeseries named " + timeseries.Name + " already exists")
			response.SetStatus(TSEXISTS)
		} else {
			log.Println("Created new timeseries named " + timeseries.Name)
			response.SetStatus(OK)
		}
		s.out <- ServerResponse{conn, response}
	case DELETE:
		delete := &DeletePacket{}
		if err := UnmarshalBinary(buf, delete); err != nil {
			log.Fatal("UnmarshalBinary:", err)
		}
		s.db.Delete(delete.Name)
		log.Println("Deleted timeseries named " + delete.Name)
		response.SetStatus(OK)
		s.out <- ServerResponse{conn, response}
	case ADDPOINT:
		add := AddPointPacket{}
		if err := UnmarshalBinary(buf, &add); err != nil {
			log.Fatal("UnmarshalBinary: ", err)
		}
		log.Println("Received ADDPOINT on " + add.Name)
		if add.HaveTimestamp == false {
			add.Timestamp = time.Now().UnixNano()
		}
		ts, ok := s.db.Load(add.Name)
		if !ok {
			response.SetStatus(TSNOTFOUND)
		} else {
			s.w <- &TimeSeriesOperation{conn, ts.(*TimeSeries), &add}
			response.SetStatus(ACCEPTED)
		}
		s.out <- ServerResponse{conn, response}
	case MADDPOINT:
		log.Println("Received MADDPOINT")
		// TODO
	case QUERY:
		query := QueryPacket{}
		if err := UnmarshalBinary(buf, &query); err != nil {
			log.Fatal("UnmarshalBinary: ", err)
		}
		ts, ok := s.db.Load(query.Name)
		if !ok {
			response := AckResponse{}
			response.SetOpcode(QUERYRESPONSE)
			response.SetStatus(TSNOTFOUND)
			s.out <- ServerResponse{conn, response}
		} else {
			s.r <- &TimeSeriesOperation{conn, ts.(*TimeSeries), &query}
		}
	default:
		response.SetStatus(UNKNOWNCMD)
		s.out <- ServerResponse{conn, response}
		// TODO
	}
}

func (s *Server) processRequests() {
	for {
		select {
		case r := <-s.r:
			response, err := r.Operation.Apply(r.TimeSeries)
			if err != nil {
				// FIXME remove fatal, marshal error
				log.Fatal(err)
			}
			s.out <- ServerResponse{r.Conn, response}
		case w := <-s.w:
			if _, err := w.Operation.Apply(w.TimeSeries); err != nil {
				// FIXME remove fatal, marshal error
				log.Fatal(err)
			}
		}
	}
}
