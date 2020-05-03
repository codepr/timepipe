package main

import (
	"bufio"
	"encoding"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	TYPE = "tcp"
	HOST = "localhost"
	PORT = "4040"
)

type Server struct {
	protocol string
	host     string
	port     string
	db       *sync.Map
	rw       chan *TimeSeriesOperation
	out      chan ServerResponse
}

func NewServer(protocol, host, port string) *Server {
	return &Server{
		protocol: protocol,
		host:     host,
		port:     port,
		db:       new(sync.Map),
		rw:       make(chan *TimeSeriesOperation),
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
	go processRequests(s.rw, s.out)

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

func (s *Server) handleRequest(conn *net.Conn, rw *bufio.ReadWriter, h *Header) {
	response := Header{}
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
		} else {
			log.Println("Created new timeseries named " + timeseries.Name)
		}
		s.out <- ServerResponse{conn, response}
	case DELETE:
		delete := &CreatePacket{}
		if err := UnmarshalBinary(buf, delete); err != nil {
			log.Fatal("UnmarshalBinary:", err)
		}
		s.db.Delete(delete.Name)
		log.Println("Deleted timeseries named " + delete.Name)
		s.out <- ServerResponse{conn, response}
	case ADDPOINT:
		add := &AddPointPacket{}
		if err := UnmarshalBinary(buf, add); err != nil {
			log.Fatal("UnmarshalBinary: ", err)
		}
		if add.HaveTimestamp == false {
			add.Timestamp = time.Now().UnixNano()
		}
		log.Println(add)
	case MADDPOINT:
		log.Println("Received MADDPOINT")
		// TODO
	case QUERY:
		log.Println("Received QUERY")
		// TODO
	default:
		response.SetStatus(UNKNOWNCMD)
		s.out <- ServerResponse{conn, response}
		// TODO
	}
}

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

func processRequests(rw chan *TimeSeriesOperation, out chan ServerResponse) {
	for {
		op := <-rw
		response, err := op.Operation.Apply(op.TimeSeries)
		if err != nil {
			// FIXME remove fatal, marshal error
			log.Fatal(err)
		}
		out <- ServerResponse{op.Conn, response}
	}
}

func main() {
	server := NewServer(TYPE, HOST, PORT)
	server.Run()
}
