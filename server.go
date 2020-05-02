package main

import (
	"bufio"
	"encoding"
	"io"
	"log"
	"net"
	"sync"
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
}

func NewServer(protocol, host, port string) *Server {
	return &Server{
		protocol: protocol,
		host:     host,
		port:     port,
		db:       new(sync.Map),
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
		response := s.handleRequest(rw, header)
		data, err := response.MarshalBinary()
		if err != nil {
			log.Print(err)
			return
		}
		_, err = conn.Write(data)
		if err != nil {
			log.Print("Error sending response")
		}
	}
}

func (s *Server) handleRequest(rw *bufio.ReadWriter,
	h *Header) encoding.BinaryMarshaler {
	response := &Header{ACK, 0}
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
	case DELETE:
		delete := &CreatePacket{}
		if err := UnmarshalBinary(buf, delete); err != nil {
			log.Fatal("UnmarshalBinary:", err)
		}
		s.db.Delete(delete.Name)
		log.Println("Deleted timeseries named " + delete.Name)
	case ADDPOINT:
		// TODO
	case MADDPOINT:
		// TODO
	case QUERY:
		// TODO
	default:
		// TODO
	}
	return response
}

func main() {
	server := NewServer(TYPE, HOST, PORT)
	server.Run()
}
