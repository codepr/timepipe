package main

import (
	"bufio"
	"encoding"
	"io"
	"log"
	"net"
)

const (
	TYPE = "tcp"
	HOST = "localhost"
	PORT = "4040"
)

type Server struct {
	listener net.Listener
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Run(protocol, host, port string) {
	l, err := net.Listen(protocol, host+":"+port)
	if err != nil {
		log.Fatal(err)
	}

	defer l.Close()

	log.Print("Listening on " + host + ":" + port)

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
		response := handleRequest(rw, header)
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

func handleRequest(rw *bufio.ReadWriter, h *Header) encoding.BinaryMarshaler {
	response := AckResponse{
		header: Header{ACK, 0},
	}
	// Read the bytes left, a.k.a. payload of the request
	buf := make([]byte, h.Len())
	if _, err := io.ReadAtLeast(rw, buf, int(h.Len())); err != nil {
		log.Fatal("Can't read remaining bytes left", err)
	}
	switch h.Opcode() {
	case CREATE:
		// TODO
	case DELETE:
		// TODO
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
	server := NewServer()
	server.Run(TYPE, HOST, PORT)
}
