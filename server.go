package main

import (
	"binary"
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
)

type Handler func(request *Request) Response

type Server struct {
	listener net.Listener
	handler  map[int]Handler
}

func NewServer() *Server {
	return &Server{handler: map[int]Handler{}}
}

func (s *Server) AddHandler(command int, f Handler) {
	s.handler[command] = f
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
		go s.handleRequest(<-ch)
	}
}

func (s *Server) handleRequest(conn net.Conn) {
	// Handle connection close
	defer func() {
		if err := conn.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	header, err := parseHeader(rw)
	if err != nil {
		log.Println("Error unpacking header")
	} else {
		for {
			if handle, ok := s.handler[int(header.Opcode())]; !ok {
				log.Print("Can't get handler")
			} else {
				// Read the bytes left, a.k.a. payload of the request
				buf := make([]byte, header.Len())
				if _, err := io.ReadAtLeast(rw, buf, int(header.Len())); err != nil {
					log.Fatal("Can't read remaining bytes left", err)
				}
				packet, err := UnpackRequest(buf)
				if err != nil {
					log.Println("Couldn't unpack request")
				}
				response := handle(packet)
				_, err = conn.Write(response.Pack())
				if err != nil {
					log.Print("Error sending response")
				}
			}
		}
	}
}

func parseHeader(rw *bufio.ReadWriter) (*Header, error) {
	buf := make([]byte, 9)
	if _, err := io.ReadAtLeast(rw, buf, 9); err != nil {
		return nil, err
	}
	header := &Header{}
	if err := header.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return header, nil
}
