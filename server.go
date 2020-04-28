package main

import (
	"bufio"
	"log"
	"net"
	"sync"
)

type Handler func(*bufio.ReadWriter) Response

type Server struct {
	sync.RWMutex
	listener net.Listener
	handler  map[int]Handler
}

func NewServer() *Server {
	return &Server{handler: map[int]Handler{}}
}

func (s *Server) AddHandler(command int, f Handler) {
	s.Lock()
	s.handler[command] = f
	s.Unlock()
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

	for {
		s.RLock()
		handle, ok := s.handler[0x01]
		if !ok {
			log.Print("Can't get handler")
		}
		s.RUnlock()

		response := handle(rw)

		_, err := conn.Write(response.Pack())
		if err != nil {
			log.Print("Error sending response")
		}
	}
}
