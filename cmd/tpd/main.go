package main

import "github.com/codepr/timepipe/network"

const (
	TYPE = "tcp"
	HOST = "localhost"
	PORT = "4040"
)

func main() {
	server := network.NewServer(TYPE, HOST, PORT)
	server.Run()
}
