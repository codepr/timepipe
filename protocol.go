package main

const (
	CREATE = iota
	DELETE
	ADDPOINT
	MADDPOINT
	QUERY
)

type Response interface {
	Pack() []byte
}

type Header struct {
	opcode uint8
	size   uint64
}
