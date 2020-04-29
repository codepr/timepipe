package main

import (
	"bytes"
	"encoding/binary"
)

const (
	CREATE = iota
	DELETE
	ADDPOINT
	MADDPOINT
	QUERY
)

type Request interface {
	Pack() []byte
}

type Response interface {
	Pack() []byte
}

type Header struct {
	value uint8
	size  uint64
}

type AckResponse struct {
	header Header
}

func (h *Header) Len() uint64 {
	return h.size
}

func (h *Header) Opcode() uint8 {
	return h.value >> 4
}

func (h *Header) Status() uint8 {
	return h.value >> 6
}

func PackHeader(h *Header) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, h.value); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.size); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnpackHeader(buf []byte) (*Header, error) {
	// Read operation code
	b := bytes.NewReader(buf)
	var value uint8
	if err := binary.Read(b, binary.LittleEndian, &value); err != nil {
		return nil, err
	}

	// Read payload len in bytes
	var size uint64
	if err := binary.Read(b, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	return &Header{value, size}, nil
}

func UnpackRequest(buf []byte) (*Request, error) {
	// TODO
	return nil, nil
}

func (r AckResponse) Pack() ([]byte, error) {
	header, err := packHeader(&r.header)
	if err != nil {
		return nil, err
	}
	return header, nil
}
