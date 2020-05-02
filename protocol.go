package main

import (
	"bytes"
	"encoding"
	"encoding/binary"
)

const (
	CREATE = iota
	DELETE
	ADDPOINT
	MADDPOINT
	QUERY
	ACK
)

type Header struct {
	value uint8
	size  uint64
}

type CreatePacket struct {
	header    Header
	Name      string
	Retention int64
}

type DeletePacket struct {
	header Header
	Name   string
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

func (h *Header) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, h.value); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, h.size); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *Header) UnmarshalBinary(buf []byte) error {
	// Read operation code
	b := bytes.NewReader(buf)
	var value uint8
	if err := binary.Read(b, binary.LittleEndian, &value); err != nil {
		return err
	}

	// Read payload len in bytes
	var size uint64
	if err := binary.Read(b, binary.LittleEndian, &size); err != nil {
		return err
	}

	h.value = value
	h.size = size
	return nil
}

func (c CreatePacket) UnmarshalBinary(buf []byte) error {
	reader := bytes.NewReader(buf)
	if err := binary.Read(reader, binary.LittleEndian, &c.Name); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &c.Retention); err != nil {
		return err
	}
	return nil
}

func (c CreatePacket) MarshalBinary() ([]byte, error) {
	header, err := c.header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, c.Name); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, c.Retention); err != nil {
		return nil, err
	}
	return append(header, buf.Bytes()...), nil
}

func (d DeletePacket) UnmarshalBinary(buf []byte) error {
	reader := bytes.NewReader(buf)
	if err := binary.Read(reader, binary.LittleEndian, &d.Name); err != nil {
		return err
	}
	return nil
}

func (d DeletePacket) MarshalBinary() ([]byte, error) {
	header, err := d.header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, d.Name); err != nil {
		return nil, err
	}
	return append(header, buf.Bytes()...), nil
}

func (r AckResponse) MarshalBinary() ([]byte, error) {
	header, err := r.header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return header, nil
}

func UnmarshalBinary(buf []byte, u encoding.BinaryUnmarshaler) error {
	return u.UnmarshalBinary(buf)
}

func MarshalBinary(m encoding.BinaryMarshaler) ([]byte, error) {
	return m.MarshalBinary()
}
