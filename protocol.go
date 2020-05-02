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
	Name      string
	Retention int64
}

type DeletePacket struct {
	Name string
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

func (h Header) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, h); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *Header) UnmarshalBinary(buf []byte) error {
	// Read operation code
	b := bytes.NewReader(buf)
	var value uint8
	if err := binary.Read(b, binary.BigEndian, &value); err != nil {
		return err
	}

	// Read payload len in bytes
	var size uint64
	if err := binary.Read(b, binary.BigEndian, &size); err != nil {
		return err
	}

	h.value = value
	h.size = size
	return nil
}

func (c *CreatePacket) UnmarshalBinary(buf []byte) error {
	reader := bytes.NewReader(buf)
	var nameLen uint16 = 0
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	name := make([]byte, nameLen)
	if err := binary.Read(reader, binary.BigEndian, &name); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &c.Retention); err != nil {
		return err
	}
	c.Name = string(name)
	return nil
}

func (c CreatePacket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, []byte(c.Name)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.Retention); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *DeletePacket) UnmarshalBinary(buf []byte) error {
	reader := bytes.NewReader(buf)
	var nameLen uint16 = 0
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	name := make([]byte, nameLen)
	if err := binary.Read(reader, binary.BigEndian, &name); err != nil {
		return err
	}
	d.Name = string(name)
	return nil
}

func (d DeletePacket) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, d.Name); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalBinary(buf []byte, u encoding.BinaryUnmarshaler) error {
	return u.UnmarshalBinary(buf)
}

func MarshalBinary(opcode uint8, m encoding.BinaryMarshaler) ([]byte, error) {
	bytesarray, err := m.MarshalBinary()
	if err != nil {
		return nil, err
	}
	header := Header{opcode, uint64(len(bytesarray))}
	byteshdr, err := header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(byteshdr, bytesarray...), err
}