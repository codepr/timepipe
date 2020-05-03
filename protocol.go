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

const (
	OK = iota
	TSNOTFOUND
	TSEXISTS
	UNKNOWNCMD
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

type AddPointPacket struct {
	Name          string
	HaveTimestamp bool
	Value         float64
	Timestamp     int64
}

func (h *Header) Len() uint64 {
	return h.size
}

func (h *Header) Opcode() uint8 {
	return h.value >> 4
}

func (h *Header) SetOpcode(opcode uint8) {
	h.value &= 0x0F
	h.value |= ((opcode << 4) & 0xF0)
}

func (h *Header) Status() uint8 {
	return h.value >> 6
}

func (h *Header) SetStatus(status uint8) {
	h.value &= 0x0F
	h.value |= ((status << 6) & 0xF0)
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

func (a *AddPointPacket) UnmarshalBinary(buf []byte) error {
	r := bytes.NewReader(buf)
	var nameLen uint16 = 0
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return err
	}
	name := make([]byte, nameLen)
	if err := binary.Read(r, binary.BigEndian, &name); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &a.HaveTimestamp); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &a.Value); err != nil {
		return err
	}
	if a.HaveTimestamp == true {
		if err := binary.Read(r, binary.BigEndian, &a.Timestamp); err != nil {
			return err
		}
	}
	a.Name = string(name)
	return nil
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
