package main

import "testing"

func TestHeaderOpcode(t *testing.T) {
	header := Header{}
	header.SetOpcode(1)
	if header.Opcode() != 1 {
		t.Errorf("Expected 1 got: %v", header.Opcode())
	}
	header.SetOpcode(4)
	if header.Opcode() != 4 {
		t.Errorf("Expected 4 got: %v", header.Opcode())
	}
}

func TestHeaderStatus(t *testing.T) {
	header := Header{}
	header.SetStatus(OK)
	if header.Status() != OK {
		t.Errorf("Expected %v got: %v", OK, header.Opcode())
	}
	header.SetStatus(TSEXISTS)
	if header.Status() != TSEXISTS {
		t.Errorf("Expected %v got: %v", TSEXISTS, header.Opcode())
	}
}
