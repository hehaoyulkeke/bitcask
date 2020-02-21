package main

import (
	"os"
	"testing"
)

func TestCS(t *testing.T) {
	tmpDir := "tmp"
	os.Mkdir(tmpDir, os.ModePerm)
	addr := "localhost:9000"
	server, err := NewServer(addr)
	if err != nil {
		t.Error(err)
	}
	go server.RunServer()
	client, err := NewClient(addr)
	if err != nil {
		t.Error(err)
	}
	err = client.Set([]byte("a"), []byte("1"))
	if err != nil {
		t.Error(err)
	}
	v, err := client.Get([]byte("a"))
	if err != nil {
		t.Error(err)
	}
	if string(v) != "1" {
		t.Error("Set/Get Fail")
	}
	err = client.Remove([]byte("a"))
	if err != nil {
		t.Error(err)
	}
	_, err = client.Get([]byte("a"))
	if !(err != nil && err.Error() == ErrKeyNotFound.Error()) {
		t.Error("Remove Fail")
	}
	client.Close()
	server.Close()
}
