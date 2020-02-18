package core

import (
	"os"
	"testing"
)

func TestGetSet(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	bc.Set([]byte("a"), []byte("1"))
	v, _ := bc.Get([]byte("a"))
	if string(v) != "1" {
		t.Error("Not Equal")
	}
	bc.Close()
	os.RemoveAll(tmpDir)
}
