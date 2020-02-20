package core

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func Test_GetSet(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	bc.Set([]byte("a"), []byte("1"))
	bc.Set([]byte("b"), []byte("2"))
	bc.Set([]byte("c"), []byte("3"))
	v, _ := bc.Get([]byte("b"))
	if string(v) != "2" {
		t.Error("Not Equal")
	}
	bc.Close()
	os.RemoveAll(tmpDir)
}

func TestBitCask_Remove(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	bc.Set([]byte("a"), []byte("1"))
	bc.Set([]byte("b"), []byte("2"))
	bc.Set([]byte("c"), []byte("3"))
	_ = bc.Remove([]byte("b"))
	v, _ := bc.Get([]byte("b"))
	if string(v) == "2" {
		t.Error("Remove Fail")
	}
	bc.Close()
	os.RemoveAll(tmpDir)
}

func Test_Recover(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	bc.Set([]byte("a"), []byte("1"))
	bc.Set([]byte("b"), []byte("2"))
	bc.Set([]byte("c"), []byte("3"))
	bc.Close()
	bc = NewBitCask(tmpDir)
	v, _ := bc.Get([]byte("b"))
	if string(v) != "2" {
		t.Error("Recover fail")
	}
	bc.Close()
	os.RemoveAll(tmpDir)
}

func Test_ConcurrentGet(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	bc.Set([]byte("a"), []byte("1"))
	bc.Set([]byte("b"), []byte("2"))
	bc.Set([]byte("c"), []byte("3"))
	go func() {
		v, _ := bc.Get([]byte("a"))
		fmt.Println(string(v))
	}()
	go func() {
		v, _ := bc.Get([]byte("b"))
		fmt.Println(string(v))
	}()
	go func() {
		v, _ := bc.Get([]byte("c"))
		fmt.Println(string(v))
	}()
	time.Sleep(1 * time.Second)
}

func Test_ConcurrentSet(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	go func() { bc.Set([]byte("a"), []byte("1")) }()
	go func() { bc.Set([]byte("b"), []byte("2")) }()
	go func() { bc.Set([]byte("c"), []byte("3")) }()
	time.Sleep(1 * time.Second)
	go func() {
		v, err := bc.Get([]byte("a"))
		if err != nil {
			t.Error(err)
		}
		fmt.Println("a", string(v))
	}()
	go func() {
		v, err := bc.Get([]byte("b"))
		if err != nil {
			t.Error(err)
		}
		fmt.Println("b", string(v))
	}()
	go func() {
		v, err := bc.Get([]byte("c"))
		if err != nil {
			t.Error(err)
		}
		fmt.Println("c", string(v))
	}()
	time.Sleep(1 * time.Second)
}
