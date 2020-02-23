package main

import (
	"os"
	"strconv"
	"sync"
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

func Test_OverWrite(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	bc.Set([]byte("a"), []byte("1"))
	bc.Set([]byte("a"), []byte("2"))
	v, _ := bc.Get([]byte("a"))
	if string(v) != "2" {
		t.Error("OverWrite Fail")
	}
	bc.Close()
	os.RemoveAll(tmpDir)
}

func Test_GetNonExistent(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	_, err := bc.Get([]byte("a"))
	if err != ErrKeyNotFound {
		t.Error("Get Fail")
	}
	bc.Close()
	os.RemoveAll(tmpDir)
}

func Test_RemoveNonExistent(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	err := bc.Remove([]byte("a"))
	if err != ErrKeyNotFound {
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
	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		bc.Set([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i)))
	}
	for i := 0; i < 1000; i++ {
		go func(i int) {
			v, err := bc.Get([]byte("key" + strconv.Itoa(i)))
			if err != nil {
				t.Error(err)
			}
			if string(v) != "value"+strconv.Itoa(i) {
				t.Error("Fail", i)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	bc.Close()
	os.RemoveAll(tmpDir)
}

func Test_ConcurrentSet(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	var wg sync.WaitGroup
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			bc.Set([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i)))
			wg.Done()
		}(i)
	}
	wg.Wait()
	for i := 0; i < 1000; i++ {
		v, err := bc.Get([]byte("key" + strconv.Itoa(i)))
		if err != nil {
			t.Error(err)
		}
		if string(v) != "value"+strconv.Itoa(i) {
			t.Error("Fail", i)
		}
	}
	bc.Close()
	os.RemoveAll(tmpDir)
}

func Test_Compact(t *testing.T) {
	tmpDir := "tmp"
	bc := NewBitCask(tmpDir)
	for i := 0; i < 100000; i++ {
		bc.Set([]byte("key"), []byte("value"))
	}
	time.Sleep(5 * time.Second)
	bc.Close()
	bc = NewBitCask(tmpDir)
	for i := 0; i < 100000; i++ {
		v, err := bc.Get([]byte("key"))
		if err != nil {
			t.Error(err)
		}
		if string(v) != "value" {
			t.Error("Fail", i)
		}
	}
	bc.Close()
	os.RemoveAll(tmpDir)
}
