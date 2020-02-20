package core

import (
	"errors"
	"path"
	"strconv"
)

const (
	HeadSize        = 17 // crc + timestamp + + type + ks + vs
	SetType    byte = 1
	RemoveType byte = 0
)

const CompactThreshold = 1024 * 1024

var (
	ErrCrc32       = errors.New("Check sum error ")
	ErrKeyNotFound = errors.New("Key not found ")
)

type CommandPos struct {
	gen    int
	start  int
	length int
}

func logPath(workDir string, gen int) string {
	return path.Join(workDir, strconv.Itoa(gen)+".data")
}
