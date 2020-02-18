package core

import (
	"errors"
	"path"
	"strconv"
)

const (
	HeadSize = 16 // crc + timestamp + ks + vs
)

var (
	ErrCrc32 = errors.New("Check sum error ")
	ErrKeyNotFound = errors.New("Key not found ")
)

type CommandPos struct {
	gen int
	start int64
	length uint32
}

func logPath(workDir string, gen int) string {
	return path.Join(workDir, strconv.Itoa(gen)+".data")
}