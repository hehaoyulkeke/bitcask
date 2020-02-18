package core

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"
)

type BitCask struct {
	workDir string
	writer *BitCaskWriter
	readers map[int]*BitCaskReader
	index map[string]CommandPos
	gen int
	uncompacted uint32
}

func NewBitCask(workDir string) *BitCask {
	_ = os.Mkdir(workDir, os.ModePerm)
	gen := 0
	readers := make(map[int]*BitCaskReader)
	writer, reader, err := newLogFile(workDir, gen)
	if err != nil {
		panic(err)
	}
	readers[gen] = reader
	index := make(map[string]CommandPos)
	uncompacted := uint32(0)
	return &BitCask{workDir, writer, readers, index, gen, uncompacted}
}

func (bc *BitCask) Get(key []byte) ([]byte, error) {
	if pos, ok := bc.index[string(key)]; ok {
		value, err := bc.readers[pos.gen].readEntry(pos.start, pos.length)
		if err != nil {
			return nil, err
		}
		return value, nil
	} else {
		return nil, ErrKeyNotFound
	}
}

func (bc *BitCask) Set(key, value []byte) error {
	start, length, err := bc.writer.writeEntry(key, value)
	if err != nil {
		return err
	}
	newPos := CommandPos{bc.gen, start, length}
	err = bc.writer.Flush()
	if err != nil {
		return err
	}
	if oldPos, ok := bc.index[string(key)]; ok {
		bc.uncompacted += oldPos.length
	}
	bc.index[string(key)] = newPos
	return nil

}

func (bc *BitCask) Close() {
	for _, v := range bc.readers {
		if err := v.close(); err != nil {
			panic(err)
		}
	}
}

type BitCaskWriter struct {
	*bufio.Writer
	pos int64
}

func newLogFile(workDir string, gen int) (*BitCaskWriter, *BitCaskReader, error) {
	file, err := os.Create(logPath(workDir, gen))
	if err != nil {
		if !os.IsExist(err) {
			return nil, nil, err
		}
	}
	bw := &BitCaskWriter{
		Writer: bufio.NewWriter(file),
		pos:    0,
	}
	br := &BitCaskReader{file}
	return bw, br, nil
}

func (w *BitCaskWriter) writeEntry(key, value []byte) (int64, uint32, error) {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	entrySize := HeadSize + keySize + valueSize
	buf := make([]byte, entrySize)
	timestamp := uint32(time.Now().UnixNano())
	binary.LittleEndian.PutUint32(buf[4:8], timestamp)
	binary.LittleEndian.PutUint32(buf[8:12], keySize)
	binary.LittleEndian.PutUint32(buf[12:16], valueSize)
	copy(buf[HeadSize:HeadSize+keySize], key)
	copy(buf[HeadSize+keySize:], value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	n, err := w.Write(buf)
	if err != nil {
		return w.pos, 0, err
	}
	defer func() {w.pos += int64(n)}()
	return w.pos, uint32(n), nil
}

type BitCaskReader struct {
	file *os.File
}

func (br *BitCaskReader) readEntry(offset int64, size uint32) ([]byte, error) {
	res := make([]byte, size)
	_, err := br.file.ReadAt(res, offset)
	if err != nil {
		return nil, err
	}
	keySize := binary.LittleEndian.Uint32(res[8:12])
	valueSize := binary.LittleEndian.Uint32(res[12:HeadSize])
	crc := binary.LittleEndian.Uint32(res[:4])
	if crc != crc32.ChecksumIEEE(res[4:]) {
		println("crc error")
		return nil, ErrCrc32
	}
	value := make([]byte, valueSize)
	copy(value, res[HeadSize+keySize:])
	return value, nil
}

func (br *BitCaskReader) close() error {
	if err := br.file.Close(); err != nil {
		return err
	}
	return nil
}