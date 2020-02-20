package core

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type BitCask struct {
	workDir     string
	writer      *BitCaskWriter
	readers     map[int]*BitCaskReader
	index       map[string]CommandPos
	gen         int
	uncompacted int
}

func NewBitCask(workDir string) *BitCask {
	_ = os.Mkdir(workDir, os.ModePerm)
	readers := make(map[int]*BitCaskReader)
	index := make(map[string]CommandPos)
	var uncompacted int = 0
	genLs, err := sortedGenLs(workDir)
	if err != nil {
		panic(err)
	}
	var gen int
	if len(genLs) == 0 {
		gen = 1
	} else {
		gen = genLs[len(genLs)-1] + 1
	}
	for _, gen := range genLs {
		offset, err := load(workDir, gen, index, readers)
		if err != nil {
			panic(err)
		}
		uncompacted += offset
	}
	writer, reader, err := newLogFile(workDir, gen)
	if err != nil {
		panic(err)
	}
	readers[gen] = reader
	return &BitCask{workDir, writer, readers, index, gen, uncompacted}
}

func load(workDir string, gen int, index map[string]CommandPos, readers map[int]*BitCaskReader) (int, error) {
	file, err := os.Open(logPath(workDir, gen))
	if err != nil {
		return 0, err
	}
	reader := &BitCaskReader{file}
	pos := 0
	uncompacted := 0
	if _, err := file.Seek(0, 0); err != nil {
		return 0, err
	}
	for {
		header := make([]byte, HeadSize)
		offset, err := file.Read(header)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		newPos := pos + offset
		cmdType := header[8]
		keySize := binary.LittleEndian.Uint32(header[9:13])
		valueSize := binary.LittleEndian.Uint32(header[13:17])
		kv := make([]byte, keySize+valueSize)
		offset, err = file.Read(kv)
		if err != nil {
			return 0, err
		}
		newPos += offset
		key := kv[:keySize]
		if cmdType == RemoveType {
			if oldPos, ok := index[string(key)]; ok {
				delete(index, string(key))
				uncompacted += oldPos.length
				uncompacted += newPos - pos
			} else {
				return 0, ErrKeyNotFound
			}
		}
		commandPos := CommandPos{gen: gen, start: pos, length: newPos - pos}
		index[string(key)] = commandPos
		pos = newPos
	}
	readers[gen] = reader
	return uncompacted, nil
}

func (bc *BitCask) Get(key []byte) ([]byte, error) {
	if pos, ok := bc.index[string(key)]; ok {
		value, err := bc.readers[pos.gen].readEntry(int64(pos.start), pos.length)
		if err != nil {
			return nil, err
		}
		return value, nil
	} else {
		return nil, nil
	}
}

func (bc *BitCask) Set(key, value []byte) error {
	start, length, err := bc.writer.writeEntry(key, value, SetType)
	if err != nil {
		return err
	}
	newPos := CommandPos{bc.gen, int(start), length}
	err = bc.writer.Flush()
	if err != nil {
		return err
	}
	if oldPos, ok := bc.index[string(key)]; ok {
		bc.uncompacted += oldPos.length
		if bc.uncompacted >= CompactThreshold {
			if err := bc.compact(); err != nil {
				return err
			}
		}
	}
	bc.index[string(key)] = newPos
	return nil
}

func (bc *BitCask) Remove(key []byte) error {
	if oldPos, ok := bc.index[string(key)]; ok {
		_, length, err := bc.writer.writeEntry(key, []byte{}, RemoveType)
		if err != nil {
			return err
		}
		err = bc.writer.Flush()
		if err != nil {
			return err
		}
		delete(bc.index, string(key))
		bc.uncompacted += length
		bc.uncompacted += oldPos.length
		if bc.uncompacted >= CompactThreshold {
			if err := bc.compact(); err != nil {
				return err
			}
		}
		return nil
	} else {
		return ErrKeyNotFound
	}
}

func (bc *BitCask) compact() error {
	compactGen := bc.gen + 1
	bc.gen += 2
	_, err := bc.newLogFile(compactGen)
	if err != nil {
		return err
	}
	bc.writer, err = bc.newLogFile(bc.gen)
	if err != nil {
		return err
	}
	pos := 0
	for k, v := range bc.index {
		reader := bc.readers[v.gen]
		buf := make([]byte, v.length)
		offset, err := reader.file.ReadAt(buf, int64(v.start))
		if err != nil {
			return err
		}
		cmdPos := CommandPos{compactGen, pos, offset}
		bc.index[k] = cmdPos
		pos += offset
	}

	genLs, err := sortedGenLs(bc.workDir)
	if err != nil {
		return err
	}
	for _, gen := range genLs {
		if gen < bc.gen {
			if err = bc.readers[gen].close(); err != nil {
				return err
			}
			if err = os.Remove(logPath(bc.workDir, gen)); err != nil {
				return nil
			}
			delete(bc.readers, gen)
		}
	}
	return nil
}

func (bc *BitCask) Close() {
	for _, v := range bc.readers {
		if err := v.close(); err != nil {
			panic(err)
		}
	}
}

func (bc *BitCask) newLogFile(gen int) (*BitCaskWriter, error) {
	writer, reader, err := newLogFile(bc.workDir, gen)
	if err != nil {
		return nil, err
	}
	bc.readers[gen] = reader
	return writer, nil
}

func sortedGenLs(path string) ([]int, error) {
	ls, _ := ioutil.ReadDir(path)
	res := make([]int, 0)
	for _, item := range ls {
		gen, err := strconv.Atoi(strings.Split(item.Name(), ".")[0])
		if err != nil {
			return nil, err
		}
		res = append(res, gen)
	}
	sort.Ints(res)
	return res, nil
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

func (w *BitCaskWriter) writeEntry(key, value []byte, cmdType byte) (int64, int, error) {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	entrySize := HeadSize + keySize + valueSize
	buf := make([]byte, entrySize)
	timestamp := uint32(time.Now().UnixNano())
	binary.LittleEndian.PutUint32(buf[4:8], timestamp)
	buf[8] = cmdType
	binary.LittleEndian.PutUint32(buf[9:13], keySize)
	binary.LittleEndian.PutUint32(buf[13:17], valueSize)
	copy(buf[HeadSize:HeadSize+keySize], key)
	copy(buf[HeadSize+keySize:], value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	n, err := w.Write(buf)
	if err != nil {
		return w.pos, 0, err
	}
	defer func() { w.pos += int64(n) }()
	return w.pos, n, nil
}

type BitCaskReader struct {
	file *os.File
}

func (br *BitCaskReader) readEntry(offset int64, size int) ([]byte, error) {
	res := make([]byte, size)
	_, err := br.file.ReadAt(res, offset)
	if err != nil {
		return nil, err
	}
	crc := binary.LittleEndian.Uint32(res[:4])
	if crc != crc32.ChecksumIEEE(res[4:]) {
		return nil, ErrCrc32
	}
	keySize := binary.LittleEndian.Uint32(res[9:13])
	valueSize := binary.LittleEndian.Uint32(res[13:HeadSize])
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
