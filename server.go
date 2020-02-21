package main

import (
	"encoding/gob"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
)

type Server struct {
	db   *BitCask
	l    net.Listener
	quit chan struct{}
}

func NewServer(addr string) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	path, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	db := NewBitCask(filepath.Join(path, "tmp"))
	return &Server{
		db:   db,
		l:    l,
		quit: make(chan struct{}),
	}, nil
}

func (s *Server) RunServer() error {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return err
		}
		go s.handle(conn)

		select {
		case <-s.quit:
			break
		default:
		}
	}
}

func (s *Server) handle(conn net.Conn) {
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)
	var req Request
	for {
		err := dec.Decode(&req)
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}
		log.Printf("Recv request %v", req)
		resp := Response{}
		switch req.RequestType {
		case Get:
			value, err := s.db.Get(req.Key)
			if err != nil {
				resp.Err = err.Error()
			} else {
				resp.Value = value
			}
			resp.ResponseType = Get
		case Set:
			err := s.db.Set(req.Key, req.Value)
			if err != nil {
				resp.Err = err.Error()
			}
			resp.ResponseType = Set
		case Remove:
			err := s.db.Remove(req.Key)
			if err != nil {
				resp.Err = err.Error()
			}
			resp.ResponseType = Remove
		}
		err = enc.Encode(&resp)
		if err != nil {
			log.Printf("Send error: %v", err)
		}
		log.Printf("Send response %v", resp)

		select {
		case <-s.quit:
			break
		default:
		}
	}
}

func (s *Server) Close() error {
	close(s.quit)
	err := s.l.Close()
	if err != nil {
		return err
	}
	s.db.Close()
	return nil
}
