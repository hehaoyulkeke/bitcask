package main

import (
	"encoding/gob"
	"errors"
	"net"
)

type Client struct {
	enc  *gob.Encoder
	dec  *gob.Decoder
	conn net.Conn
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)
	return &Client{enc, dec, conn}, nil
}

func (cli *Client) Get(key []byte) ([]byte, error) {
	req := Request{
		Key:         key,
		Value:       nil,
		RequestType: Get,
	}
	resp := Response{}
	err := cli.enc.Encode(&req)
	if err != nil {
		return nil, err
	}
	err = cli.dec.Decode(&resp)
	if err != nil {
		return nil, err
	}
	if resp.Err != Ok {
		return resp.Value, errors.New(resp.Err)
	}
	if resp.ResponseType != Get {
		return nil, ErrUnexpectedResponse
	}
	return resp.Value, nil
}

func (cli *Client) Set(key, value []byte) error {
	req := Request{
		Key:         key,
		Value:       value,
		RequestType: Set,
	}
	resp := Response{}
	err := cli.enc.Encode(&req)
	if err != nil {
		return err
	}
	err = cli.dec.Decode(&resp)
	if err != nil {
		return err
	}
	if resp.Err != Ok {
		return errors.New(resp.Err)
	}
	if resp.ResponseType != Set {
		return ErrUnexpectedResponse
	}
	return nil
}

func (cli *Client) Remove(key []byte) error {
	req := Request{
		Key:         key,
		Value:       nil,
		RequestType: Remove,
	}
	resp := Response{}
	err := cli.enc.Encode(&req)
	if err != nil {
		return err
	}
	err = cli.dec.Decode(&resp)
	if err != nil {
		return err
	}
	if resp.Err != Ok {
		return errors.New(resp.Err)
	}
	if resp.ResponseType != Remove {
		return ErrUnexpectedResponse
	}
	return nil
}

func (cli *Client) Close() error {
	return cli.conn.Close()
}
