package main

type Request struct {
	Key         []byte
	Value       []byte
	RequestType string
}

type Response struct {
	Value        []byte
	Err          string
	ResponseType string
}

const (
	Get    = "Get"
	Set    = "Set"
	Remove = "Remove"
)
