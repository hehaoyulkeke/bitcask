package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	flag.NewFlagSet("set", flag.ExitOnError)
	flag.NewFlagSet("get", flag.ExitOnError)
	flag.NewFlagSet("rm", flag.ExitOnError)

	client, err := NewClient("localhost:9000")
	defer client.Close()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	switch os.Args[1] {
	case "get":
		val, err := client.Get([]byte(os.Args[2]))
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(string(val))
		}
	case "set":
		err := client.Set([]byte(os.Args[2]), []byte(os.Args[3]))
		if err != nil {
			fmt.Println(err)
		}
	case "rm":
		err := client.Remove([]byte(os.Args[2]))
		if err != nil {
			fmt.Println(err)
		}
	default:
		fmt.Println("unexpected command")
		os.Exit(1)
	}

}
