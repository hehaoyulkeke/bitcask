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

	switch os.Args[1] {
	case "set":
		fmt.Printf("set key %v-value %v\n", os.Args[2], os.Args[3])
	case "get":
		fmt.Printf("get key %v\n", os.Args[2])
	case "rm":
		fmt.Printf("remove key %v\n", os.Args[2])
	default:
		fmt.Println("unexpected command")
		os.Exit(1)
	}

}
