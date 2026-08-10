package main

import(
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

var base = flag.String("s", "http://localhost:8080", "server base URL")

func main() { 
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}
	switch args[0] {
	case "create":
		cmdCreate(args[1:])
	case "put":
		cmdPut(args[1:])
	case "take":
		cmdTake(args[1:])
	case "stats":
		cmdStats()
	case "demo":
		cmdDemo()
	default:
		usage()
	}

	func usage() {
		fmt.Fprintln(os.Stderr, `usage:
  client [-s URL] create <queue> [--order fifo|lifo]
  client [-s URL] put <queue> <json-payload> [--priority N] [--delay 5s]
  client [-s URL] take <queue>
  client [-s URL] stats
  client [-s URL] demo`)
		os.Exit(2)
	}

	//HTTP helpers

	func call(method, path string, body, out interface{}) (int, error) {
		var buf io.Reader
		if body != nil {
			enc, err := json.Marshal(body)
			if err != nil {
				return 0, err
			}
			buf = bytes.NewReader(enc)
		}
	}
}