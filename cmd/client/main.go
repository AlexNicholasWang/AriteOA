package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type client struct {
	base string
	hc   *http.Client
}

func (c *client) do(method, path string, body any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNoContent {
		fmt.Println("no message available")
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	if len(data) == 0 {
		return nil
	}
	var v any
	if json.Unmarshal(data, &v) == nil {
		out, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(out))
	} else {
		fmt.Print(string(data))
	}
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `Queuemaxxing CLI

Usage:
  client [global flags] <command> [command flags]

Global flags:
  -server URL   API base URL (default http://localhost:8080)

Commands:
  list
  create   -name NAME [-discipline fifo|lifo] [-priority]
  enqueue  -queue NAME -body TEXT [-priority N] [-delay 5s]
  dequeue  -queue NAME [-visibility 30s] [-wait 1s]
  ack      -queue NAME -id MESSAGE_ID -receipt RECEIPT
  nack     -queue NAME -id MESSAGE_ID -receipt RECEIPT [-delay 3s]
  stats    -queue NAME
`)
}

func main() {
	global := flag.NewFlagSet("client", flag.ContinueOnError)
	server := global.String("server", "http://localhost:8080", "API base URL")
	global.SetOutput(io.Discard)
	if err := global.Parse(os.Args[1:]); err != nil {
		usage()
		os.Exit(2)
	}
	args := global.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	c := &client{base: strings.TrimRight(*server, "/"), hc: &http.Client{Timeout: 35 * time.Second}}
	cmd := args[0]
	rest := args[1:]
	var err error

	switch cmd {
	case "list":
		err = c.do(http.MethodGet, "/api/queues", nil)
	case "create":
		fs := flag.NewFlagSet(cmd, flag.ExitOnError)
		name := fs.String("name", "", "queue name")
		discipline := fs.String("discipline", "fifo", "fifo or lifo")
		priority := fs.Bool("priority", false, "enable priority ordering")
		_ = fs.Parse(rest)
		if *name == "" {
			fs.Usage()
			os.Exit(2)
		}
		err = c.do(http.MethodPost, "/api/queues", map[string]any{"name": *name, "discipline": *discipline, "priority": *priority})
	case "enqueue":
		fs := flag.NewFlagSet(cmd, flag.ExitOnError)
		q := fs.String("queue", "", "queue name")
		body := fs.String("body", "", "message body")
		priority := fs.Int("priority", 0, "message priority")
		delay := fs.Duration("delay", 0, "delivery delay")
		_ = fs.Parse(rest)
		if *q == "" || *body == "" {
			fs.Usage()
			os.Exit(2)
		}
		err = c.do(http.MethodPost, "/api/queues/"+url.PathEscape(*q)+"/messages", map[string]any{"body": *body, "priority": *priority, "delay_ms": delay.Milliseconds()})
	case "dequeue":
		fs := flag.NewFlagSet(cmd, flag.ExitOnError)
		q := fs.String("queue", "", "queue name")
		visibility := fs.Duration("visibility", 30*time.Second, "visibility timeout")
		wait := fs.Duration("wait", time.Second, "long-poll wait")
		_ = fs.Parse(rest)
		if *q == "" {
			fs.Usage()
			os.Exit(2)
		}
		path := fmt.Sprintf("/api/queues/%s/dequeue?visibility_ms=%d&wait_ms=%d", url.PathEscape(*q), visibility.Milliseconds(), wait.Milliseconds())
		err = c.do(http.MethodPost, path, nil)
	case "ack", "nack":
		fs := flag.NewFlagSet(cmd, flag.ExitOnError)
		q := fs.String("queue", "", "queue name")
		id := fs.String("id", "", "message id")
		receipt := fs.String("receipt", "", "delivery receipt")
		delay := fs.Duration("delay", 0, "redelivery delay (nack only)")
		_ = fs.Parse(rest)
		if *q == "" || *id == "" || *receipt == "" {
			fs.Usage()
			os.Exit(2)
		}
		payload := map[string]any{"receipt": *receipt}
		if cmd == "nack" {
			payload["delay_ms"] = delay.Milliseconds()
		}
		err = c.do(http.MethodPost, "/api/queues/"+url.PathEscape(*q)+"/messages/"+url.PathEscape(*id)+"/"+cmd, payload)
	case "stats":
		fs := flag.NewFlagSet(cmd, flag.ExitOnError)
		q := fs.String("queue", "", "queue name")
		_ = fs.Parse(rest)
		if *q == "" {
			fs.Usage()
			os.Exit(2)
		}
		err = c.do(http.MethodGet, "/api/queues/"+url.PathEscape(*q)+"/stats", nil)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
