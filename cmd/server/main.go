package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"queuemaxxing/internal/queue"
)

type api struct{ q *queue.Engine }

type errResp struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		writeJSON(w, 400, errResp{err.Error()})
		return false
	}
	return true
}

func (a *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/queues", a.queues)
	mux.HandleFunc("/api/queues/", a.queueOps)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return logRequests(mux)
}
func (a *api) queues(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, a.q.ListQueues())
	case http.MethodPost:
		var cfg queue.QueueConfig
		if !decode(w, r, &cfg) {
			return
		}
		if err := a.q.CreateQueue(cfg); err != nil {
			writeJSON(w, 400, errResp{err.Error()})
			return
		}
		writeJSON(w, 201, cfg)
	default:
		w.WriteHeader(405)
	}
}
func (a *api) queueOps(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/queues/"), "/")
	if len(parts) < 2 {
		writeJSON(w, 404, errResp{"not found"})
		return
	}
	name, op := parts[0], parts[1]
	switch op {
	case "messages":
		if r.Method == http.MethodPost && len(parts) == 2 {
			var req struct {
				Body       string            `json:"body"`
				Priority   int               `json:"priority"`
				DelayMS    int64             `json:"delay_ms"`
				Attributes map[string]string `json:"attributes"`
			}
			if !decode(w, r, &req) {
				return
			}
			m, err := a.q.Enqueue(name, req.Body, req.Priority, time.Duration(req.DelayMS)*time.Millisecond, req.Attributes)
			if err != nil {
				respondErr(w, err)
				return
			}
			writeJSON(w, 201, m)
			return
		}
		if len(parts) == 4 {
			id, action := parts[2], parts[3]
			var req struct {
				Receipt string `json:"receipt"`
				DelayMS int64  `json:"delay_ms"`
			}
			if !decode(w, r, &req) {
				return
			}
			var err error
			if action == "ack" {
				err = a.q.Ack(name, id, req.Receipt)
			} else if action == "nack" {
				err = a.q.Nack(name, id, req.Receipt, time.Duration(req.DelayMS)*time.Millisecond)
			} else {
				writeJSON(w, 404, errResp{"not found"})
				return
			}
			if err != nil {
				respondErr(w, err)
				return
			}
			writeJSON(w, 200, map[string]bool{"ok": true})
			return
		}
		w.WriteHeader(405)
	case "dequeue":
		if r.Method != http.MethodPost {
			w.WriteHeader(405)
			return
		}
		visibility, _ := strconv.Atoi(r.URL.Query().Get("visibility_ms"))
		wait, _ := strconv.Atoi(r.URL.Query().Get("wait_ms"))
		d, err := a.q.Dequeue(name, time.Duration(visibility)*time.Millisecond, time.Duration(wait)*time.Millisecond)
		if err != nil {
			respondErr(w, err)
			return
		}
		if d == nil {
			w.WriteHeader(204)
			return
		}
		writeJSON(w, 200, d)
	case "stats":
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		s, err := a.q.Stats(name)
		if err != nil {
			respondErr(w, err)
			return
		}
		writeJSON(w, 200, s)
	default:
		writeJSON(w, 404, errResp{"not found"})
	}
}
func respondErr(w http.ResponseWriter, err error) {
	if errors.Is(err, queue.ErrNotFound) {
		writeJSON(w, 404, errResp{err.Error()})
	} else {
		writeJSON(w, 409, errResp{err.Error()})
	}
}
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
func main() {
	addr := flag.String("addr", ":8080", "listen address")
	data := flag.String("data", "./data/queue.wal", "WAL path")
	flag.Parse()
	q, err := queue.Open(*data)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()
	a := &api{q: q}
	srv := &http.Server{Addr: *addr, Handler: a.routes(), ReadHeaderTimeout: 5 * time.Second}
	fmt.Printf("Queuemaxxing API listening on %s\n", *addr)
	log.Fatal(srv.ListenAndServe())
}
