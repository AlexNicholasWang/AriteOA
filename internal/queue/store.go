package queue

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type walRecord struct {
	Type       string       `json:"type"`
	Config     *QueueConfig `json:"config,omitempty"`
	Message    *Message     `json:"message,omitempty"`
	MessageID  string       `json:"message_id,omitempty"`
	Receipt    string       `json:"receipt,omitempty"`
	LeaseUntil int64        `json:"lease_until,omitempty"`
}

type WAL struct {
	mu sync.Mutex
	f  *os.File
}

func OpenWAL(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f}, nil
}

func (w *WAL) Append(r walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err = w.f.Write(append(b, '\n')); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *WAL) Replay(fn func(walRecord) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	s := bufio.NewScanner(w.f)
	buf := make([]byte, 64*1024)
	s.Buffer(buf, 16*1024*1024)
	line := 0
	for s.Scan() {
		line++
		var r walRecord
		if err := json.Unmarshal(s.Bytes(), &r); err != nil {
			return fmt.Errorf("corrupt WAL at line %d: %w", line, err)
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	_, err := w.f.Seek(0, io.SeekEnd)
	return err
}

func (w *WAL) Close() error { w.mu.Lock(); defer w.mu.Unlock(); return w.f.Close() }

var ErrNotFound = errors.New("not found")
