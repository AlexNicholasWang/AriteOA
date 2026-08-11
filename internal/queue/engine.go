package queue

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type lease struct {
	Receipt string
	Until   time.Time
}

type queueState struct {
	Config   QueueConfig
	Messages map[string]*Message
	Leases   map[string]lease // message id -> lease
}

type Engine struct {
	mu       sync.Mutex
	cond     *sync.Cond
	wal      *WAL
	queues   map[string]*queueState
	sequence uint64
	closed   bool
}

func Open(path string) (*Engine, error) {
	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}
	e := &Engine{wal: wal, queues: map[string]*queueState{}}
	e.cond = sync.NewCond(&e.mu)
	if err := wal.Replay(e.applyReplay); err != nil {
		wal.Close()
		return nil, err
	}
	return e, nil
}

func (e *Engine) applyReplay(r walRecord) error {
	switch r.Type {
	case "create_queue":
		if r.Config == nil {
			return errors.New("create_queue missing config")
		}
		e.queues[r.Config.Name] = &queueState{Config: *r.Config, Messages: map[string]*Message{}, Leases: map[string]lease{}}
	case "enqueue":
		if r.Message == nil {
			return errors.New("enqueue missing message")
		}
		q := e.queues[r.Message.Queue]
		if q == nil {
			return fmt.Errorf("unknown queue %q in WAL", r.Message.Queue)
		}
		m := *r.Message
		q.Messages[m.ID] = &m
		if m.Sequence > e.sequence {
			e.sequence = m.Sequence
		}
	case "lease":
		for _, q := range e.queues {
			if _, ok := q.Messages[r.MessageID]; ok {
				q.Leases[r.MessageID] = lease{Receipt: r.Receipt, Until: time.Unix(0, r.LeaseUntil)}
				break
			}
		}
	case "ack":
		for _, q := range e.queues {
			if _, ok := q.Messages[r.MessageID]; ok {
				delete(q.Messages, r.MessageID)
				delete(q.Leases, r.MessageID)
				break
			}
		}
	case "nack":
		for _, q := range e.queues {
			if m, ok := q.Messages[r.MessageID]; ok {
				delete(q.Leases, r.MessageID)
				if r.LeaseUntil > 0 {
					m.AvailableAt = time.Unix(0, r.LeaseUntil)
				}
				break
			}
		}
	}
	return nil
}

func (e *Engine) CreateQueue(cfg QueueConfig) error {
	if cfg.Name == "" {
		return errors.New("name required")
	}
	if cfg.Discipline == "" {
		cfg.Discipline = FIFO
	}
	if cfg.Discipline != FIFO && cfg.Discipline != LIFO {
		return errors.New("discipline must be fifo or lifo")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.queues[cfg.Name]; ok {
		return fmt.Errorf("queue already exists")
	}
	if err := e.wal.Append(walRecord{Type: "create_queue", Config: &cfg}); err != nil {
		return err
	}
	e.queues[cfg.Name] = &queueState{Config: cfg, Messages: map[string]*Message{}, Leases: map[string]lease{}}
	return nil
}

func (e *Engine) ListQueues() []QueueConfig {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]QueueConfig, 0, len(e.queues))
	for _, q := range e.queues {
		out = append(out, q.Config)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (e *Engine) Enqueue(queueName, body string, priority int, delay time.Duration, attrs map[string]string) (Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queues[queueName]
	if q == nil {
		return Message{}, ErrNotFound
	}
	e.sequence++
	now := time.Now().UTC()
	m := Message{ID: newID(), Queue: queueName, Body: body, Priority: priority, CreatedAt: now, AvailableAt: now.Add(delay), Sequence: e.sequence, Attributes: attrs}
	if err := e.wal.Append(walRecord{Type: "enqueue", Message: &m}); err != nil {
		return Message{}, err
	}
	q.Messages[m.ID] = &m
	e.cond.Broadcast()
	return m, nil
}

func (e *Engine) Dequeue(queueName string, visibility time.Duration, wait time.Duration) (*Delivery, error) {
	if visibility <= 0 {
		visibility = 30 * time.Second
	}
	deadline := time.Now().Add(wait)
	e.mu.Lock()
	defer e.mu.Unlock()
	for {
		q := e.queues[queueName]
		if q == nil {
			return nil, ErrNotFound
		}
		now := time.Now().UTC()
		e.expireLeasesLocked(q, now)
		if m := pick(q, now); m != nil {
			m.Attempts++
			receipt := newID()
			until := now.Add(visibility)
			if err := e.wal.Append(walRecord{Type: "lease", MessageID: m.ID, Receipt: receipt, LeaseUntil: until.UnixNano()}); err != nil {
				return nil, err
			}
			q.Leases[m.ID] = lease{Receipt: receipt, Until: until}
			copyM := *m
			return &Delivery{Message: copyM, Receipt: receipt, LeaseExpires: until}, nil
		}
		if wait <= 0 || time.Now().After(deadline) {
			return nil, nil
		}
		remaining := time.Until(deadline)
		sleep := 250 * time.Millisecond
		if remaining < sleep {
			sleep = remaining
		}
		e.mu.Unlock()
		time.Sleep(sleep)
		e.mu.Lock()
	}
}

func pick(q *queueState, now time.Time) *Message {
	var candidates []*Message
	for id, m := range q.Messages {
		if _, leased := q.Leases[id]; leased {
			continue
		}
		if m.AvailableAt.After(now) {
			continue
		}
		candidates = append(candidates, m)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if q.Config.Priority && a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if q.Config.Discipline == LIFO {
			return a.Sequence > b.Sequence
		}
		return a.Sequence < b.Sequence
	})
	return candidates[0]
}

func (e *Engine) expireLeasesLocked(q *queueState, now time.Time) {
	for id, l := range q.Leases {
		if !l.Until.After(now) {
			delete(q.Leases, id)
		}
	}
}

func (e *Engine) Ack(queueName, id, receipt string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queues[queueName]
	if q == nil {
		return ErrNotFound
	}
	l, ok := q.Leases[id]
	if !ok || l.Receipt != receipt {
		return errors.New("invalid or expired receipt")
	}
	if err := e.wal.Append(walRecord{Type: "ack", MessageID: id, Receipt: receipt}); err != nil {
		return err
	}
	delete(q.Messages, id)
	delete(q.Leases, id)
	return nil
}

func (e *Engine) Nack(queueName, id, receipt string, delay time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queues[queueName]
	if q == nil {
		return ErrNotFound
	}
	l, ok := q.Leases[id]
	if !ok || l.Receipt != receipt {
		return errors.New("invalid or expired receipt")
	}
	m := q.Messages[id]
	at := time.Now().UTC().Add(delay)
	if err := e.wal.Append(walRecord{Type: "nack", MessageID: id, Receipt: receipt, LeaseUntil: at.UnixNano()}); err != nil {
		return err
	}
	delete(q.Leases, id)
	m.AvailableAt = at
	e.cond.Broadcast()
	return nil
}

func (e *Engine) Stats(name string) (Stats, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.queues[name]
	if q == nil {
		return Stats{}, ErrNotFound
	}
	now := time.Now().UTC()
	e.expireLeasesLocked(q, now)
	s := Stats{Total: len(q.Messages), Leased: len(q.Leases)}
	for id, m := range q.Messages {
		if _, ok := q.Leases[id]; ok {
			continue
		}
		if m.AvailableAt.After(now) {
			s.Delayed++
		} else {
			s.Ready++
		}
	}
	return s, nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.cond.Broadcast()
	return e.wal.Close()
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
