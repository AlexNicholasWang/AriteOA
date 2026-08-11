package queue

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFIFOAndRestart(t *testing.T) {
	p := filepath.Join(t.TempDir(), "q.wal")
	e, _ := Open(p)
	if err := e.CreateQueue(QueueConfig{Name: "q", Discipline: FIFO}); err != nil {
		t.Fatal(err)
	}
	m1, _ := e.Enqueue("q", "one", 0, 0, nil)
	_, _ = e.Enqueue("q", "two", 0, 0, nil)
	d, _ := e.Dequeue("q", time.Second, 0)
	if d.Message.ID != m1.ID {
		t.Fatalf("want first message")
	}
	if err := e.Ack("q", d.Message.ID, d.Receipt); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	d2, _ := e2.Dequeue("q", time.Second, 0)
	if d2 == nil || d2.Message.Body != "two" {
		t.Fatalf("restart replay failed: %#v", d2)
	}
}
func TestPriorityLIFO(t *testing.T) {
	e, _ := Open(filepath.Join(t.TempDir(), "q.wal"))
	defer e.Close()
	_ = e.CreateQueue(QueueConfig{Name: "q", Discipline: LIFO, Priority: true})
	_, _ = e.Enqueue("q", "low", 1, 0, nil)
	_, _ = e.Enqueue("q", "high-old", 9, 0, nil)
	_, _ = e.Enqueue("q", "high-new", 9, 0, nil)
	d, _ := e.Dequeue("q", time.Second, 0)
	if d.Message.Body != "high-new" {
		t.Fatalf("got %s", d.Message.Body)
	}
}
func TestDelayAndRedelivery(t *testing.T) {
	e, _ := Open(filepath.Join(t.TempDir(), "q.wal"))
	defer e.Close()
	_ = e.CreateQueue(QueueConfig{Name: "q"})
	_, _ = e.Enqueue("q", "x", 0, 40*time.Millisecond, nil)
	d, _ := e.Dequeue("q", 20*time.Millisecond, 0)
	if d != nil {
		t.Fatal("delayed message delivered early")
	}
	time.Sleep(50 * time.Millisecond)
	d, _ = e.Dequeue("q", 30*time.Millisecond, 0)
	if d == nil {
		t.Fatal("missing delayed message")
	}
	time.Sleep(40 * time.Millisecond)
	d2, _ := e.Dequeue("q", time.Second, 0)
	if d2 == nil || d2.Message.ID != d.Message.ID {
		t.Fatal("message not replayed after lease timeout")
	}
}
