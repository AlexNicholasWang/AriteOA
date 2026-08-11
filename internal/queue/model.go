package queue

import "time"

type Discipline string

const (
	FIFO Discipline = "fifo"
	LIFO Discipline = "lifo"
)

type QueueConfig struct {
	Name       string     `json:"name"`
	Discipline Discipline `json:"discipline"`
	Priority   bool       `json:"priority"`
}

type Message struct {
	ID          string            `json:"id"`
	Queue       string            `json:"queue"`
	Body        string            `json:"body"`
	Priority    int               `json:"priority"`
	CreatedAt   time.Time         `json:"created_at"`
	AvailableAt time.Time         `json:"available_at"`
	Sequence    uint64            `json:"sequence"`
	Attempts    int               `json:"attempts"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

type Delivery struct {
	Message      Message   `json:"message"`
	Receipt      string    `json:"receipt"`
	LeaseExpires time.Time `json:"lease_expires"`
}

type Stats struct {
	Ready   int `json:"ready"`
	Delayed int `json:"delayed"`
	Leased  int `json:"leased"`
	Total   int `json:"total"`
}
