package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type received struct {
	Seq    int64 `json:"seq"`
	Offset int64 `json:"offset"`
	RecvNs int64 `json:"recv_ns"`
}

type consumer struct {
	cl     *kgo.Client
	mu     sync.Mutex
	recs   []received
	lastAt time.Time
	stop   chan struct{}
	done   chan struct{}
}

// newConsumer reads the topic from the beginning with no consumer group, so a
// broker restart never costs it committed offsets and the sink is the only
// record of what actually arrived.
func newConsumer() *consumer {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.WithLogger(kgo.BasicLogger(discard{}, kgo.LogLevelError, nil)),
	)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	c := &consumer{cl: cl, lastAt: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	go c.loop()
	return c
}

func (c *consumer) loop() {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		fetches := c.cl.PollFetches(ctx)
		cancel()
		now := time.Now().UnixNano()
		fetches.EachRecord(func(r *kgo.Record) {
			var v struct {
				Seq int64 `json:"seq"`
			}
			if err := json.Unmarshal(r.Value, &v); err != nil {
				return
			}
			c.mu.Lock()
			c.recs = append(c.recs, received{Seq: v.Seq, Offset: r.Offset, RecvNs: now})
			c.lastAt = time.Now()
			c.mu.Unlock()
		})
	}
}

// drain waits until no record has arrived for `idle`.
func (c *consumer) drain(idle time.Duration) {
	for {
		c.mu.Lock()
		since := time.Since(c.lastAt)
		c.mu.Unlock()
		if since > idle {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (c *consumer) snapshot() []received {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]received, len(c.recs))
	copy(out, c.recs)
	return out
}

func (c *consumer) close() {
	close(c.stop)
	<-c.done
	c.cl.Close()
}
