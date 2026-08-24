// Command bench measures message loss and duplication for two PostgreSQL-to-Kafka
// publishing strategies when the broker is killed mid-run.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	topic     = "orders"
	brokerCtr = "ovd-redpanda"
	broker    = "127.0.0.1:19092"
)

type config struct {
	arm       string
	n         int
	workers   int
	inject    bool
	injectPct float64
	killPause time.Duration
	out       string
	dsn       string
	rep       int
}

// summary is the machine-readable result of one run.
type summary struct {
	Arm         string  `json:"arm"`
	Rep         int     `json:"rep"`
	Inject      bool    `json:"inject"`
	N           int     `json:"n"`
	Written     int     `json:"written"`
	Received    int     `json:"received"`
	Distinct    int     `json:"distinct"`
	Lost        int     `json:"lost"`
	Dupes       int     `json:"dupes"`
	PublishErrs int64   `json:"publish_errors"`
	WriteP50Ms  float64 `json:"write_p50_ms"`
	WriteP95Ms  float64 `json:"write_p95_ms"`
	WriteP99Ms  float64 `json:"write_p99_ms"`
	WriteMaxMs  float64 `json:"write_max_ms"`
	ThroughputW float64 `json:"write_throughput_per_s"`
	LagP50Ms    float64 `json:"e2e_lag_p50_ms"`
	LagP95Ms    float64 `json:"e2e_lag_p95_ms"`
	LagP99Ms    float64 `json:"e2e_lag_p99_ms"`
	LagMaxMs    float64 `json:"e2e_lag_max_ms"`
	RecoveryS   float64 `json:"recovery_s"`
	WallS       float64 `json:"wall_s"`
	KilledAt    string  `json:"killed_at,omitempty"`
	BackAt      string  `json:"back_at,omitempty"`
}

func main() {
	var c config
	flag.StringVar(&c.arm, "arm", "dualwrite", "dualwrite|outbox")
	flag.IntVar(&c.n, "n", 50000, "number of orders")
	flag.IntVar(&c.workers, "workers", 16, "concurrent writers")
	flag.BoolVar(&c.inject, "inject", true, "docker kill the broker mid-run")
	flag.Float64Var(&c.injectPct, "inject-pct", 0.40, "fraction of writes before the kill")
	flag.DurationVar(&c.killPause, "kill-pause", 10*time.Second, "how long the broker stays down")
	flag.StringVar(&c.out, "out", "results", "output directory")
	flag.StringVar(&c.dsn, "dsn", "postgres://postgres:postgres@127.0.0.1:55432/bench", "postgres dsn")
	flag.IntVar(&c.rep, "rep", 1, "repetition number (1 = warm-up, discarded)")
	flag.Parse()

	if err := run(c); err != nil {
		log.Fatal(err)
	}
}

func run(c config) error {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, c.dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		return err
	}
	if err := resetTopic(); err != nil {
		return err
	}

	cons := newConsumer()
	defer cons.close()

	inj := &injector{enabled: c.inject, at: int64(float64(c.n) * c.injectPct), pause: c.killPause}

	start := time.Now()
	var w *writerResult
	switch c.arm {
	case "dualwrite":
		w, err = runDualWrite(ctx, pool, c, inj)
	case "outbox":
		w, err = runOutbox(ctx, pool, c, inj)
	default:
		return fmt.Errorf("unknown arm %q", c.arm)
	}
	if err != nil {
		return err
	}
	inj.wait()
	wall := time.Since(start)

	cons.drain(15 * time.Second)
	return report(ctx, pool, c, w, cons, inj, wall)
}

// resetTopic deletes and recreates the topic so every repetition starts empty.
func resetTopic() error {
	_ = exec.Command("docker", "exec", brokerCtr, "rpk", "topic", "delete", topic).Run()
	out, err := exec.Command("docker", "exec", brokerCtr, "rpk", "topic", "create", topic,
		"-p", "1", "-r", "1").CombinedOutput()
	if err != nil {
		return fmt.Errorf("create topic: %v: %s", err, out)
	}
	return nil
}

// injector kills and restarts the broker container once, when the write counter
// crosses `at`. Timing is identical for every arm because it is driven by the
// write counter, not by the wall clock.
type injector struct {
	enabled  bool
	at       int64
	pause    time.Duration
	fired    atomic.Bool
	done     sync.WaitGroup
	killedAt time.Time
	backAt   time.Time
}

func (i *injector) maybeFire(count int64) {
	if !i.enabled || count < i.at || !i.fired.CompareAndSwap(false, true) {
		return
	}
	i.done.Add(1)
	go func() {
		defer i.done.Done()
		i.killedAt = time.Now()
		log.Printf("INJECT: docker kill %s", brokerCtr)
		if out, err := exec.Command("docker", "kill", brokerCtr).CombinedOutput(); err != nil {
			log.Printf("kill failed: %v %s", err, out)
		}
		time.Sleep(i.pause)
		if out, err := exec.Command("docker", "start", brokerCtr).CombinedOutput(); err != nil {
			log.Printf("start failed: %v %s", err, out)
		}
		i.backAt = time.Now()
		log.Printf("INJECT: broker back after %s", i.backAt.Sub(i.killedAt))
	}()
}

func (i *injector) wait() { i.done.Wait() }

type writerResult struct {
	commitNs  []int64 // index = seq
	latencyNs []int64
	pubErrs   int64
}

func newSeqPayload(seq int64) []byte {
	b, _ := json.Marshal(map[string]int64{"seq": seq})
	return b
}

func percentile(sorted []int64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return float64(sorted[idx]) / 1e6
}
