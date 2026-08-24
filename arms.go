package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// dataset is generated from a fixed seed, so every run writes identical rows.
func dataset(n int) []int64 {
	r := rand.New(rand.NewSource(42)) // #nosec G404 -- reproducibility, not security
	amounts := make([]int64, n)
	for i := range amounts {
		amounts[i] = r.Int63n(100000) + 1
	}
	return amounts
}

func customer(seq int64) string {
	return "cust-" + string(rune('a'+seq%26)) + "-" + itoa(seq%1000)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

// runDualWrite commits to Postgres and then publishes synchronously in the
// request path. A publish that fails is logged and dropped: that is the whole
// point of the arm.
func runDualWrite(ctx context.Context, pool *pgxpool.Pool, c config, inj *injector) (*writerResult, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.RecordRetries(1),
		kgo.RecordDeliveryTimeout(2*time.Second),
		kgo.ProduceRequestTimeout(2*time.Second),
		kgo.RetryTimeout(2*time.Second),
		kgo.WithLogger(kgo.BasicLogger(discard{}, kgo.LogLevelError, nil)),
	)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	res := &writerResult{commitNs: make([]int64, c.n), latencyNs: make([]int64, c.n)}
	amounts := dataset(c.n)
	var counter atomic.Int64
	var wg sync.WaitGroup
	t0all := time.Now()
	for w := 0; w < c.workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				seq := counter.Add(1) - 1
				if seq >= int64(c.n) {
					return
				}
				t0 := time.Now()
				_, err := pool.Exec(ctx,
					"INSERT INTO orders (seq, customer, amount_cents) VALUES ($1,$2,$3)",
					seq, customer(seq), amounts[seq])
				if err != nil {
					log.Printf("insert %d: %v", seq, err)
					continue
				}
				res.commitNs[seq] = time.Now().UnixNano()
				inj.maybeFire(seq)
				pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				pr := cl.ProduceSync(pctx, &kgo.Record{Topic: topic, Value: newSeqPayload(seq)})
				cancel()
				if perr := pr.FirstErr(); perr != nil {
					atomic.AddInt64(&res.pubErrs, 1)
				}
				res.latencyNs[seq] = time.Since(t0).Nanoseconds()
			}
		}()
	}
	wg.Wait()
	res.writeWall = time.Since(t0all)
	return res, nil
}

// runOutbox writes the order and its event in one transaction, and a relay
// poller publishes unsent outbox rows and marks them sent. The relay retries
// until the broker comes back.
func runOutbox(ctx context.Context, pool *pgxpool.Pool, c config, inj *injector) (*writerResult, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.RecordDeliveryTimeout(120*time.Second),
		kgo.WithLogger(kgo.BasicLogger(discard{}, kgo.LogLevelError, nil)),
	)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	res := &writerResult{commitNs: make([]int64, c.n), latencyNs: make([]int64, c.n)}
	amounts := dataset(c.n)
	var counter atomic.Int64
	writersDone := make(chan struct{})

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		relay(ctx, pool, cl, writersDone, res)
	}()

	var wg sync.WaitGroup
	t0all := time.Now()
	for w := 0; w < c.workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				seq := counter.Add(1) - 1
				if seq >= int64(c.n) {
					return
				}
				t0 := time.Now()
				tx, err := pool.Begin(ctx)
				if err != nil {
					log.Printf("begin %d: %v", seq, err)
					continue
				}
				_, err = tx.Exec(ctx,
					"INSERT INTO orders (seq, customer, amount_cents) VALUES ($1,$2,$3)",
					seq, customer(seq), amounts[seq])
				if err == nil {
					_, err = tx.Exec(ctx, "INSERT INTO outbox (seq, payload) VALUES ($1,$2)",
						seq, string(newSeqPayload(seq)))
				}
				if err != nil {
					_ = tx.Rollback(ctx)
					log.Printf("tx %d: %v", seq, err)
					continue
				}
				if err := tx.Commit(ctx); err != nil {
					log.Printf("commit %d: %v", seq, err)
					continue
				}
				res.commitNs[seq] = time.Now().UnixNano()
				res.latencyNs[seq] = time.Since(t0).Nanoseconds()
				inj.maybeFire(seq)
			}
		}()
	}
	wg.Wait()
	res.writeWall = time.Since(t0all)
	close(writersDone)
	<-relayDone
	return res, nil
}

// relay polls the outbox, publishes, then marks rows sent. Publish-then-mark is
// at-least-once by construction: a crash between the two replays the batch.
func relay(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, writersDone <-chan struct{}, res *writerResult) {
	for {
		rows, err := pool.Query(ctx,
			"SELECT seq, payload FROM outbox WHERE sent_at IS NULL ORDER BY seq LIMIT 500")
		if err != nil {
			log.Printf("relay query: %v", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		var seqs []int64
		var recs []*kgo.Record
		for rows.Next() {
			var seq int64
			var payload string
			if err := rows.Scan(&seq, &payload); err != nil {
				break
			}
			seqs = append(seqs, seq)
			recs = append(recs, &kgo.Record{Topic: topic, Value: []byte(payload)})
		}
		rows.Close()

		if len(recs) == 0 {
			select {
			case <-writersDone:
				return
			default:
				time.Sleep(20 * time.Millisecond)
				continue
			}
		}
		if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			atomic.AddInt64(&res.pubErrs, 1)
			log.Printf("relay produce (%d recs): %v", len(recs), err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if _, err := pool.Exec(ctx,
			"UPDATE outbox SET sent_at = clock_timestamp() WHERE seq = ANY($1)", seqs); err != nil {
			log.Printf("relay mark: %v", err)
		}
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
