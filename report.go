package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// report reconciles the consumer sink against the orders table, which is the
// ground truth, and writes both the summary and the raw per-record data.
func report(ctx context.Context, pool *pgxpool.Pool, c config, w *writerResult, cons *consumer, inj *injector, wall time.Duration) error {
	written := map[int64]bool{}
	rows, err := pool.Query(ctx, "SELECT seq FROM orders")
	if err != nil {
		return err
	}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return err
		}
		written[seq] = true
	}
	rows.Close()

	recs := cons.snapshot()
	seen := map[int64]int64{} // seq -> first recv ns
	dupes := 0
	for _, r := range recs {
		if _, ok := seen[r.Seq]; ok {
			dupes++
			continue
		}
		seen[r.Seq] = r.RecvNs
	}
	lost := 0
	var lags []int64
	for seq := range written {
		recv, ok := seen[seq]
		if !ok {
			lost++
			continue
		}
		if w.commitNs[seq] > 0 {
			lags = append(lags, recv-w.commitNs[seq])
		}
	}

	var lat []int64
	for _, v := range w.latencyNs {
		if v > 0 {
			lat = append(lat, v)
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })

	s := summary{
		Arm: c.arm, Rep: c.rep, Inject: c.inject, N: c.n,
		Written: len(written), Received: len(recs), Distinct: len(seen),
		Lost: lost, Dupes: dupes, PublishErrs: w.pubErrs,
		WriteP50Ms: percentile(lat, 0.50), WriteP95Ms: percentile(lat, 0.95),
		WriteP99Ms: percentile(lat, 0.99), WriteMaxMs: percentile(lat, 1.0),
		LagP50Ms: percentile(lags, 0.50), LagP95Ms: percentile(lags, 0.95),
		LagP99Ms: percentile(lags, 0.99), LagMaxMs: percentile(lags, 1.0),
		WallS:       wall.Seconds(),
		ThroughputW: float64(len(written)) / wall.Seconds(),
	}
	if inj.enabled && !inj.backAt.IsZero() {
		s.KilledAt = inj.killedAt.Format(time.RFC3339Nano)
		s.BackAt = inj.backAt.Format(time.RFC3339Nano)
		s.RecoveryS = recoverySeconds(written, seen, w, inj.backAt.UnixNano())
	}

	tag := fmt.Sprintf("%s-%s-rep%d", c.arm, map[bool]string{true: "inject", false: "control"}[c.inject], c.rep)
	if err := os.MkdirAll(c.out, 0o755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(c.out, tag+".json"), s); err != nil {
		return err
	}
	if err := writeGzipJSON(filepath.Join(c.out, tag+".raw.json.gz"), map[string]any{
		"received": recs, "commit_ns": w.commitNs, "latency_ns": w.latencyNs,
	}); err != nil {
		return err
	}
	out, _ := json.MarshalIndent(s, "", "  ")
	fmt.Println(string(out))
	return nil
}

// recoverySeconds is the time from the broker coming back to the consumer
// having received every message whose write committed before that moment.
func recoverySeconds(written map[int64]bool, seen map[int64]int64, w *writerResult, backNs int64) float64 {
	var last int64
	for seq := range written {
		if w.commitNs[seq] == 0 || w.commitNs[seq] >= backNs {
			continue
		}
		if recv, ok := seen[seq]; ok && recv > last {
			last = recv
		}
	}
	if last == 0 {
		return 0
	}
	return float64(last-backNs) / 1e9
}

func writeGzipJSON(path string, v any) error {
	f, err := os.Create(path) // #nosec G304 -- path is built from our own flags
	if err != nil {
		return err
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	if err := json.NewEncoder(zw).Encode(v); err != nil {
		return err
	}
	return zw.Close()
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
