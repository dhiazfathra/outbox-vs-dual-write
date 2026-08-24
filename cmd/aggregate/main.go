// Command aggregate turns the per-run JSON summaries in results/ into a
// markdown table. Rep 1 of every arm is a warm-up and is excluded.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type row struct {
	Arm         string  `json:"arm"`
	Rep         int     `json:"rep"`
	Inject      bool    `json:"inject"`
	Written     int     `json:"written"`
	Received    int     `json:"received"`
	Distinct    int     `json:"distinct"`
	Lost        int     `json:"lost"`
	Dupes       int     `json:"dupes"`
	PublishErrs int64   `json:"publish_errors"`
	WriteP50Ms  float64 `json:"write_p50_ms"`
	WriteP95Ms  float64 `json:"write_p95_ms"`
	WriteP99Ms  float64 `json:"write_p99_ms"`
	LagP50Ms    float64 `json:"e2e_lag_p50_ms"`
	LagP95Ms    float64 `json:"e2e_lag_p95_ms"`
	LagP99Ms    float64 `json:"e2e_lag_p99_ms"`
	LagMaxMs    float64 `json:"e2e_lag_max_ms"`
	RecoveryS   float64 `json:"recovery_s"`
	Throughput  float64 `json:"write_throughput_per_s"`
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	return v[len(v)/2]
}

func main() {
	paths, _ := filepath.Glob("results/*.json")
	groups := map[string][]row{}
	for _, p := range paths {
		if filepath.Ext(p) != ".json" || len(p) > 10 && filepath.Base(p) == "SUMMARY.json" {
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var r row
		if err := json.Unmarshal(b, &r); err != nil || r.Arm == "" {
			continue
		}
		if r.Rep == 1 {
			continue // warm-up
		}
		mode := "control"
		if r.Inject {
			mode = "inject"
		}
		groups[r.Arm+"/"+mode] = append(groups[r.Arm+"/"+mode], r)
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Println("# Results (medians over repetitions, rep 1 discarded as warm-up)")
	fmt.Println()
	fmt.Println("| arm | reps | written | lost | dupes | write p50 ms | write p95 ms | write p99 ms | lag p50 ms | lag p99 ms | lag max ms | recovery s | writes/s |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, k := range keys {
		g := groups[k]
		f := func(sel func(row) float64) float64 {
			v := make([]float64, 0, len(g))
			for _, r := range g {
				v = append(v, sel(r))
			}
			return median(v)
		}
		fmt.Printf("| %s | %d | %.0f | %.0f | %.0f | %.2f | %.2f | %.2f | %.1f | %.1f | %.1f | %.2f | %.0f |\n",
			k, len(g),
			f(func(r row) float64 { return float64(r.Written) }),
			f(func(r row) float64 { return float64(r.Lost) }),
			f(func(r row) float64 { return float64(r.Dupes) }),
			f(func(r row) float64 { return r.WriteP50Ms }),
			f(func(r row) float64 { return r.WriteP95Ms }),
			f(func(r row) float64 { return r.WriteP99Ms }),
			f(func(r row) float64 { return r.LagP50Ms }),
			f(func(r row) float64 { return r.LagP99Ms }),
			f(func(r row) float64 { return r.LagMaxMs }),
			f(func(r row) float64 { return r.RecoveryS }),
			f(func(r row) float64 { return r.Throughput }),
		)
	}
	fmt.Println()
	fmt.Println("Per-repetition detail:")
	fmt.Println()
	fmt.Println("| arm | mode | rep | written | received | distinct | lost | dupes | publish errors |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|")
	for _, k := range keys {
		g := groups[k]
		sort.Slice(g, func(i, j int) bool { return g[i].Rep < g[j].Rep })
		for _, r := range g {
			mode := "control"
			if r.Inject {
				mode = "inject"
			}
			fmt.Printf("| %s | %s | %d | %d | %d | %d | %d | %d | %d |\n",
				r.Arm, mode, r.Rep, r.Written, r.Received, r.Distinct, r.Lost, r.Dupes, r.PublishErrs)
		}
	}
}
