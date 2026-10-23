package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// rec is a concurrency-safe latency recorder in microseconds.
type rec struct {
	mu   sync.Mutex
	h    *hdrhistogram.Histogram
	errs map[string]int
	sent int64
	busy time.Duration // time the cell was active (sending)
	load []float64     // 1-minute load average sampled at each window
}

func newRec() *rec {
	return &rec{h: hdrhistogram.New(1, 120_000_000, 3), errs: map[string]int{}}
}

func (r *rec) add(d time.Duration) {
	us := d.Microseconds()
	if us < 1 {
		us = 1
	}

	r.mu.Lock()
	_ = r.h.RecordValue(us)
	r.mu.Unlock()
}

func (r *rec) err(e string) {
	r.mu.Lock()
	r.errs[e]++
	r.mu.Unlock()
}

// row is one cell's result in one run.
type row struct {
	Cell     string         `json:"cell"`
	Target   float64        `json:"target_per_s"`
	Achieved float64        `json:"achieved_per_s"`
	Acked    int64          `json:"acked"`
	P50      float64        `json:"p50_ms"`
	P90      float64        `json:"p90_ms"`
	P99      float64        `json:"p99_ms"`
	Max      float64        `json:"max_ms"`
	Errors   map[string]int `json:"errors,omitempty"`
	Load     float64        `json:"load1_mean"`
}

func (r *rec) row(cell string, target float64) row {
	r.mu.Lock()
	defer r.mu.Unlock()

	var load float64
	for _, l := range r.load {
		load += l
	}

	if len(r.load) > 0 {
		load /= float64(len(r.load))
	}

	ms := func(q float64) float64 { return float64(r.h.ValueAtQuantile(q)) / 1000 }

	errs := map[string]int{}
	for k, v := range r.errs {
		errs[k] = v
	}

	return row{
		Cell: cell, Target: target,
		Achieved: float64(r.h.TotalCount()) / r.busy.Seconds(),
		Acked:    r.h.TotalCount(),
		P50:      ms(50), P90: ms(90), P99: ms(99), Max: float64(r.h.Max()) / 1000,
		Errors: errs, Load: load,
	}
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}

	s := append([]float64(nil), v...)
	sort.Float64s(s)

	return s[len(s)/2]
}

func printRows(title string, rows []row) {
	fmt.Printf("\n%s\n", title)
	fmt.Printf("  %-26s %9s %10s %9s %9s %9s %9s %7s  %s\n", "cell", "target/s", "achieved/s", "p50 ms", "p90 ms", "p99 ms", "max ms", "load", "errors")

	for _, r := range rows {
		fmt.Printf("  %-26s %9.0f %10.0f %9.3f %9.3f %9.3f %9.1f %7.1f  %v\n", r.Cell, r.Target, r.Achieved, r.P50, r.P90, r.P99, r.Max, r.Load, r.Errors)
	}
}

// medianRows takes per-run rows (same cell order in each run) and returns the
// per-cell median of each column across runs.
func medianRows(runs [][]row) []row {
	if len(runs) == 0 {
		return nil
	}

	out := make([]row, len(runs[0]))

	for i := range runs[0] {
		var ach, p50, p90, p99, mx, ld []float64

		for _, rr := range runs {
			ach = append(ach, rr[i].Achieved)
			p50 = append(p50, rr[i].P50)
			p90 = append(p90, rr[i].P90)
			p99 = append(p99, rr[i].P99)
			mx = append(mx, rr[i].Max)
			ld = append(ld, rr[i].Load)
		}

		out[i] = row{
			Cell: runs[0][i].Cell, Target: runs[0][i].Target,
			Achieved: median(ach), P50: median(p50), P90: median(p90), P99: median(p99), Max: median(mx), Load: median(ld),
		}
	}

	return out
}

func save(dir, name string, v any) {
	if dir == "" {
		return
	}

	_ = os.MkdirAll(dir, 0o755)

	b, _ := json.MarshalIndent(v, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, name+".json"), b, 0o644)
}
