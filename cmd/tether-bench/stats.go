package main

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

// summary is the distribution of a set of durations. The percentiles are exact
// (nearest rank over every sample), so there is no bucketing error to explain
// next to a p99.
type summary struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
	Mean  float64 `json:"mean_ms"`
}

// summarize sorts d in place and returns its summary.
func summarize(d []time.Duration) summary {
	if len(d) == 0 {
		return summary{}
	}

	slices.Sort(d)

	var sum time.Duration

	for _, v := range d {
		sum += v
	}

	return summary{
		Count: len(d),
		P50:   ms(quantile(d, 0.50)),
		P90:   ms(quantile(d, 0.90)),
		P99:   ms(quantile(d, 0.99)),
		P999:  ms(quantile(d, 0.999)),
		Max:   ms(d[len(d)-1]),
		Mean:  ms(sum / time.Duration(len(d))),
	}
}

// quantile is the nearest-rank q quantile of sorted.
func quantile(sorted []time.Duration, q float64) time.Duration {
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	idx = min(max(idx, 0), len(sorted)-1)

	return sorted[idx]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// String is one line for a table.
func (s summary) String() string {
	if s.Count == 0 {
		return "no samples"
	}

	return fmt.Sprintf("n=%d p50=%.2fms p90=%.2fms p99=%.2fms p99.9=%.2fms max=%.2fms",
		s.Count, s.P50, s.P90, s.P99, s.P999, s.Max)
}

// machine describes where a run happened, for the line every result carries.
type machine struct {
	CPU      string `json:"cpu"`
	Cores    int    `json:"cores"`
	LoadAvg  string `json:"load_average"`
	GoOS     string `json:"goos"`
	GoArch   string `json:"goarch"`
	GoVer    string `json:"go"`
	TakenAt  string `json:"taken_at"`
	RAMBytes int64  `json:"ram_bytes,omitempty"`
}

// describeMachine reads the CPU, core count, memory and load averages of the
// host. Anything it cannot read stays empty.
func describeMachine(ctx context.Context) machine {
	m := machine{
		Cores: runtime.NumCPU(), GoOS: runtime.GOOS, GoArch: runtime.GOARCH, GoVer: runtime.Version(),
		TakenAt: time.Now().Format(time.RFC3339),
	}

	if runtime.GOOS == "darwin" {
		m.CPU = sysctl(ctx, "machdep.cpu.brand_string")

		if n, err := strconv.ParseInt(sysctl(ctx, "hw.memsize"), 10, 64); err == nil {
			m.RAMBytes = n
		}
	}

	m.LoadAvg = loadAverage(ctx)

	return m
}

func sysctl(ctx context.Context, name string) string {
	// #nosec G204 -- the names are the ones above, not input.
	out, err := exec.CommandContext(ctx, "sysctl", "-n", name).Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

// loadAverage is the 1, 5 and 15 minute load averages as uptime prints them.
func loadAverage(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "uptime").Output()
	if err != nil {
		return ""
	}

	s := strings.TrimSpace(string(out))
	if _, after, ok := strings.Cut(s, "load averages:"); ok {
		return strings.TrimSpace(after)
	}

	if _, after, ok := strings.Cut(s, "load average:"); ok {
		return strings.TrimSpace(after)
	}

	return s
}

// rssBytes is the resident set size of each process, by pid. A process that has
// gone reads zero.
func rssBytes(ctx context.Context, pids []int) []int64 {
	out := make([]int64, len(pids))

	for i, pid := range pids {
		// #nosec G204 -- pid is an integer.
		raw, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
		if err != nil {
			continue
		}

		kb, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if err == nil {
			out[i] = kb * 1024
		}
	}

	return out
}

func mib(b int64) float64 { return float64(b) / (1 << 20) }

// parsePIDs reads a comma separated list of process IDs.
func parsePIDs(s string) ([]int, error) {
	var out []int

	for _, p := range splitList(s) {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("pid %q: %w", p, err)
		}

		out = append(out, n)
	}

	return out, nil
}

// cpuSeconds is the CPU time each process has used so far, by pid, read from
// ps. A process that has gone reads zero.
func cpuSeconds(ctx context.Context, pids []int) []float64 {
	out := make([]float64, len(pids))

	for i, pid := range pids {
		// #nosec G204 -- pid is an integer.
		raw, err := exec.CommandContext(ctx, "ps", "-o", "time=", "-p", strconv.Itoa(pid)).Output()
		if err != nil {
			continue
		}

		out[i] = parseCPUTime(strings.TrimSpace(string(raw)))
	}

	return out
}

// parseCPUTime reads ps's time column: [[hh:]mm:]ss with a fraction on the last
// part. It returns zero for anything else.
func parseCPUTime(s string) float64 {
	var secs float64

	for part := range strings.SplitSeq(s, ":") {
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0
		}

		secs = secs*60 + v
	}

	return secs
}
