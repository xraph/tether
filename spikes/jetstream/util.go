package main

import (
	"errors"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func sysctl(k string) string {
	out, err := exec.Command("sysctl", "-n", k).Output()
	if err != nil {
		return "?"
	}

	return strings.TrimSpace(string(out))
}

func loadavg() string { return sysctl("vm.loadavg") }

// load1 is the 1-minute load average as a number.
func load1() float64 {
	f := strings.Fields(strings.Trim(loadavg(), "{} "))
	if len(f) == 0 {
		return 0
	}

	v, _ := strconv.ParseFloat(f[0], 64)

	return v
}

func machine() string {
	return sysctl("machdep.cpu.brand_string") + ", " + sysctl("hw.ncpu") + " cores, " +
		sysctl("hw.memsize") + " bytes RAM, " + runtime.GOOS + "/" + runtime.GOARCH + ", " +
		runtime.Version() + ", nats-server " + server.VERSION + ", nats.go " + nats.Version
}

func asAPI(err error, target **jetstream.APIError) bool { return errors.As(err, target) }
