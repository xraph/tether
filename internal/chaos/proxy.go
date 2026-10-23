// Package chaos holds fault injection for Tether's own tests. The TCP proxy
// sits between a client and a server and, per direction, passes bytes through,
// delays them, holds them back without closing anything, or resets the
// connection. The cluster tests use it to cut a broker node off from NATS.
package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Mode is what the proxy does with the bytes of one direction.
type Mode int

// The modes of a direction.
const (
	// Pass forwards bytes as they arrive.
	Pass Mode = iota
	// Delay forwards bytes after the direction's delay (SetDelay).
	Delay
	// Blackhole forwards nothing and closes nothing. The peer sees silence,
	// not a reset. Bytes already sent are held, and a connection accepted in
	// this mode stays open, so a return to Pass delivers what was held, the
	// way a healed network partition does.
	Blackhole
	// Reset closes every connection with a TCP reset and does the same to each
	// new one at accept. Setting Reset on either direction resets the
	// connection.
	Reset
)

func (m Mode) String() string {
	switch m {
	case Pass:
		return "pass"
	case Delay:
		return "delay"
	case Blackhole:
		return "blackhole"
	case Reset:
		return "reset"
	default:
		return fmt.Sprintf("mode(%d)", int(m))
	}
}

// Direction names one way through the proxy.
type Direction int

// The directions. Both is accepted by SetMode and SetDelay only.
const (
	ClientToServer Direction = iota
	ServerToClient
	Both
)

const (
	// chunkSize is the size of one read from a socket.
	chunkSize = 32 * 1024
	// queueDepth is how many chunks a direction holds in flight. A full queue
	// stops reads, which pushes back on the sender like a full TCP window.
	queueDepth = 64
	// dialTimeout bounds the connect to the target.
	dialTimeout = 5 * time.Second
)

// Proxy is a TCP proxy that injects faults. The zero value is not usable; call
// NewProxy. It is safe for concurrent use.
type Proxy struct {
	target string
	ln     net.Listener

	mu      sync.Mutex
	modes   [2]Mode
	delays  [2]time.Duration
	changed chan struct{} // closed and replaced on every change
	conns   map[*link]struct{}

	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
}

// NewProxy listens on a random loopback port and forwards to target
// ("host:port"). Every direction starts in Pass.
func NewProxy(target string) (*Proxy, error) {
	var lc net.ListenConfig

	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("chaos: listen: %w", err)
	}

	p := &Proxy{
		target:  target,
		ln:      ln,
		changed: make(chan struct{}),
		conns:   make(map[*link]struct{}),
		done:    make(chan struct{}),
	}

	p.wg.Add(1)

	go p.accept()

	return p, nil
}

// Addr is the address clients dial, "127.0.0.1:port". Prefix it with "nats://"
// for a NATS URL.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Conns is the number of client connections the proxy holds open.
func (p *Proxy) Conns() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.conns)
}

// SetMode sets the mode of a direction (both for Both). Reset also closes the
// connections that exist now. The change reaches connections already open.
func (p *Proxy) SetMode(dir Direction, m Mode) {
	p.mu.Lock()

	for _, d := range dirs(dir) {
		p.modes[d] = m
	}

	p.notifyLocked()

	var victims []*link
	if m == Reset {
		victims = p.linksLocked()
	}

	p.mu.Unlock()

	for _, l := range victims {
		l.reset()
	}
}

// SetDelay sets the added latency of a direction (both for Both). It applies
// while that direction is in Delay.
func (p *Proxy) SetDelay(dir Direction, d time.Duration) {
	p.mu.Lock()

	for _, x := range dirs(dir) {
		p.delays[x] = d
	}

	p.notifyLocked()
	p.mu.Unlock()
}

// Close stops listening, drops every connection and waits for the proxy's
// goroutines. It is idempotent.
func (p *Proxy) Close() {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		p.wg.Wait()

		return
	}

	p.closed = true
	close(p.done)

	victims := p.linksLocked()

	p.mu.Unlock()

	_ = p.ln.Close()

	for _, l := range victims {
		l.close()
	}

	p.wg.Wait()
}

func dirs(d Direction) []Direction {
	if d == Both {
		return []Direction{ClientToServer, ServerToClient}
	}

	return []Direction{d}
}

// notifyLocked wakes everything waiting on a mode change.
func (p *Proxy) notifyLocked() {
	close(p.changed)

	p.changed = make(chan struct{})
}

func (p *Proxy) linksLocked() []*link {
	out := make([]*link, 0, len(p.conns))
	for l := range p.conns {
		out = append(out, l)
	}

	return out
}

// state is what a pump needs to know about its direction at one moment.
type state struct {
	mode    Mode
	delay   time.Duration
	changed <-chan struct{}
	reset   bool // either direction is in Reset
}

func (p *Proxy) state(dir Direction) state {
	p.mu.Lock()
	defer p.mu.Unlock()

	return state{
		mode:    p.modes[dir],
		delay:   p.delays[dir],
		changed: p.changed,
		reset:   p.modes[ClientToServer] == Reset || p.modes[ServerToClient] == Reset,
	}
}

func (p *Proxy) accept() {
	defer p.wg.Done()

	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}

		p.wg.Add(1)

		go p.serve(c)
	}
}

// link is one proxied connection.
type link struct {
	client, server net.Conn
	once           sync.Once
	done           chan struct{}
}

func (l *link) close() {
	l.once.Do(func() {
		close(l.done)
		_ = l.client.Close()
		_ = l.server.Close()
	})
}

// reset closes both sockets so the peers see a TCP reset.
func (l *link) reset() {
	setLinger0(l.client)
	setLinger0(l.server)
	l.close()
}

func setLinger0(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		discard(tc.SetLinger(0))
	}
}

func (p *Proxy) serve(client net.Conn) {
	defer p.wg.Done()

	if p.state(ClientToServer).reset {
		setLinger0(client)
		_ = client.Close()

		return
	}

	d := net.Dialer{Timeout: dialTimeout}

	server, err := d.DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		setLinger0(client)
		_ = client.Close()

		return
	}

	l := &link{client: client, server: server, done: make(chan struct{})}

	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		l.close()

		return
	}

	p.conns[l] = struct{}{}

	p.mu.Unlock()

	var pumps sync.WaitGroup

	pumps.Add(2)

	go func() {
		defer pumps.Done()

		p.pump(l, client, server, ClientToServer)
	}()

	go func() {
		defer pumps.Done()

		p.pump(l, server, client, ServerToClient)
	}()

	pumps.Wait()
	l.close()

	p.mu.Lock()
	delete(p.conns, l)
	p.mu.Unlock()
}

type chunk struct {
	data []byte
	at   time.Time
	err  error // set on the last chunk: io.EOF or a read error
}

// pump moves bytes from src to dst. A reader goroutine fills a queue and this
// goroutine applies the direction's mode before each write, so a mode change
// reaches bytes already read.
func (p *Proxy) pump(l *link, src, dst net.Conn, dir Direction) {
	queue := make(chan chunk, queueDepth)

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		p.readLoop(l, src, dir, queue)
	}()

	for {
		var ch chunk

		select {
		case ch = <-queue:
		case <-l.done:
			return
		}

		if !p.admit(l, dir, ch.at) {
			return
		}

		if len(ch.data) > 0 {
			if _, err := dst.Write(ch.data); err != nil {
				l.close()

				return
			}
		}

		if ch.err != nil {
			if errors.Is(ch.err, io.EOF) {
				if tc, ok := dst.(*net.TCPConn); ok {
					discard(tc.CloseWrite())
				}

				return
			}

			l.close()

			return
		}
	}
}

// admit waits until the chunk read at readAt may be written: not while the
// direction is in Blackhole, and not before the delay has passed in Delay. It
// returns false when the connection is gone or was reset.
func (p *Proxy) admit(l *link, dir Direction, readAt time.Time) bool {
	for {
		st := p.state(dir)

		switch {
		case st.reset || st.mode == Reset:
			l.reset()

			return false
		case st.mode == Blackhole:
			select {
			case <-st.changed:
				continue
			case <-l.done:
				return false
			}
		case st.mode == Delay && st.delay > 0:
			wait := time.Until(readAt.Add(st.delay))
			if wait <= 0 {
				return true
			}

			t := time.NewTimer(wait)

			select {
			case <-t.C:
				// The mode may have changed meanwhile: look again.
				continue
			case <-st.changed:
				t.Stop()

				continue
			case <-l.done:
				t.Stop()

				return false
			}
		default:
			return true
		}
	}
}

func (p *Proxy) readLoop(l *link, src net.Conn, dir Direction, queue chan<- chunk) {
	buf := make([]byte, chunkSize)

	for {
		// Do not read while the direction is a black hole: the bytes stay in the
		// kernel and the sender is pushed back, as in a real partition.
		if !p.waitReadable(l, dir) {
			return
		}

		n, err := src.Read(buf)
		ch := chunk{at: time.Now(), err: err}

		if n > 0 {
			ch.data = append([]byte(nil), buf[:n]...)
		}

		if n > 0 || err != nil {
			select {
			case queue <- ch:
			case <-l.done:
				return
			}
		}

		if err != nil {
			return
		}
	}
}

func (p *Proxy) waitReadable(l *link, dir Direction) bool {
	for {
		st := p.state(dir)
		if st.mode != Blackhole {
			return true
		}

		select {
		case <-st.changed:
		case <-l.done:
			return false
		}
	}
}

// discard drops an error that cannot be acted on: the socket is going away
// anyway.
func discard(error) {}
